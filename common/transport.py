"""Framed TCP transport between nodes, adapted from Assignment 1's messaging.py.

Guarantees:
  * A message always arrives whole: 4-byte length prefix, then UTF-8 JSON.
  * Messages on one directed channel arrive in send order: there is one
    persistent connection per direction.
  * Every message is checked against common.messages on send and on receipt.

Differences from Assignment 1, needed because nodes now join and leave:
  * Nodes are named by string id, and the address book changes at runtime.
  * The hello frame carries the sender's listen address, so a node can reply
    to a newcomer that is not in the config.
  * The loss of a node is reported to the application.

Handlers all run on one dispatcher thread, in arrival order. A slow handler
delays every later message, so run long work on your own thread.
"""

import json
import logging
import os
import queue
import socket
import struct
import threading
import time
from collections import Counter

from common.messages import SchemaError, make_message, validate

_HEADER = struct.Struct("!I")
MAX_FRAME_BYTES = 4 * 1024 * 1024
HELLO_KEY = "__hello_from__"

_CONNECT_RETRY_S = 0.2
_ACCEPT_POLL_S = 0.2
# Bounds one attempt to a node that has gone. Windows takes about 2s to report
# a refused connection, and a firewall that drops packets never reports one.
_RECONNECT_TIMEOUT_S = 0.25


class TransportError(RuntimeError):
    """A message could not be delivered to the operating system's TCP stack."""


def _encode_frame(obj):
    try:
        body = json.dumps(obj).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SchemaError("payload is not JSON-serialisable: {}".format(exc))
    if len(body) > MAX_FRAME_BYTES:
        raise TransportError("message too large: {} bytes".format(len(body)))
    return _HEADER.pack(len(body)) + body


def _recv_exactly(sock, count):
    chunks = []
    while count:
        chunk = sock.recv(count)
        if not chunk:
            return None
        chunks.append(chunk)
        count -= len(chunk)
    return b"".join(chunks)


def _recv_frame(sock):
    """Read one frame, or return None if the connection closed."""
    header = _recv_exactly(sock, _HEADER.size)
    if header is None:
        return None
    (length,) = _HEADER.unpack(header)
    if length > MAX_FRAME_BYTES:
        raise TransportError("declared frame length {} exceeds limit".format(length))
    body = _recv_exactly(sock, length)
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


def _close(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


class Endpoint:
    """One node's network presence: a listener plus its outgoing channels."""

    def __init__(self, node_id, host, port, routes=None, logger=None,
                 connect_timeout=2.0, send_timeout=5.0):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.log = logger or logging.getLogger(node_id)
        self.connect_timeout = connect_timeout
        self.send_timeout = send_timeout

        self._routes = dict(routes or {})
        self._links = {}             # target id -> outgoing socket
        self._reached = set()        # targets connected to at least once
        self._send_locks = {}        # target id -> lock, kept for the Endpoint's lifetime
        self._inbound = {}           # sender id -> newest incoming socket
        self._state_lock = threading.Lock()

        self._handlers = {}
        self._any_handlers = []
        self._disconnect_handlers = []
        self._inbox = queue.Queue()

        self._server = None
        self._running = threading.Event()
        self._dispatcher = None

        self._sent = Counter()
        self._received = Counter()
        self._rejected = 0
        self._bytes_sent = 0

    # -- registration ------------------------------------------------------

    def on(self, msg_type, handler):
        """Call handler(msg) for each message of this type."""
        self._handlers[msg_type] = handler

    def on_any(self, handler):
        """Call handler(msg) for every message, before its type handler."""
        self._any_handlers.append(handler)

    def on_disconnect(self, handler):
        """Call handler(node_id) when a node's incoming channel closes.

        Runs after that node's earlier messages have been handled, so a LEAVE
        is always seen before the disconnect it causes.
        """
        self._disconnect_handlers.append(handler)

    # -- address book ------------------------------------------------------

    def add_route(self, node_id, host, port):
        """Record where a node listens. A changed address drops the old link."""
        address = (host, int(port))
        with self._state_lock:
            changed = self._routes.get(node_id) not in (None, address)
            self._routes[node_id] = address
        if changed:
            self._drop_link(node_id)

    def remove_route(self, node_id):
        with self._state_lock:
            self._routes.pop(node_id, None)
        self._drop_link(node_id)

    def routes(self):
        with self._state_lock:
            return dict(self._routes)

    # -- lifecycle ---------------------------------------------------------

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # On Windows this option would let two processes share a port.
        if os.name != "nt":
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bind_host = self.host if self.host.startswith("127.") else "0.0.0.0"
        try:
            server.bind((bind_host, self.port))
        except OSError as exc:
            server.close()
            raise TransportError("cannot bind {}:{}: {}. Is this node already running?".format(
                bind_host, self.port, exc))
        server.listen(32)
        server.settimeout(_ACCEPT_POLL_S)
        self.port = server.getsockname()[1]
        self._server = server
        self._running.set()

        threading.Thread(target=self._accept_loop, daemon=True).start()
        self._dispatcher = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._dispatcher.start()
        self.log.info("listening on %s:%s", self.host, self.port)
        return self

    def stop(self):
        """Close everything. Safe to call twice."""
        if not self._running.is_set():
            return
        self._running.clear()
        self._inbox.put(None)
        if self._server is not None:
            self._server.close()
        with self._state_lock:
            sockets = list(self._links.values()) + list(self._inbound.values())
            self._links.clear()
            self._inbound.clear()
        for sock in sockets:
            _close(sock)
        if self._dispatcher is not threading.current_thread():
            self._dispatcher.join(timeout=2)
        self.log.info("stopped")

    # -- sending -----------------------------------------------------------

    def send(self, target, msg_type, /, **payload):
        """Send one message and return its msg_id.

        Raises SchemaError for a bad message, and TransportError if the target
        cannot be reached.
        """
        if not self._running.is_set():
            raise TransportError("endpoint is not running")
        if target == self.node_id:
            raise TransportError("a node cannot send to itself")

        msg = make_message(msg_type, self.node_id, **payload)
        frame = _encode_frame(msg)
        with self._lock_for(target):
            sock = self._get_link(target)
            try:
                sock.sendall(frame)
            except OSError as exc:
                self._drop_link(target, sock)
                raise TransportError("send to {} failed: {}".format(target, exc))

        with self._state_lock:
            self._sent[msg_type] += 1
            self._bytes_sent += len(frame)
        self.log.debug("SEND %s -> %s", msg_type, target)
        return msg["msg_id"]

    def broadcast(self, targets, msg_type, /, **payload):
        """Send to each target and return the ids that could not be reached."""
        failed = []
        for target in targets:
            try:
                self.send(target, msg_type, **payload)
            except TransportError as exc:
                self.log.warning("broadcast %s to %s failed: %s", msg_type, target, exc)
                failed.append(target)
        return failed

    def stats(self):
        """Message counters, used for the P2P vs cluster comparison."""
        with self._state_lock:
            return {
                "sent": dict(self._sent),
                "received": dict(self._received),
                "rejected": self._rejected,
                "bytes_sent": self._bytes_sent,
            }

    def _lock_for(self, target):
        with self._state_lock:
            return self._send_locks.setdefault(target, threading.Lock())

    def _get_link(self, target):
        """Return the socket to target, connecting if needed. Needs the send lock."""
        with self._state_lock:
            sock = self._links.get(target)
            address = self._routes.get(target)
            reached_before = target in self._reached
        if sock is not None:
            return sock
        if address is None:
            raise TransportError("no route to {}".format(target))

        # Retrying covers nodes still starting up. A node that was reachable
        # and now refuses has gone, and waiting here would stall the
        # dispatcher long enough to make healthy nodes look dead.
        deadline = time.monotonic() + (0 if reached_before else self.connect_timeout)
        attempt_timeout = _RECONNECT_TIMEOUT_S if reached_before else self.connect_timeout
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(attempt_timeout)
            try:
                sock.connect(address)
                sock.settimeout(self.send_timeout)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.sendall(_encode_frame({
                    HELLO_KEY: self.node_id, "host": self.host, "port": self.port,
                }))
                break
            except OSError as exc:
                sock.close()
                if time.monotonic() >= deadline:
                    raise TransportError("cannot reach {} at {}:{}: {}".format(
                        target, address[0], address[1], exc))
                time.sleep(_CONNECT_RETRY_S)

        with self._state_lock:
            self._links[target] = sock
            self._reached.add(target)
        threading.Thread(target=self._watch_link, args=(target, sock), daemon=True).start()
        self.log.info("channel open %s -> %s", self.node_id, target)
        return sock

    def _watch_link(self, target, sock):
        """Notice a closed outgoing channel, so the next send reconnects.

        Nothing is ever written back on this socket, so any return means it
        closed. Without this, the first send to a dead node would appear to
        succeed and the message would be lost.
        """
        while True:
            try:
                if not sock.recv(1024):
                    break
            except socket.timeout:      # The socket's send timeout also applies here.
                continue
            except OSError:
                break
        self._drop_link(target, sock)

    def _drop_link(self, target, sock=None):
        with self._state_lock:
            current = self._links.get(target)
            if current is None or (sock is not None and current is not sock):
                current = None
            else:
                del self._links[target]
        if current is not None:
            _close(current)

    # -- receiving ---------------------------------------------------------

    def _accept_loop(self):
        while self._running.is_set():
            try:
                conn, addr = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.settimeout(None)
            threading.Thread(target=self._read_channel, args=(conn, addr), daemon=True).start()

    def _read_channel(self, conn, addr):
        sender = None
        try:
            hello = _recv_frame(conn)
            if not isinstance(hello, dict) or HELLO_KEY not in hello:
                self.log.warning("rejected unidentified connection from %s", addr[0])
                return
            sender = hello[HELLO_KEY]
            self.add_route(sender, hello["host"], hello["port"])
            with self._state_lock:
                self._inbound[sender] = conn
            self.log.info("channel open %s -> %s", sender, self.node_id)

            while self._running.is_set():
                msg = _recv_frame(conn)
                if msg is None:
                    break
                self._accept_message(sender, msg)
        except (OSError, ValueError, KeyError, TypeError, TransportError) as exc:
            if self._running.is_set():
                self.log.warning("channel from %s failed: %s", sender or addr[0], exc)
        finally:
            _close(conn)
            self._channel_closed(sender, conn)

    def _accept_message(self, sender, msg):
        try:
            validate(msg)
            if msg["sender"] != sender:
                raise SchemaError("sender {!r} on channel from {!r}".format(msg["sender"], sender))
        except SchemaError as exc:
            with self._state_lock:
                self._rejected += 1
            self.log.warning("rejected message from %s: %s", sender, exc)
            return
        with self._state_lock:
            self._received[msg["type"]] += 1
        self.log.debug("RECV %s <- %s", msg["type"], sender)
        self._inbox.put(("message", msg))

    def _channel_closed(self, sender, conn):
        if sender is None:
            return
        with self._state_lock:
            # A rejoining node may already have a newer channel open.
            current = self._inbound.get(sender) is conn
            if current:
                del self._inbound[sender]
        if current and self._running.is_set():
            self.log.info("channel closed %s -> %s", sender, self.node_id)
            self._inbox.put(("disconnect", sender))

    def _dispatch_loop(self):
        while True:
            item = self._inbox.get()
            if item is None:
                return
            kind, value = item
            if kind == "disconnect":
                for handler in self._disconnect_handlers:
                    self._call(handler, value)
                continue

            for handler in self._any_handlers:
                self._call(handler, value)
            handler = self._handlers.get(value["type"])
            if handler is None:
                self.log.warning("no handler for %s from %s", value["type"], value["sender"])
            else:
                self._call(handler, value)

    def _call(self, handler, value):
        try:
            handler(value)
        except Exception:
            self.log.exception("handler failed for %s", value)
