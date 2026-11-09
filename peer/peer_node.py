"""Peer node. Owner: Sri Harsha (core). Chandra Hasitha runs peers 3 and 4.

All peers run this same file:

    python -m peer.peer_node --id peer1
    python -m peer.peer_node --id peer5 --port 6005          # late joiner
    python -m peer.peer_node --id peer2 --share notes=README.md

What this file does (the P2P half of the system):

  * Discovery. A peer only knows the seeds at startup. JOIN / JOIN_ACK
    (joinleave/service.py) give it a first member list, and from then on
    every peer GOSSIPs its peer list to one random live peer every few
    seconds. New, dead and departed peers spread this way, with no central
    list. Every peer learned from gossip gets a route with add_route().

  * Resource registry. Two tables:
        store    resources this peer holds: name -> content
        holders  what other peers hold:     peer_id -> set of names
    `holders` is filled by RESOURCE_ANNOUNCE (sent to every peer we meet)
    and by RESOURCE_HIT, and a peer's entries are dropped when it leaves
    or fails.

  * Direct messaging. To find a resource, a peer floods RESOURCE_QUERY to
    its neighbours with a ttl. Each peer answers with RESOURCE_HIT straight
    to the origin if it holds the resource, and forwards the query while
    ttl > 0. Duplicate queries are dropped by query_id. The origin then
    fetches the content directly from the holder with RESOURCE_FETCH /
    RESOURCE_DATA. No coordinator is involved at any step.

    A fetched resource is kept and re-announced, so it is now held by two
    peers and survives the original holder leaving (replication).

Threads: handlers run on the endpoint's single dispatcher thread, and
membership callbacks may run on the heartbeat thread. All outgoing sends go
through one sender thread so a slow first connection never blocks a
handler, and `_lock` guards the tables.

Interactive console (when run in a terminal): type `help`.
"""

import json
import os
import queue
import random
import sys
import threading
import time
import uuid
from collections import OrderedDict

from common import MsgType, Status, TransportError, build_node, node_arg_parser, run_until_stopped
from joinleave.service import MembershipService
from peer.bridge import Bridge

DEFAULT_TTL = 3                 # enough hops for a small overlay
FETCH_TIMEOUT_S = 5.0
SEEN_QUERIES_MAX = 1000         # bounded memory for duplicate detection


def demo_resources(node_id):
    """Resources a configured peer holds at startup, so the demo has data.

    peer1 holds dataset-a, peer2 dataset-b, and so on: 20,000 integers each,
    generated from the peer id, so every run is the same. A peer not in the
    numbered range (e.g. a late joiner) starts with nothing but its notes.
    """
    resources = {"notes-{}".format(node_id): "Shared notes from {}.".format(node_id)}
    digits = "".join(ch for ch in node_id if ch.isdigit())
    if node_id.startswith("peer") and digits and 1 <= int(digits) <= 4:
        letter = "abcd"[int(digits) - 1]
        rng = random.Random(node_id)
        numbers = [rng.randint(1, 1_000_000) for _ in range(20_000)]
        resources["dataset-" + letter] = ",".join(map(str, numbers))
    return resources


class PeerNode:
    def __init__(self, ctx, resources=None, ttl=DEFAULT_TTL):
        self.ctx = ctx
        self.ttl = ttl
        self.membership = MembershipService(ctx)
        self.bridge = Bridge(ctx)

        self._lock = threading.Lock()
        self.store = dict(resources or {})       # name -> content (held here)
        self.origin_of = {name: ctx.node_id for name in self.store}  # where each copy came from
        self.holders = {}                        # peer_id -> set(names)
        self._seen = OrderedDict()               # query_id -> True, for dedupe
        self._queries = {}                       # query_id -> pending query (we are origin)
        self._fetching = {}                      # name -> query_id awaiting RESOURCE_DATA

        self._outbox = queue.Queue()
        self._stop = threading.Event()
        self._threads = []

        ctx.ep.on(MsgType.GOSSIP, self.on_gossip)
        ctx.ep.on(MsgType.RESOURCE_ANNOUNCE, self.on_resource_announce)
        ctx.ep.on(MsgType.RESOURCE_QUERY, self.on_resource_query)
        ctx.ep.on(MsgType.RESOURCE_HIT, self.on_resource_hit)
        ctx.ep.on(MsgType.RESOURCE_FETCH, self.on_resource_fetch)
        ctx.ep.on(MsgType.RESOURCE_DATA, self.on_resource_data)

        ctx.members.on_join(self._peer_joined)
        ctx.members.on_leave(self._peer_gone)
        ctx.members.on_fail(self._peer_gone)

    # -- lifecycle ------------------------------------------------------------

    def start(self):
        """Call after ctx.ep.start() and membership.start()."""
        for target in (self._sender_loop, self._gossip_loop):
            thread = threading.Thread(target=target, daemon=True,
                                      name="{}-{}".format(self.ctx.node_id, target.__name__))
            thread.start()
            self._threads.append(thread)
        self.ctx.log.info("peer started, holding %s", sorted(self.store))

    def stop(self):
        self._stop.set()
        self._outbox.put(None)
        self.membership.stop()
        for thread in self._threads:
            thread.join(timeout=2)

    # -- sending (one thread, so handlers never block) ------------------------

    def _send(self, target, msg_type, **payload):
        self._outbox.put((target, msg_type, payload))

    def _sender_loop(self):
        while True:
            item = self._outbox.get()
            if item is None:
                return
            target, msg_type, payload = item
            try:
                self.ctx.ep.send(target, msg_type, **payload)
            except TransportError as exc:
                self.ctx.log.warning("%s to %s failed: %s", msg_type, target, exc)

    def _ensure_route(self, node_id):
        """Make sure we can reach a peer we may only know from gossip."""
        if node_id in self.ctx.ep.routes():
            return True
        member = self.ctx.members.get(node_id)
        if member is None:
            return False
        self.ctx.ep.add_route(node_id, member.host, member.port)
        return True

    def live_peers(self):
        return [m.node_id for m in self.ctx.members.alive("peer")]

    # -- discovery: gossip ----------------------------------------------------

    def _gossip_loop(self):
        """Every 2 heartbeats, push our peer list to one random live peer."""
        interval = 2 * self.ctx.cfg.timing["heartbeat_interval_s"]
        while not self._stop.wait(interval):
            peers = self.live_peers()
            if not peers:
                continue
            target = random.choice(peers)
            entries = [e for e in self.ctx.members.snapshot() if e["role"] == "peer"]
            self._send(target, MsgType.GOSSIP, members=entries)

    def on_gossip(self, msg):
        entries = [e for e in msg["payload"]["members"] if e["role"] == "peer"]
        before = set(self.live_peers())
        for entry in entries:
            if entry["node_id"] != self.ctx.node_id:
                self.ctx.ep.add_route(entry["node_id"], entry["host"], entry["port"])
        self.ctx.members.merge(entries)
        learned = sorted(set(self.live_peers()) - before)
        if learned:
            self.ctx.log.info("GOSSIP from %s: discovered %s", msg["sender"], learned)

    # -- registry: announcements and membership events ------------------------

    def _announce_to(self, targets):
        with self._lock:
            names = sorted(self.store)
        for target in targets:
            self._send(target, MsgType.RESOURCE_ANNOUNCE, resources=names)

    def _peer_joined(self, member):
        if member.role == "peer" and member.node_id != self.ctx.node_id:
            self._ensure_route(member.node_id)
            self._announce_to([member.node_id])

    def _peer_gone(self, member):
        if member.role != "peer":
            return
        with self._lock:
            lost = self.holders.pop(member.node_id, set())
        if lost:
            self.ctx.log.info("REGISTRY dropped %s held by %s (%s)",
                              sorted(lost), member.node_id, member.status)

    def on_resource_announce(self, msg):
        names = set(msg["payload"]["resources"])
        with self._lock:
            self.holders[msg["sender"]] = names
        self.ctx.log.info("ANNOUNCE from %s: holds %s", msg["sender"], sorted(names))

    # -- searching: flooded query with TTL ------------------------------------

    def find(self, name, ttl=None):
        """Flood a RESOURCE_QUERY for name. Returns the query record."""
        query_id = uuid.uuid4().hex[:10]
        query = {"query_id": query_id, "name": name, "started": time.monotonic(),
                 "holder": None, "hits": [], "done": threading.Event(), "content": None}
        with self._lock:
            self._queries[query_id] = query
            self._remember(query_id)
        ttl = self.ttl if ttl is None else ttl
        targets = self.live_peers()
        self.ctx.log.info("QUERY %s for '%s' sent to %s (ttl=%d)", query_id, name, targets, ttl)
        for target in targets:
            self._send(target, MsgType.RESOURCE_QUERY, query_id=query_id, name=name,
                       origin=self.ctx.node_id, ttl=ttl)
        return query

    def _remember(self, query_id):
        """Record a query id; return False if it was already seen. Hold _lock."""
        if query_id in self._seen:
            return False
        self._seen[query_id] = True
        if len(self._seen) > SEEN_QUERIES_MAX:
            self._seen.popitem(last=False)
        return True

    def on_resource_query(self, msg):
        p = msg["payload"]
        with self._lock:
            first_time = self._remember(p["query_id"])
            held = p["name"] in self.store
        if not first_time:
            self.ctx.log.debug("QUERY %s from %s is a duplicate, dropped", p["query_id"], msg["sender"])
            return

        if held and p["origin"] != self.ctx.node_id:
            self._ensure_route(p["origin"])
            self._send(p["origin"], MsgType.RESOURCE_HIT, query_id=p["query_id"],
                       name=p["name"], holder=self.ctx.node_id)
            self.ctx.log.info("QUERY %s for '%s' from %s: I hold it, sent HIT to %s",
                              p["query_id"], p["name"], msg["sender"], p["origin"])

        if p["ttl"] > 1:
            skip = {msg["sender"], p["origin"], self.ctx.node_id}
            targets = [n for n in self.live_peers() if n not in skip]
            for target in targets:
                self._send(target, MsgType.RESOURCE_QUERY, query_id=p["query_id"],
                           name=p["name"], origin=p["origin"], ttl=p["ttl"] - 1)
            if targets:
                self.ctx.log.info("QUERY %s forwarded to %s (ttl=%d)",
                                  p["query_id"], targets, p["ttl"] - 1)

    def on_resource_hit(self, msg):
        p = msg["payload"]
        with self._lock:
            self.holders.setdefault(p["holder"], set()).add(p["name"])
            query = self._queries.get(p["query_id"])
            if query is None:
                return
            query["hits"].append(p["holder"])
            first = query["holder"] is None
            if first:
                query["holder"] = p["holder"]
                self._fetching[p["name"]] = p["query_id"]
        if first:
            took = (time.monotonic() - query["started"]) * 1000
            self.ctx.log.info("HIT for '%s' from %s after %.1f ms, fetching directly",
                              p["name"], p["holder"], took)
            self._ensure_route(p["holder"])
            self._send(p["holder"], MsgType.RESOURCE_FETCH, name=p["name"])
        else:
            self.ctx.log.info("HIT for '%s' from %s ignored, already fetching from %s",
                              p["name"], p["holder"], query["holder"])

    # -- fetching: direct transfer ---------------------------------------------

    def fetch_from(self, holder, name):
        """Fetch name directly from a known holder, without a query."""
        query = {"query_id": "direct-" + uuid.uuid4().hex[:6], "name": name,
                 "started": time.monotonic(), "holder": holder, "hits": [holder],
                 "done": threading.Event(), "content": None}
        with self._lock:
            self._queries[query["query_id"]] = query
            self._fetching[name] = query["query_id"]
        self._ensure_route(holder)
        self._send(holder, MsgType.RESOURCE_FETCH, name=name)
        self.ctx.log.info("FETCH '%s' directly from %s (known from registry)", name, holder)
        return query

    def on_resource_fetch(self, msg):
        name = msg["payload"]["name"]
        with self._lock:
            content = self.store.get(name)
        found = content is not None
        self._send(msg["sender"], MsgType.RESOURCE_DATA, name=name, found=found,
                   content=content if found else "")
        self.ctx.log.info("FETCH '%s' from %s: %s", name, msg["sender"],
                          "sent {} bytes".format(len(content)) if found else "not held")

    def on_resource_data(self, msg):
        p = msg["payload"]
        with self._lock:
            query_id = self._fetching.pop(p["name"], None)
            query = self._queries.get(query_id) if query_id else None
            if p["found"]:
                self.store[p["name"]] = p["content"]
                self.origin_of[p["name"]] = msg["sender"]
        if p["found"]:
            self.ctx.log.info("DATA '%s' received from %s: %d bytes, now held here too",
                              p["name"], msg["sender"], len(p["content"]))
            self._announce_to(self.live_peers())     # we are a holder now
        else:
            self.ctx.log.warning("DATA '%s': %s no longer holds it", p["name"], msg["sender"])
        if query is not None:
            query["content"] = p["content"] if p["found"] else None
            query["done"].set()

    # -- convenience for the console and for the bridge -----------------------

    def get(self, name, timeout=FETCH_TIMEOUT_S):
        """Return the content of name, from here, a known holder, or a query.

        Blocks, so never call it from a handler. Returns None if nobody has it.
        """
        with self._lock:
            if name in self.store:
                return self.store[name]
            live = set(self.live_peers())
            known = sorted(h for h, names in self.holders.items() if name in names and h in live)
        query = self.fetch_from(known[0], name) if known else self.find(name)
        if not query["done"].wait(timeout):
            self.ctx.log.warning("'%s' not found within %.1fs", name, timeout)
            return None
        return query["content"]

    def share(self, name, content):
        """Add a resource at runtime and tell every peer."""
        with self._lock:
            self.store[name] = content
            self.origin_of[name] = self.ctx.node_id
        self._announce_to(self.live_peers())
        self.ctx.log.info("SHARE '%s' (%d bytes), announced to %s",
                          name, len(content), self.live_peers())

    def registry(self):
        """Who holds what, for display: {resource: [holders]}."""
        live = set(self.live_peers())
        table = {}
        with self._lock:
            for name in self.store:
                table.setdefault(name, []).append(self.ctx.node_id + " (me)")
            for holder, names in self.holders.items():
                for name in names:
                    table.setdefault(name, []).append(
                        holder if holder in live else holder + " (gone)")
        return {name: sorted(h) for name, h in sorted(table.items())}


# -- interactive console -------------------------------------------------------

HELP = """commands:
  peers                  live peers this node knows
  ls                     resources held here
  registry               who holds what across the overlay
  find <name>            flood a query and fetch from the first holder
  get <name>             use the registry if a holder is known, else find
  show <name>            print the start of a resource held here
  share <name> <text>    share a new resource with every peer
  submit <kind> <json>   send a job to the cluster through the bridge
  stats                  message counters, for the P2P vs cluster comparison
  help"""


def console(node):
    log = node.ctx.log
    print("console ready, type 'help'", flush=True)
    for line in sys.stdin:
        parts = line.strip().split(maxsplit=2)
        if not parts:
            continue
        cmd, args = parts[0].lower(), parts[1:]
        try:
            if cmd == "help":
                print(HELP, flush=True)
            elif cmd == "peers":
                log.info("live peers: %s", node.live_peers())
            elif cmd == "ls":
                with node._lock:
                    held = {n: "{} bytes, from {}".format(len(c), node.origin_of[n])
                            for n, c in sorted(node.store.items())}
                log.info("held here: %s", held)
            elif cmd == "registry":
                for name, holders in node.registry().items():
                    log.info("  %-14s %s", name, ", ".join(holders))
            elif cmd in ("find", "get") and args:
                started = time.monotonic()
                if cmd == "find":
                    query = node.find(args[0])
                    content = query["content"] if query["done"].wait(FETCH_TIMEOUT_S) else None
                else:
                    content = node.get(args[0])
                took = (time.monotonic() - started) * 1000
                if content is None:
                    log.info("'%s' not found (%.1f ms)", args[0], took)
                else:
                    log.info("'%s' ready: %d bytes in %.1f ms", args[0], len(content), took)
            elif cmd == "show" and args:
                with node._lock:
                    content = node.store.get(args[0])
                print(content[:300] if content is not None else "not held here", flush=True)
            elif cmd == "share" and len(args) == 2:
                node.share(args[0], args[1])
            elif cmd == "submit" and args:
                params = json.loads(args[1]) if len(args) > 1 else {}
                job_id = node.bridge.submit(args[0], params)
                log.info("submitted job %s to the cluster", job_id)
            elif cmd == "stats":
                log.info("stats: %s", node.ctx.ep.stats())
            else:
                print("unknown command, type 'help'", flush=True)
        except NotImplementedError as exc:
            log.warning("not available yet: %s", exc)
        except Exception as exc:                       # keep the console alive
            log.warning("%s failed: %s", cmd, exc)


def _console_wanted(args):
    if args.no_console or not sys.stdin.isatty():
        return False
    try:
        # A background job (started with &) must not read the terminal.
        return os.tcgetpgrp(sys.stdin.fileno()) == os.getpgrp()
    except (AttributeError, OSError):
        return True                                   # Windows


def main():
    parser = node_arg_parser("Peer node")
    parser.add_argument("--share", action="append", default=[], metavar="NAME=FILE",
                        help="also share the contents of FILE as NAME (repeatable)")
    parser.add_argument("--ttl", type=int, default=DEFAULT_TTL, help="hops a query may travel")
    parser.add_argument("--no-console", action="store_true", help="do not read commands from stdin")
    args = parser.parse_args()

    ctx = build_node(args, "peer")
    resources = demo_resources(args.id)
    for item in args.share:
        name, _, path = item.partition("=")
        with open(path, "r", encoding="utf-8") as handle:
            resources[name] = handle.read()

    node = PeerNode(ctx, resources, ttl=args.ttl)
    ctx.ep.start()
    node.membership.start()
    node.start()
    if _console_wanted(args):
        threading.Thread(target=console, args=(node,), daemon=True).start()
    run_until_stopped(ctx, on_stop=node.stop)


if __name__ == "__main__":
    main()
