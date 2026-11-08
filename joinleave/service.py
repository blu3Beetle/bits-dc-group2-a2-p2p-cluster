"""Join and leave handling for every node. Owner: Sohail.

Every node (peer, master, worker) runs one MembershipService. It decides when
to send JOIN, HEARTBEAT and LEAVE, and drives ctx.members, which holds the
table and the ALIVE/DEAD/LEFT rules.

Who tracks whom:

    peer    joins the seeds, then heartbeats every other ALIVE peer
    worker  joins the master, then heartbeats the master
    master  joins nobody, heartbeats every ALIVE worker

So the P2P overlay and the cluster each have their own membership, which is
the point of comparing them. A worker never tracks other workers, because
they never heartbeat each other and would all look dead.

Failure detection has two paths:
  * fast: the TCP channel from a node closes (crash, kill -9), and
    build_node() marks it DEAD at once;
  * slow: a node goes silent with its socket still open (hung, network cut),
    and sweep() marks it DEAD after failure_timeout_s.

A graceful exit (Ctrl+C) sends LEAVE first, so others record LEFT, not DEAD.

Threads: JOIN is retried on its own thread, because connecting to a node that
is not up yet can block for a couple of seconds and must not delay
heartbeats. Heartbeat and sweep run on a second thread. Handlers run on the
dispatcher thread and never block.
"""

import threading
import time

from common import MsgType, Status, TransportError

# Roles each role keeps in its membership table.
WATCHES = {
    "peer": ("peer",),
    "worker": ("master",),
    "master": ("worker",),
}


class MembershipService:
    def __init__(self, ctx, on_worker_lost=None):
        """on_worker_lost(worker_id) runs on the master when a worker leaves or
        fails, for example Master.requeue_worker_tasks. It may run on the
        heartbeat thread, so guard any state it shares with handlers."""
        self.ctx = ctx
        self.on_worker_lost = on_worker_lost
        self.watches = WATCHES[ctx.role]

        self._stop = threading.Event()
        self._joined = threading.Event()
        self._lock = threading.Lock()
        self._announced = set()     # nodes that already know about us
        self._threads = []

        ctx.ep.on(MsgType.JOIN, self.on_join)
        ctx.ep.on(MsgType.JOIN_ACK, self.on_join_ack)
        ctx.ep.on(MsgType.LEAVE, self.on_leave)
        ctx.ep.on(MsgType.HEARTBEAT, self.on_heartbeat)

        ctx.members.on_join(self._member_joined)
        ctx.members.on_leave(self._member_left)
        ctx.members.on_fail(self._member_failed)

    # -- lifecycle ------------------------------------------------------------

    def start(self):
        if not self.join_targets():
            self._joined.set()          # The master has nobody to join.
        for target in (self._join_loop, self._heartbeat_loop):
            thread = threading.Thread(target=target, daemon=True,
                                      name="{}-{}".format(self.ctx.node_id, target.__name__))
            thread.start()
            self._threads.append(thread)
        self.ctx.log.info("membership started: role=%s incarnation=%s watching=%s",
                          self.ctx.role, self.ctx.incarnation, ",".join(self.watches))

    def stop(self):
        """Graceful leave: tell everyone we track, then stop the threads."""
        if self._stop.is_set():
            return
        self._stop.set()
        targets = self.heartbeat_targets()
        for node_id in targets:
            self._send_quietly(node_id, MsgType.LEAVE, reason="graceful shutdown")
        self.ctx.log.info("membership stopped: sent LEAVE to %s", targets or "nobody")
        for thread in self._threads:
            thread.join(timeout=2)

    # -- who to talk to -------------------------------------------------------

    def join_targets(self):
        cfg = self.ctx.cfg
        if self.ctx.role == "peer":
            return [s for s in cfg.seeds if s != self.ctx.node_id]
        if self.ctx.role == "worker":
            return [cfg.master_id]
        return []

    def heartbeat_targets(self):
        return [m.node_id for role in self.watches for m in self.ctx.members.alive(role)]

    # -- background threads ---------------------------------------------------

    def _join_loop(self):
        """Send JOIN until a JOIN_ACK arrives, and again if we lose everyone."""
        interval = self.ctx.cfg.timing["heartbeat_interval_s"]
        attempts = 0
        while not self._stop.is_set():
            if not self._joined.is_set():
                attempts += 1
                for target in self.join_targets():
                    if self._send_join(target, quiet=attempts > 1):
                        break
            if self._stop.wait(interval):
                return

    def _heartbeat_loop(self):
        interval = self.ctx.cfg.timing["heartbeat_interval_s"]
        while not self._stop.wait(interval):
            for node_id in self.heartbeat_targets():
                self._send_quietly(node_id, MsgType.HEARTBEAT,
                                   incarnation=self.ctx.incarnation)
            self.ctx.members.sweep()

    # -- handlers (dispatcher thread) -----------------------------------------

    def on_join(self, msg):
        p = msg["payload"]
        sender = msg["sender"]
        self.ctx.ep.add_route(sender, p["host"], p["port"])
        self.ctx.members.add(sender, p["host"], p["port"], p["role"], p["incarnation"])
        with self._lock:
            self._announced.add(sender)
        self._send_quietly(sender, MsgType.JOIN_ACK, members=self.ctx.members.snapshot())
        self.ctx.log.info("JOIN from %s (%s at %s:%s), sent JOIN_ACK",
                          sender, p["role"], p["host"], p["port"])

    def on_join_ack(self, msg):
        entries = [e for e in msg["payload"]["members"]
                   if e["node_id"] == self.ctx.node_id or e["role"] in self.watches]
        for entry in entries:
            if entry["node_id"] != self.ctx.node_id:
                self.ctx.ep.add_route(entry["node_id"], entry["host"], entry["port"])
        self.ctx.members.merge(entries)
        self._joined.set()
        self.ctx.log.info("JOIN_ACK from %s: now tracking %s",
                          msg["sender"], self.heartbeat_targets())

        # Nodes we learned about here have never heard of us. Announce to them
        # off the dispatcher thread, since a first connection can block.
        with self._lock:
            new = [e["node_id"] for e in entries
                   if e["status"] == Status.ALIVE and e["node_id"] != self.ctx.node_id
                   and e["node_id"] != msg["sender"] and e["node_id"] not in self._announced]
            # Claimed now, so two JOIN_ACKs arriving together announce once.
            self._announced.update(new)
            self._announced.add(msg["sender"])
        if new:
            threading.Thread(target=self._announce, args=(new,), daemon=True).start()

    def on_leave(self, msg):
        self.ctx.log.info("LEAVE from %s: %s", msg["sender"], msg["payload"]["reason"])
        self.ctx.members.mark_left(msg["sender"])

    def on_heartbeat(self, msg):
        sender = msg["sender"]
        member = self.ctx.members.get(sender)
        if member is None:
            # Contact from a node that never joined us. It will JOIN, or we
            # will learn it from a JOIN_ACK or gossip.
            self.ctx.log.debug("heartbeat from unknown node %s ignored", sender)
            return
        incarnation = msg["payload"]["incarnation"]
        if incarnation > member.incarnation:
            # A restart, or the node refuting a rumour that it was dead.
            self.ctx.log.info("%s incarnation %s -> %s", sender, member.incarnation, incarnation)
            self.ctx.members.merge([{
                "node_id": sender, "host": member.host, "port": member.port,
                "role": member.role, "incarnation": incarnation, "status": Status.ALIVE,
            }])

    # -- membership events (any thread) ---------------------------------------

    def _member_joined(self, member):
        if member.role in self.watches:
            self.ctx.log.info("MEMBER JOINED %s (%s). Alive %ss: %s",
                              member.node_id, member.role, member.role,
                              self._alive_ids(member.role))

    def _member_left(self, member):
        self._lost(member, "LEFT", "graceful leave")

    def _member_failed(self, member):
        silent = time.monotonic() - member.last_seen
        self._lost(member, "FAILED", "crashed or silent, last heard {:.1f}s ago".format(silent))

    def _lost(self, member, verb, why):
        if member.role not in self.watches:
            return
        with self._lock:
            self._announced.discard(member.node_id)
        self.ctx.log.warning("MEMBER %s %s (%s): %s. Alive %ss: %s",
                             verb, member.node_id, member.role, why, member.role,
                             self._alive_ids(member.role))
        if member.role == "worker" and self.on_worker_lost is not None:
            self.on_worker_lost(member.node_id)
        if self.join_targets() and not self.heartbeat_targets():
            # Lost everyone we track, such as a worker whose master went away.
            self.ctx.log.warning("no live %ss left, will JOIN again", "/".join(self.watches))
            self._joined.clear()

    # -- helpers --------------------------------------------------------------

    def _announce(self, node_ids):
        for node_id in node_ids:
            self._send_join(node_id, quiet=False)

    def _send_join(self, target, quiet):
        ok = self._send_quietly(target, MsgType.JOIN, warn=not quiet,
                                host=self.ctx.ep.host, port=self.ctx.ep.port,
                                role=self.ctx.role, incarnation=self.ctx.incarnation)
        with self._lock:
            if ok:
                self._announced.add(target)
            else:
                self._announced.discard(target)
        if ok:
            self.ctx.log.info("sent JOIN to %s", target)
        return ok

    def _send_quietly(self, target, msg_type, warn=False, **payload):
        """Send, and report failure as False: nodes can vanish at any time."""
        try:
            self.ctx.ep.send(target, msg_type, **payload)
            return True
        except TransportError as exc:
            log = self.ctx.log.warning if warn else self.ctx.log.debug
            log("%s to %s failed: %s", msg_type, target, exc)
            return False

    def _alive_ids(self, role):
        return [m.node_id for m in self.ctx.members.alive(role, include_self=True)]
