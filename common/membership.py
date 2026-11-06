"""Thread-safe view of which nodes exist. Data structure and rules only.

This module does no networking. joinleave/service.py decides when to send
heartbeats and how to react; it drives this table and registers callbacks.

Each node picks a new `incarnation` every time it starts, so a restarted node
always overrides stale news about its previous life. When two reports share
an incarnation, ALIVE < DEAD < LEFT: bad news spreads and is not undone by an
older ALIVE report. A direct message from a DEAD node does revive it, and a
live node that hears it is reported DEAD or LEFT raises its own incarnation,
so its next gossip overrides the rumour.

Callbacks run on whichever thread caused the change, which may be the
heartbeat thread rather than the dispatcher. Guard any state they share.
"""

import logging
import threading
import time
from dataclasses import dataclass


class Status:
    ALIVE = "ALIVE"
    DEAD = "DEAD"
    LEFT = "LEFT"


_RANK = {Status.ALIVE: 0, Status.DEAD: 1, Status.LEFT: 2}


@dataclass
class Member:
    node_id: str
    host: str
    port: int
    role: str
    incarnation: int
    status: str
    last_seen: float

    def to_entry(self):
        return {
            "node_id": self.node_id, "host": self.host, "port": self.port,
            "role": self.role, "incarnation": self.incarnation, "status": self.status,
        }


class MembershipTable:
    def __init__(self, self_id, failure_timeout_s, clock=time.monotonic):
        self.self_id = self_id
        self.failure_timeout_s = failure_timeout_s
        self._clock = clock
        self._members = {}
        self._lock = threading.Lock()
        self._join_handlers = []
        self._leave_handlers = []
        self._fail_handlers = []

    # -- callbacks: each receives a copy of the Member -----------------------

    def on_join(self, handler):
        """A node became ALIVE: it is new, restarted, or was wrongly thought dead."""
        self._join_handlers.append(handler)

    def on_leave(self, handler):
        """A node left gracefully."""
        self._leave_handlers.append(handler)

    def on_fail(self, handler):
        """A node crashed or stopped responding."""
        self._fail_handlers.append(handler)

    def _fire(self, events):
        for handlers, member in events:
            for handler in handlers:
                try:
                    handler(member)
                except Exception:
                    # Must not escape into the caller, which may be the
                    # heartbeat thread.
                    logging.getLogger(self.self_id).exception(
                        "membership callback failed for %s", member.node_id)

    # -- updates -------------------------------------------------------------

    def add(self, node_id, host, port, role, incarnation):
        """Record a node that announced itself directly."""
        self.merge([{
            "node_id": node_id, "host": host, "port": port, "role": role,
            "incarnation": incarnation, "status": Status.ALIVE,
        }])
        self.touch(node_id)

    def touch(self, node_id):
        """Record direct contact. Unknown and LEFT nodes are ignored."""
        events = []
        with self._lock:
            member = self._members.get(node_id)
            if member is None or member.status == Status.LEFT:
                return
            member.last_seen = self._clock()
            if member.status == Status.DEAD:
                member.status = Status.ALIVE
                events.append((self._join_handlers, self._copy(member)))
        self._fire(events)

    def mark_left(self, node_id):
        self._set_status(node_id, Status.LEFT, self._leave_handlers)

    def mark_dead(self, node_id):
        self._set_status(node_id, Status.DEAD, self._fail_handlers)

    def _set_status(self, node_id, status, handlers):
        events = []
        with self._lock:
            member = self._members.get(node_id)
            if (member is None or node_id == self.self_id
                    or _RANK[member.status] >= _RANK[status]):
                return
            member.status = status
            events.append((handlers, self._copy(member)))
        self._fire(events)

    def sweep(self):
        """Mark silent nodes DEAD and return their ids. Call this periodically."""
        now = self._clock()
        events = []
        with self._lock:
            for member in self._members.values():
                if (member.node_id != self.self_id and member.status == Status.ALIVE
                        and now - member.last_seen > self.failure_timeout_s):
                    member.status = Status.DEAD
                    events.append((self._fail_handlers, self._copy(member)))
        self._fire(events)
        return [member.node_id for _handlers, member in events]

    def merge(self, entries):
        """Apply entries from snapshot(), such as those in a GOSSIP message."""
        events = []
        with self._lock:
            for entry in entries:
                event = self._merge_one(entry)
                if event is not None:
                    events.append(event)
        self._fire(events)

    def _merge_one(self, entry):
        node_id = entry["node_id"]
        current = self._members.get(node_id)
        if current is not None:
            if node_id == self.self_id:
                # Nobody knows this node better than itself: refute the rumour.
                if entry["status"] != Status.ALIVE:
                    current.incarnation = max(current.incarnation, entry["incarnation"]) + 1
                return None
            newer = entry["incarnation"] > current.incarnation
            worse = (entry["incarnation"] == current.incarnation
                     and _RANK[entry["status"]] > _RANK[current.status])
            if not (newer or worse):
                return None

        previous = current.status if current is not None else None
        self._members[node_id] = Member(
            node_id, entry["host"], entry["port"], entry["role"],
            entry["incarnation"], entry["status"], self._clock())
        member = self._copy(self._members[node_id])

        if member.status == previous:
            return None
        if member.status == Status.ALIVE:
            return self._join_handlers, member
        if previous is None:
            return None         # First heard of a node that is already gone.
        if member.status == Status.LEFT:
            return self._leave_handlers, member
        return self._fail_handlers, member

    # -- queries -------------------------------------------------------------

    def get(self, node_id):
        with self._lock:
            member = self._members.get(node_id)
            return self._copy(member) if member else None

    def alive(self, role=None, include_self=False):
        """ALIVE members, sorted by id."""
        with self._lock:
            found = [
                self._copy(m) for m in self._members.values()
                if m.status == Status.ALIVE
                and (role is None or m.role == role)
                and (include_self or m.node_id != self.self_id)
            ]
        return sorted(found, key=lambda m: m.node_id)

    def snapshot(self):
        """JSON-safe list of every member, for JOIN_ACK and GOSSIP."""
        with self._lock:
            return [m.to_entry() for m in self._members.values()]

    @staticmethod
    def _copy(member):
        return Member(**vars(member))
