"""Tests for joinleave/service.py: node join, leave, failure and rejoin.

Every test starts real nodes as threads in this process, talking over real
TCP on 127.0.0.1, built with the same build_node() the entry points use.
Timing is shortened (heartbeat 0.1s, failure timeout 0.5s) so the suite is
quick; the logic is the same as with the defaults.

    python3 -m unittest tests.test_joinleave -v
"""

import itertools
import json
import logging
import os
import socket
import tempfile
import time
import unittest
from types import SimpleNamespace

from common import Status, TransportError, build_node
from joinleave.service import MembershipService

HEARTBEAT_S = 0.1
TIMEOUT_S = 0.5
WAIT_S = 5
_run = itertools.count(1)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until(condition, timeout=WAIT_S):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return condition()


class Node:
    """One running node: its context, its service and the events it saw."""

    def __init__(self, ctx, service):
        self.ctx = ctx
        self.service = service
        self.events = []
        self.lost_workers = []
        ctx.members.on_join(lambda m: self.events.append(("join", m.node_id)))
        ctx.members.on_leave(lambda m: self.events.append(("leave", m.node_id)))
        ctx.members.on_fail(lambda m: self.events.append(("fail", m.node_id)))

    def alive(self, role=None):
        return [m.node_id for m in self.ctx.members.alive(role)]

    def status_of(self, node_id):
        member = self.ctx.members.get(node_id)
        return member.status if member else None

    def leave(self):
        """Ctrl+C: LEAVE, then close."""
        self.service.stop()
        self.ctx.ep.stop()

    def crash(self):
        """kill -9: sockets close with no LEAVE."""
        self.service._stop.set()
        self.ctx.ep.stop()

    def hang(self):
        """Stops sending, but its sockets stay open, so only the timeout notices."""
        self.service._stop.set()


class JoinLeaveCase(unittest.TestCase):
    """Builds a topology like config/nodes.json, with free ports and fast timing."""

    PEERS = 4
    WORKERS = 3

    def setUp(self):
        self.prefix = "t{}-".format(next(_run))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        os.environ["DC_LOG_DIR"] = tmp.name

        nodes = {}
        for i in range(1, self.PEERS + 1):
            nodes[self.id_("peer{}".format(i))] = {"role": "peer", "host": "127.0.0.1", "port": free_port()}
        nodes[self.id_("master")] = {"role": "master", "host": "127.0.0.1", "port": free_port()}
        for i in range(1, self.WORKERS + 1):
            nodes[self.id_("worker{}".format(i))] = {"role": "worker", "host": "127.0.0.1", "port": free_port()}
        self.config_path = os.path.join(tmp.name, "nodes.json")
        with open(self.config_path, "w") as handle:
            json.dump({
                "nodes": nodes,
                "seeds": [self.id_("peer1"), self.id_("peer2")],
                "timing": {"heartbeat_interval_s": HEARTBEAT_S, "failure_timeout_s": TIMEOUT_S},
            }, handle)

    def id_(self, name):
        return self.prefix + name

    def start(self, name, role, port=None):
        args = SimpleNamespace(id=self.id_(name), port=port, host=None,
                               config=self.config_path, verbose=False)
        ctx = build_node(args, role)
        for handler in ctx.log.handlers:                  # keep test output clean
            handler.close()
        ctx.log.handlers[:] = [logging.NullHandler()]
        node = Node(ctx, None)
        node.service = MembershipService(ctx, on_worker_lost=node.lost_workers.append)
        # A node restarted inside this process can find its port still held for
        # one accept-poll (0.2s) by the old listener thread. Real restarts are
        # new processes, so this only affects the tests.
        for attempt in range(10):
            try:
                ctx.ep.start()
                break
            except TransportError:
                if attempt == 9:
                    raise
                time.sleep(0.1)
        node.service.start()
        self.addCleanup(node.crash)
        return node

    def start_cluster(self, workers=WORKERS):
        master = self.start("master", "master")
        nodes = [self.start("worker{}".format(i), "worker") for i in range(1, workers + 1)]
        ids = [self.id_("worker{}".format(i)) for i in range(1, workers + 1)]
        self.assertTrue(wait_until(lambda: master.alive("worker") == ids))
        return master, nodes

    def start_peers(self, count=PEERS):
        peers = [self.start("peer{}".format(i), "peer") for i in range(1, count + 1)]
        ids = [self.id_("peer{}".format(i)) for i in range(1, count + 1)]
        for peer in peers:
            others = [i for i in ids if i != peer.ctx.node_id]
            self.assertTrue(wait_until(lambda: peer.alive("peer") == others),
                            "{} sees {}".format(peer.ctx.node_id, peer.alive("peer")))
        return peers


class ClusterJoinTests(JoinLeaveCase):
    def test_workers_join_master_and_track_only_the_master(self):
        master, workers = self.start_cluster()
        for worker in workers:
            self.assertTrue(wait_until(lambda: worker.alive() == [self.id_("master")]))

    def test_no_false_failures_while_everyone_is_healthy(self):
        master, workers = self.start_cluster()
        time.sleep(TIMEOUT_S * 3)
        for node in [master] + workers:
            self.assertNotIn("fail", [kind for kind, _ in node.events], node.ctx.node_id)

    def test_worker_started_before_master_joins_when_master_arrives(self):
        worker = self.start("worker1", "worker")
        time.sleep(HEARTBEAT_S * 3)
        master = self.start("master", "master")
        self.assertTrue(wait_until(lambda: master.alive("worker") == [self.id_("worker1")]))

    def test_new_worker_joins_at_runtime_on_a_port_not_in_config(self):
        master, _ = self.start_cluster()
        self.start("worker4", "worker", port=free_port())
        self.assertTrue(wait_until(lambda: self.id_("worker4") in master.alive("worker")))
        self.assertIn(("join", self.id_("worker4")), master.events)


class ClusterLeaveTests(JoinLeaveCase):
    def test_graceful_leave_is_left_not_failed(self):
        master, workers = self.start_cluster()
        workers[1].leave()
        w2 = self.id_("worker2")
        self.assertTrue(wait_until(lambda: master.status_of(w2) == Status.LEFT))
        time.sleep(TIMEOUT_S * 2)
        self.assertEqual(master.status_of(w2), Status.LEFT)
        self.assertNotIn(("fail", w2), master.events)
        self.assertEqual(master.lost_workers, [w2])

    def test_crash_is_detected_from_the_closed_channel(self):
        master, workers = self.start_cluster()
        started = time.monotonic()
        workers[0].crash()
        w1 = self.id_("worker1")
        self.assertTrue(wait_until(lambda: master.status_of(w1) == Status.DEAD))
        self.assertLess(time.monotonic() - started, TIMEOUT_S)
        self.assertEqual(master.lost_workers, [w1])

    def test_silent_node_is_detected_by_timeout(self):
        master, workers = self.start_cluster()
        started = time.monotonic()
        workers[2].hang()
        w3 = self.id_("worker3")
        self.assertTrue(wait_until(lambda: master.status_of(w3) == Status.DEAD))
        self.assertGreaterEqual(time.monotonic() - started, TIMEOUT_S - HEARTBEAT_S)
        self.assertEqual(master.lost_workers, [w3])

    def test_remaining_workers_are_unaffected_by_a_removal(self):
        master, workers = self.start_cluster()
        workers[0].crash()
        self.assertTrue(wait_until(lambda: master.alive("worker") == [self.id_("worker2"), self.id_("worker3")]))
        time.sleep(TIMEOUT_S * 2)
        self.assertEqual(master.alive("worker"), [self.id_("worker2"), self.id_("worker3")])

    def test_crashed_worker_restarts_and_rejoins(self):
        master, workers = self.start_cluster()
        old_incarnation = workers[0].ctx.incarnation
        workers[0].crash()
        w1 = self.id_("worker1")
        self.assertTrue(wait_until(lambda: master.status_of(w1) == Status.DEAD))
        time.sleep(0.01)    # incarnation is a millisecond timestamp
        reborn = self.start("worker1", "worker")
        self.assertGreater(reborn.ctx.incarnation, old_incarnation)
        self.assertTrue(wait_until(lambda: master.status_of(w1) == Status.ALIVE))

    def test_workers_rejoin_after_master_restart(self):
        master, workers = self.start_cluster()
        master.crash()
        for worker in workers:
            self.assertTrue(wait_until(lambda: worker.alive() == []))
        reborn = self.start("master", "master")
        ids = [self.id_("worker{}".format(i)) for i in range(1, 4)]
        self.assertTrue(wait_until(lambda: reborn.alive("worker") == ids))


class PeerMembershipTests(JoinLeaveCase):
    def test_four_peers_discover_each_other_through_seeds(self):
        self.start_peers()

    def test_peers_do_not_track_the_cluster(self):
        peers = self.start_peers()
        self.start_cluster()
        time.sleep(TIMEOUT_S)
        for peer in peers:
            self.assertEqual(peer.alive("worker") + peer.alive("master"), [])

    def test_late_peer_is_learned_by_every_existing_peer(self):
        peers = self.start_peers()
        self.start("peer5", "peer", port=free_port())
        p5 = self.id_("peer5")
        for peer in peers:
            self.assertTrue(wait_until(lambda: p5 in peer.alive("peer")), peer.ctx.node_id)

    def test_peer_leaving_is_seen_as_left_by_all_others(self):
        peers = self.start_peers()
        peers[3].leave()
        p4 = self.id_("peer4")
        for peer in peers[:3]:
            self.assertTrue(wait_until(lambda: peer.status_of(p4) == Status.LEFT))
            self.assertNotIn(("fail", p4), peer.events)

    def test_peers_keep_working_after_a_seed_crashes(self):
        peers = self.start_peers()
        peers[0].crash()
        p1 = self.id_("peer1")
        rest = [self.id_("peer{}".format(i)) for i in (2, 3, 4)]
        for peer in peers[1:]:
            others = [i for i in rest if i != peer.ctx.node_id]
            self.assertTrue(wait_until(lambda: peer.status_of(p1) == Status.DEAD))
            self.assertTrue(wait_until(lambda: peer.alive("peer") == others))


if __name__ == "__main__":
    unittest.main()
