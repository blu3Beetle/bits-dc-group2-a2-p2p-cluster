"""Tests for peer/peer_node.py: discovery, registry and direct P2P messaging.

Real PeerNodes run as threads in this process and talk over real TCP on
127.0.0.1, with short timings so the suite is quick.

    python3 -m unittest tests.test_peer -v
"""

import logging
import time
import unittest
from types import SimpleNamespace

from common import TransportError, build_node
from peer.peer_node import PeerNode, demo_resources
from tests.test_joinleave import JoinLeaveCase, free_port, wait_until


class PeerCase(JoinLeaveCase):
    def start_peer(self, name, resources=None, port=None):
        args = SimpleNamespace(id=self.id_(name), port=port, host=None,
                               config=self.config_path, verbose=False)
        ctx = build_node(args, "peer")
        for handler in ctx.log.handlers:
            handler.close()
        ctx.log.handlers[:] = [logging.NullHandler()]
        node = PeerNode(ctx, resources if resources is not None else {})
        for attempt in range(10):
            try:
                ctx.ep.start()
                break
            except TransportError:
                if attempt == 9:
                    raise
                time.sleep(0.1)
        node.membership.start()
        node.start()
        self.addCleanup(lambda: (node._stop.set(), node._outbox.put(None),
                                 node.membership._stop.set(), ctx.ep.stop()))
        return node

    def start_four(self):
        peers = [self.start_peer("peer{}".format(i), {"res-{}".format(i): "content {}".format(i)})
                 for i in range(1, 5)]
        ids = [p.ctx.node_id for p in peers]
        for peer in peers:
            others = [i for i in ids if i != peer.ctx.node_id]
            self.assertTrue(wait_until(lambda: peer.live_peers() == others))
        return peers


class DiscoveryTests(PeerCase):
    def test_four_peers_discover_each_other(self):
        self.start_four()

    def test_gossip_spreads_a_late_peer(self):
        peers = self.start_four()
        late = self.start_peer("peer5", port=free_port())
        for peer in peers:
            self.assertTrue(wait_until(lambda: late.ctx.node_id in peer.live_peers()))


class RegistryTests(PeerCase):
    def test_announcements_fill_every_registry(self):
        peers = self.start_four()
        for peer in peers:
            self.assertTrue(wait_until(lambda: len(peer.registry()) == 4), peer.registry())

    def test_departed_peer_is_dropped_from_registry(self):
        peers = self.start_four()
        self.assertTrue(wait_until(lambda: "res-4" in peers[0].registry()))
        peers[3].membership.stop()
        peers[3].ctx.ep.stop()
        self.assertTrue(wait_until(lambda: "res-4" not in peers[0].registry()))


class MessagingTests(PeerCase):
    def test_query_finds_holder_and_fetches_directly(self):
        peers = self.start_four()
        query = peers[0].find("res-3")
        self.assertTrue(query["done"].wait(5))
        self.assertEqual(query["holder"], peers[2].ctx.node_id)
        self.assertEqual(query["content"], "content 3")
        self.assertIn("res-3", peers[0].store)

    def test_missing_resource_returns_none(self):
        peers = self.start_four()
        self.assertIsNone(peers[0].get("nothing", timeout=1))

    def test_duplicate_queries_are_answered_once(self):
        peers = self.start_four()
        query = peers[0].find("res-4")
        self.assertTrue(query["done"].wait(5))
        time.sleep(0.3)
        self.assertEqual(query["hits"], [peers[3].ctx.node_id])

    def test_fetched_copy_survives_original_holder_leaving(self):
        peers = self.start_four()
        self.assertEqual(peers[0].get("res-4"), "content 4")
        peers[3].membership.stop()
        peers[3].ctx.ep.stop()
        self.assertTrue(wait_until(lambda: peers[3].ctx.node_id not in peers[1].live_peers()))
        self.assertEqual(peers[1].get("res-4"), "content 4")

    def test_runtime_share_is_found_by_others(self):
        peers = self.start_four()
        peers[1].share("new-file", "hello")
        self.assertEqual(peers[2].get("new-file"), "hello")


class DemoDataTests(unittest.TestCase):
    def test_each_configured_peer_has_its_dataset(self):
        for i, letter in enumerate("abcd", 1):
            self.assertIn("dataset-" + letter, demo_resources("peer{}".format(i)))
        self.assertEqual(list(demo_resources("peer5")), ["notes-peer5"])


if __name__ == "__main__":
    unittest.main()
