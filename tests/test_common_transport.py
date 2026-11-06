import logging
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import unittest

from common import Endpoint, MsgType, SchemaError, TransportError, make_message
from common.transport import HELLO_KEY, _encode_frame

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAIT_S = 5

logging.getLogger("transport-test").addHandler(logging.NullHandler())
QUIET = logging.getLogger("transport-test")
QUIET.propagate = False

CHILD = """
import logging, sys, time
from common import Endpoint, MsgType
logging.disable(logging.CRITICAL)
ep = Endpoint("child", "127.0.0.1", int(sys.argv[2]),
              routes={"parent": ("127.0.0.1", int(sys.argv[1]))}).start()
print(ep.port, flush=True)
ep.send("parent", MsgType.HEARTBEAT, incarnation=1)
time.sleep(60)
"""


class TransportCase(unittest.TestCase):
    def endpoint(self, node_id, routes=None, port=0):
        ep = Endpoint(node_id, "127.0.0.1", port, routes=routes, logger=QUIET,
                      connect_timeout=0.3)
        ep.start()
        self.addCleanup(ep.stop)
        return ep

    def collect(self, ep, msg_type):
        received = queue.Queue()
        ep.on(msg_type, received.put)
        return received

    def pair(self):
        receiver = self.endpoint("b")
        sender = self.endpoint("a", routes={"b": ("127.0.0.1", receiver.port)})
        return sender, receiver


class DeliveryTests(TransportCase):
    def test_message_arrives_with_payload_and_sender(self):
        sender, receiver = self.pair()
        inbox = self.collect(receiver, MsgType.RESOURCE_FETCH)
        msg_id = sender.send("b", MsgType.RESOURCE_FETCH, name="data")
        msg = inbox.get(timeout=WAIT_S)
        self.assertEqual((msg["msg_id"], msg["sender"]), (msg_id, "a"))
        self.assertEqual(msg["payload"], {"name": "data"})

    def test_burst_of_large_messages_stays_whole_and_in_order(self):
        sender, receiver = self.pair()
        inbox = self.collect(receiver, MsgType.RESOURCE_DATA)
        padding = "x" * 20000
        for seq in range(300):
            sender.send("b", MsgType.RESOURCE_DATA, name=str(seq), found=True, content=padding)
        got = [inbox.get(timeout=WAIT_S)["payload"] for _ in range(300)]
        self.assertEqual([int(p["name"]) for p in got], list(range(300)))
        self.assertTrue(all(p["content"] == padding for p in got))

    def test_concurrent_senders_do_not_corrupt_frames(self):
        sender, receiver = self.pair()
        inbox = self.collect(receiver, MsgType.RESOURCE_DATA)

        def blast(prefix):
            for seq in range(100):
                sender.send("b", MsgType.RESOURCE_DATA, name="{}{}".format(prefix, seq),
                            found=True, content="y" * 5000)

        threads = [threading.Thread(target=blast, args=(p,)) for p in "abcd"]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        names = {inbox.get(timeout=WAIT_S)["payload"]["name"] for _ in range(400)}
        self.assertEqual(len(names), 400)

    def test_receiver_can_reply_without_a_configured_route(self):
        sender, receiver = self.pair()
        replies = self.collect(sender, MsgType.RESOURCE_DATA)
        receiver.on(MsgType.RESOURCE_FETCH, lambda msg: receiver.send(
            msg["sender"], MsgType.RESOURCE_DATA, name="d", found=True, content="c"))
        sender.send("b", MsgType.RESOURCE_FETCH, name="d")
        self.assertEqual(replies.get(timeout=WAIT_S)["sender"], "b")

    def test_on_any_runs_before_the_type_handler(self):
        sender, receiver = self.pair()
        order = queue.Queue()
        receiver.on_any(lambda msg: order.put("any"))
        receiver.on(MsgType.RESOURCE_FETCH, lambda msg: order.put("type"))
        sender.send("b", MsgType.RESOURCE_FETCH, name="d")
        self.assertEqual([order.get(timeout=WAIT_S) for _ in range(2)], ["any", "type"])

    def test_failing_handler_does_not_stop_later_messages(self):
        sender, receiver = self.pair()
        inbox = queue.Queue()

        def handler(msg):
            if msg["payload"]["name"] == "boom":
                raise RuntimeError("expected by this test")
            inbox.put(msg)

        receiver.on(MsgType.RESOURCE_FETCH, handler)
        sender.send("b", MsgType.RESOURCE_FETCH, name="boom")
        sender.send("b", MsgType.RESOURCE_FETCH, name="fine")
        self.assertEqual(inbox.get(timeout=WAIT_S)["payload"]["name"], "fine")

    def test_stats_count_messages(self):
        sender, receiver = self.pair()
        inbox = self.collect(receiver, MsgType.RESOURCE_FETCH)
        for _ in range(3):
            sender.send("b", MsgType.RESOURCE_FETCH, name="d")
        for _ in range(3):
            inbox.get(timeout=WAIT_S)
        self.assertEqual(sender.stats()["sent"], {MsgType.RESOURCE_FETCH: 3})
        self.assertGreater(sender.stats()["bytes_sent"], 0)
        self.assertEqual(receiver.stats()["received"], {MsgType.RESOURCE_FETCH: 3})


class RejectionTests(TransportCase):
    def test_bad_send_raises_and_sends_nothing(self):
        sender, _receiver = self.pair()
        with self.assertRaises(SchemaError):
            sender.send("b", MsgType.RESOURCE_FETCH, wrong_key="d")
        self.assertEqual(sender.stats()["sent"], {})

    def test_non_json_payload_raises_schema_error(self):
        sender, _receiver = self.pair()
        with self.assertRaisesRegex(SchemaError, "JSON"):
            sender.send("b", MsgType.RESOURCE_ANNOUNCE, resources={"a", "b"})

    def test_invalid_frame_is_rejected_and_channel_survives(self):
        receiver = self.endpoint("b")
        inbox = self.collect(receiver, MsgType.RESOURCE_FETCH)
        with socket.create_connection(("127.0.0.1", receiver.port)) as sock:
            sock.sendall(_encode_frame({HELLO_KEY: "a", "host": "127.0.0.1", "port": 1}))
            sock.sendall(_encode_frame({"type": "NOPE"}))
            sock.sendall(_encode_frame(make_message(MsgType.RESOURCE_FETCH, "a", name="ok")))
            self.assertEqual(inbox.get(timeout=WAIT_S)["payload"]["name"], "ok")
        self.assertEqual(receiver.stats()["rejected"], 1)

    def test_spoofed_sender_is_rejected(self):
        receiver = self.endpoint("b")
        inbox = self.collect(receiver, MsgType.RESOURCE_FETCH)
        with socket.create_connection(("127.0.0.1", receiver.port)) as sock:
            sock.sendall(_encode_frame({HELLO_KEY: "a", "host": "127.0.0.1", "port": 1}))
            sock.sendall(_encode_frame(make_message(MsgType.RESOURCE_FETCH, "c", name="no")))
            sock.sendall(_encode_frame(make_message(MsgType.RESOURCE_FETCH, "a", name="ok")))
            self.assertEqual(inbox.get(timeout=WAIT_S)["payload"]["name"], "ok")

    def test_connection_without_hello_is_dropped(self):
        receiver = self.endpoint("b")
        with socket.create_connection(("127.0.0.1", receiver.port)) as sock:
            sock.sendall(_encode_frame({"hello": "wrong"}))
            sock.settimeout(WAIT_S)
            self.assertEqual(sock.recv(1), b"")

    def test_unknown_target_raises(self):
        sender = self.endpoint("a")
        with self.assertRaisesRegex(TransportError, "no route"):
            sender.send("ghost", MsgType.RESOURCE_FETCH, name="d")

    def test_send_to_self_raises(self):
        with self.assertRaises(TransportError):
            self.endpoint("a").send("a", MsgType.RESOURCE_FETCH, name="d")

    def test_port_in_use_raises(self):
        first = self.endpoint("a")
        second = Endpoint("b", "127.0.0.1", first.port, logger=QUIET)
        with self.assertRaisesRegex(TransportError, "cannot bind"):
            second.start()

    def test_broadcast_returns_unreachable_targets(self):
        live = self.endpoint("live")
        inbox = self.collect(live, MsgType.RESOURCE_FETCH)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            dead_port = probe.getsockname()[1]
        sender = self.endpoint("a", routes={
            "live": ("127.0.0.1", live.port), "dead": ("127.0.0.1", dead_port)})
        failed = sender.broadcast(["dead", "live"], MsgType.RESOURCE_FETCH, name="d")
        self.assertEqual(failed, ["dead"])
        inbox.get(timeout=WAIT_S)


class NodeLossTests(TransportCase):
    def spawn_child(self, parent_port, port=0):
        child = subprocess.Popen(
            [sys.executable, "-c", CHILD, str(parent_port), str(port)],
            cwd=ROOT, stdout=subprocess.PIPE, text=True)
        self.addCleanup(child.stdout.close)
        self.addCleanup(child.wait)
        self.addCleanup(child.kill)
        return child, int(child.stdout.readline())

    def test_leave_is_handled_before_the_disconnect_it_causes(self):
        sender, receiver = self.pair()
        order = queue.Queue()
        receiver.on(MsgType.LEAVE, lambda msg: order.put("leave"))
        receiver.on_disconnect(lambda node_id: order.put("disconnect"))
        sender.send("b", MsgType.LEAVE, reason="done")
        sender.stop()
        self.assertEqual([order.get(timeout=WAIT_S) for _ in range(2)], ["leave", "disconnect"])

    def test_killed_process_is_detected_then_can_rejoin_on_same_port(self):
        parent = self.endpoint("parent")
        beats = self.collect(parent, MsgType.HEARTBEAT)
        lost = queue.Queue()
        parent.on_disconnect(lost.put)

        child, port = self.spawn_child(parent.port)
        beats.get(timeout=WAIT_S)

        child.kill()
        child.wait()
        self.assertEqual(lost.get(timeout=WAIT_S), "child")

        # The dead link must fail rather than swallow the message.
        with self.assertRaises(TransportError):
            parent.send("child", MsgType.RESOURCE_FETCH, name="d")

        self.spawn_child(parent.port, port=port)
        beats.get(timeout=WAIT_S)
        parent.send("child", MsgType.RESOURCE_FETCH, name="d")

    def test_send_to_a_node_that_went_away_fails_without_retrying(self):
        sender, receiver = self.pair()
        inbox = self.collect(receiver, MsgType.RESOURCE_FETCH)
        sender.connect_timeout = 3
        sender.send("b", MsgType.RESOURCE_FETCH, name="d")
        inbox.get(timeout=WAIT_S)
        receiver.stop()

        deadline = time.monotonic() + WAIT_S
        while True:
            started = time.monotonic()
            try:
                sender.send("b", MsgType.RESOURCE_FETCH, name="d")
            except TransportError:
                break
            self.assertLess(time.monotonic(), deadline, "send kept succeeding")
            time.sleep(0.01)
        self.assertLess(time.monotonic() - started, 1)

    def test_link_survives_idle_longer_than_send_timeout(self):
        receiver = self.endpoint("b")
        inbox = self.collect(receiver, MsgType.RESOURCE_FETCH)
        sender = Endpoint("a", "127.0.0.1", 0, routes={"b": ("127.0.0.1", receiver.port)},
                          logger=QUIET, send_timeout=0.05).start()
        self.addCleanup(sender.stop)
        sender.send("b", MsgType.RESOURCE_FETCH, name="one")
        inbox.get(timeout=WAIT_S)
        time.sleep(0.3)
        sender.send("b", MsgType.RESOURCE_FETCH, name="two")
        inbox.get(timeout=WAIT_S)
        self.assertEqual(receiver.stats()["rejected"], 0)
        self.assertEqual(len(sender._links), 1)

    def test_blocked_send_times_out_instead_of_hanging(self):
        listener = socket.socket()
        self.addCleanup(listener.close)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)      # Accepts at TCP level but never reads: a frozen peer.
        sender = Endpoint("a", "127.0.0.1", 0, logger=QUIET, send_timeout=0.3,
                          routes={"frozen": listener.getsockname()}).start()
        self.addCleanup(sender.stop)
        with self.assertRaises(TransportError):
            for _ in range(2000):
                sender.send("frozen", MsgType.RESOURCE_DATA, name="d", found=True,
                            content="z" * 100000)

    def test_changed_address_replaces_the_old_link(self):
        first = self.endpoint("b")
        first_inbox = self.collect(first, MsgType.RESOURCE_FETCH)
        sender = self.endpoint("a", routes={"b": ("127.0.0.1", first.port)})
        sender.send("b", MsgType.RESOURCE_FETCH, name="one")
        first_inbox.get(timeout=WAIT_S)

        second = self.endpoint("b2")
        second_inbox = self.collect(second, MsgType.RESOURCE_FETCH)
        sender.add_route("b", "127.0.0.1", second.port)
        sender.send("b", MsgType.RESOURCE_FETCH, name="two")
        self.assertEqual(second_inbox.get(timeout=WAIT_S)["payload"]["name"], "two")

    def test_stop_is_idempotent_and_blocks_sending(self):
        sender, _receiver = self.pair()
        sender.stop()
        sender.stop()
        with self.assertRaises(TransportError):
            sender.send("b", MsgType.RESOURCE_FETCH, name="d")


if __name__ == "__main__":
    unittest.main()
