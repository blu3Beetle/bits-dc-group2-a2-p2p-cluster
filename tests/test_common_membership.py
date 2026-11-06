import logging
import unittest

from common import MembershipTable, Status


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


def entry(node_id, incarnation=1, status=Status.ALIVE, role="peer"):
    return {"node_id": node_id, "host": "127.0.0.1", "port": 1, "role": role,
            "incarnation": incarnation, "status": status}


class MembershipTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.table = MembershipTable("me", failure_timeout_s=3.0, clock=self.clock)
        self.table.add("me", "127.0.0.1", 1, "peer", 1)
        self.events = []
        self.table.on_join(lambda m: self.events.append(("join", m.node_id)))
        self.table.on_leave(lambda m: self.events.append(("leave", m.node_id)))
        self.table.on_fail(lambda m: self.events.append(("fail", m.node_id)))

    def add_other(self, incarnation=1):
        self.table.add("other", "127.0.0.1", 2, "peer", incarnation)

    def test_add_fires_join_once(self):
        self.add_other()
        self.add_other()
        self.assertEqual(self.events, [("join", "other")])

    def test_alive_excludes_self_and_filters_role(self):
        self.add_other()
        self.table.add("w1", "127.0.0.1", 3, "worker", 1)
        self.assertEqual([m.node_id for m in self.table.alive()], ["other", "w1"])
        self.assertEqual([m.node_id for m in self.table.alive("worker")], ["w1"])
        self.assertEqual(len(self.table.alive(include_self=True)), 3)

    def test_silent_node_is_swept_once(self):
        self.add_other()
        self.clock.now += 3.1
        self.assertEqual(self.table.sweep(), ["other"])
        self.assertEqual(self.table.sweep(), [])
        self.assertEqual(self.events[-1], ("fail", "other"))

    def test_touch_prevents_sweep(self):
        self.add_other()
        self.clock.now += 2.0
        self.table.touch("other")
        self.clock.now += 2.0
        self.assertEqual(self.table.sweep(), [])

    def test_self_is_never_swept_or_marked_dead(self):
        self.clock.now += 100
        self.assertEqual(self.table.sweep(), [])
        self.table.mark_dead("me")
        self.assertEqual(self.table.get("me").status, Status.ALIVE)

    def test_direct_contact_revives_dead_node(self):
        self.add_other()
        self.table.mark_dead("other")
        self.table.touch("other")
        self.assertEqual(self.table.get("other").status, Status.ALIVE)
        self.assertEqual(self.events, [("join", "other"), ("fail", "other"), ("join", "other")])

    def test_left_node_is_not_revived_by_touch(self):
        self.add_other()
        self.table.mark_left("other")
        self.table.touch("other")
        self.assertEqual(self.table.get("other").status, Status.LEFT)

    def test_disconnect_after_leave_stays_graceful(self):
        self.add_other()
        self.table.mark_left("other")
        self.table.mark_dead("other")
        self.assertEqual(self.events, [("join", "other"), ("leave", "other")])

    def test_restart_with_new_incarnation_overrides_left(self):
        self.add_other(incarnation=1)
        self.table.mark_left("other")
        self.add_other(incarnation=2)
        self.assertEqual(self.table.get("other").status, Status.ALIVE)
        self.assertEqual(self.events[-1], ("join", "other"))

    def test_stale_gossip_cannot_override_newer_incarnation(self):
        self.add_other(incarnation=5)
        self.table.merge([entry("other", incarnation=4, status=Status.DEAD)])
        self.assertEqual(self.table.get("other").status, Status.ALIVE)

    def test_stale_alive_cannot_override_bad_news(self):
        self.add_other()
        self.table.mark_dead("other")
        self.table.merge([entry("other", status=Status.ALIVE)])
        self.assertEqual(self.table.get("other").status, Status.DEAD)

    def test_gossip_spreads_failure(self):
        self.add_other()
        self.table.merge([entry("other", status=Status.DEAD)])
        self.assertEqual(self.events[-1], ("fail", "other"))

    def test_false_rumour_about_self_is_refuted(self):
        self.table.merge([entry("me", incarnation=9, status=Status.DEAD)])
        self.assertEqual(self.table.get("me").status, Status.ALIVE)
        self.assertEqual(self.table.get("me").incarnation, 10)

        remote = MembershipTable("remote", 3.0, clock=self.clock)
        remote.merge([entry("me", incarnation=9, status=Status.DEAD)])
        remote.merge(self.table.snapshot())
        self.assertEqual(remote.get("me").status, Status.ALIVE)

    def test_alive_report_about_self_does_not_change_incarnation(self):
        self.table.merge([entry("me", incarnation=9)])
        self.assertEqual(self.table.get("me").incarnation, 1)

    def test_failing_callback_does_not_escape_or_skip_others(self):
        self.table.add("a", "127.0.0.1", 2, "peer", 1)
        self.table.add("b", "127.0.0.1", 3, "peer", 1)
        failed = []

        def explode(_member):
            raise RuntimeError("expected by this test")

        self.table._fail_handlers.insert(0, explode)
        self.table.on_fail(lambda m: failed.append(m.node_id))
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)
        self.clock.now += 3.1
        self.assertEqual(sorted(self.table.sweep()), ["a", "b"])
        self.assertEqual(sorted(failed), ["a", "b"])

    def test_learning_of_already_gone_node_fires_nothing(self):
        self.table.merge([entry("ghost", status=Status.LEFT)])
        self.assertEqual(self.events, [])
        self.assertEqual(self.table.alive(), [])

    def test_two_tables_converge_through_snapshot(self):
        self.add_other()
        remote = MembershipTable("remote", 3.0, clock=self.clock)
        remote.merge(self.table.snapshot())
        self.assertEqual({m.node_id for m in remote.alive()}, {"me", "other"})

    def test_returned_members_are_copies(self):
        self.add_other()
        self.table.get("other").status = Status.DEAD
        self.assertEqual(self.table.get("other").status, Status.ALIVE)


if __name__ == "__main__":
    unittest.main()
