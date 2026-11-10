"""Tests for cluster/worker.py, run against a real master endpoint over TCP.

The master here is only a MembershipService plus a TASK_RESULT collector, so
the worker is tested on its own, without cluster/master.py.

    python3 -m unittest tests.test_worker -v
"""

import logging
import queue
import time
import unittest
from types import SimpleNamespace

from common import MsgType, Status, TransportError, build_node
from cluster.worker import Worker, count_primes
from tests.test_joinleave import JoinLeaveCase, Node, wait_until

LONG_TASK = {"start": 0, "end": 600_000}       # roughly a second of CPU


class WorkerCase(JoinLeaveCase):
    def setUp(self):
        super().setUp()
        self.master = self.start("master", "master")
        self.results = queue.Queue()
        self.master.ctx.ep.on(MsgType.TASK_RESULT, self.results.put)
        self._task = 0

    def start_worker(self, name="worker1"):
        args = SimpleNamespace(id=self.id_(name), port=None, host=None,
                               config=self.config_path, verbose=False)
        ctx = build_node(args, "worker")
        for handler in ctx.log.handlers:
            handler.close()
        ctx.log.handlers[:] = [logging.NullHandler()]
        worker = Worker(ctx)
        node = Node(ctx, worker.membership)
        for attempt in range(10):
            try:
                ctx.ep.start()
                break
            except TransportError:
                if attempt == 9:
                    raise
                time.sleep(0.1)
        worker.membership.start()
        self.addCleanup(node.crash)
        self.assertTrue(wait_until(lambda: self.id_(name) in self.master.alive("worker")))
        return node

    def assign(self, worker_name, kind, params):
        self._task += 1
        task_id = "t{}".format(self._task)
        self.master.ctx.ep.send(self.id_(worker_name), MsgType.TASK_ASSIGN,
                                task_id=task_id, job_id="job1", kind=kind, params=params)
        return task_id

    def result(self, timeout=10):
        return self.results.get(timeout=timeout)["payload"]


class TaskKindTests(WorkerCase):
    def test_sum(self):
        self.start_worker()
        task_id = self.assign("worker1", "sum", {"numbers": [1, 2, 3, 4]})
        r = self.result()
        self.assertEqual((r["task_id"], r["job_id"], r["status"], r["result"]),
                         (task_id, "job1", "SUCCESS", 10))
        self.assertEqual(r["worker_id"], self.id_("worker1"))
        self.assertGreaterEqual(r["elapsed_s"], 0)

    def test_word_count(self):
        self.start_worker()
        self.assign("worker1", "word_count", {"text": "peer to peer and cluster"})
        r = self.result()
        self.assertEqual((r["status"], r["result"]), ("SUCCESS", 5))

    def test_count_primes(self):
        self.start_worker()
        self.assign("worker1", "count_primes", {"start": 0, "end": 100})
        r = self.result()
        self.assertEqual((r["status"], r["result"]), ("SUCCESS", 25))

    def test_count_primes_helper_matches_known_values(self):
        self.assertEqual(count_primes(0, 10), 4)            # 2, 3, 5, 7
        self.assertEqual(count_primes(10, 20), 4)           # 11, 13, 17, 19
        self.assertEqual(count_primes(0, 2), 0)


class TaskFailureTests(WorkerCase):
    def test_unknown_kind_fails_and_worker_keeps_working(self):
        self.start_worker()
        self.assign("worker1", "no_such_kind", {})
        self.assertEqual(self.result()["status"], "FAILED")
        self.assign("worker1", "sum", {"numbers": [5]})
        self.assertEqual(self.result()["result"], 5)

    def test_bad_params_fail_and_report_the_error(self):
        self.start_worker()
        self.assign("worker1", "sum", {"wrong_key": [1]})
        r = self.result()
        self.assertEqual(r["status"], "FAILED")
        self.assertIn("numbers", r["result"])


class ConcurrencyTests(WorkerCase):
    def test_long_task_does_not_block_a_short_one(self):
        self.start_worker()
        slow = self.assign("worker1", "count_primes", LONG_TASK)
        fast = self.assign("worker1", "sum", {"numbers": [1, 1]})
        first, second = self.result(), self.result(timeout=30)
        self.assertEqual((first["task_id"], second["task_id"]), (fast, slow))

    def test_worker_stays_alive_while_busy(self):
        self.start_worker()
        self.assign("worker1", "count_primes", LONG_TASK)
        self.result(timeout=30)
        self.assertEqual(self.master.status_of(self.id_("worker1")), Status.ALIVE)
        self.assertNotIn(("fail", self.id_("worker1")), self.master.events)

    def test_tasks_go_to_different_workers(self):
        self.start_worker("worker1")
        self.start_worker("worker2")
        self.assign("worker1", "sum", {"numbers": [1]})
        self.assign("worker2", "sum", {"numbers": [2]})
        done_by = {self.result()["worker_id"], self.result()["worker_id"]}
        self.assertEqual(done_by, {self.id_("worker1"), self.id_("worker2")})


class WorkerLossTests(WorkerCase):
    def test_crash_during_a_task_is_reported_to_the_master(self):
        worker = self.start_worker()
        self.assign("worker1", "count_primes", LONG_TASK)
        time.sleep(0.1)
        worker.crash()
        w1 = self.id_("worker1")
        self.assertTrue(wait_until(lambda: self.master.status_of(w1) == Status.DEAD))
        self.assertEqual(self.master.lost_workers, [w1])    # master would requeue here
        with self.assertRaises(queue.Empty):
            self.results.get(timeout=0.5)


if __name__ == "__main__":
    unittest.main()
