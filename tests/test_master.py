"""Tests for cluster/master.py. Owner: Kashif.

The splitting rules are tested on their own. Everything else runs the real
Master against real Workers (cluster/worker.py) over TCP on 127.0.0.1, built
with the same build_node() the entry points use. A plain endpoint plays the
peer that submits jobs, so the master is tested without the bridge.

    python3 -m unittest tests.test_master -v
"""

import logging
import queue
import time
import unittest
from types import SimpleNamespace

from common import MsgType, Status, TransportError, build_node
from cluster.master import JobError, Master, combine, max_concurrency, split_job
from cluster.worker import Worker, count_primes
from tests.test_joinleave import JoinLeaveCase, Node, free_port, wait_until

SLOW = {"start": 0, "end": 400_000}         # a few tenths of a second of CPU per task


def _quiet(ctx):
    for handler in ctx.log.handlers:
        handler.close()
    ctx.log.handlers[:] = [logging.NullHandler()]


def _start_endpoint(ctx):
    for attempt in range(10):
        try:
            ctx.ep.start()
            return
        except TransportError:
            if attempt == 9:
                raise
            time.sleep(0.1)


class SplitTests(unittest.TestCase):
    def test_count_primes_ranges_cover_the_range_exactly(self):
        parts = split_job("count_primes", {"start": 10, "end": 1000, "tasks": 7}, 4)
        self.assertEqual(len(parts), 7)
        self.assertEqual(parts[0]["start"], 10)
        self.assertEqual(parts[-1]["end"], 1000)
        for a, b in zip(parts, parts[1:]):
            self.assertEqual(a["end"], b["start"])
        self.assertEqual(sum(count_primes(p["start"], p["end"]) for p in parts), count_primes(10, 1000))

    def test_sum_and_word_count_chunks_keep_every_item(self):
        parts = split_job("sum", {"numbers": list(range(10)), "tasks": 3}, 4)
        self.assertEqual([len(p["numbers"]) for p in parts], [4, 3, 3])
        self.assertEqual(combine("sum", [sum(p["numbers"]) for p in parts]), 45)
        parts = split_job("word_count", {"text": "a b  c\nd e", "tasks": 2}, 4)
        self.assertEqual(sum(len(p["text"].split()) for p in parts), 5)

    def test_never_more_tasks_than_items(self):
        self.assertEqual(len(split_job("sum", {"numbers": [1, 2], "tasks": 10}, 4)), 2)
        self.assertEqual(len(split_job("count_primes", {"start": 0, "end": 3}, 8)), 3)
        self.assertEqual(split_job("word_count", {"text": "  "}, 4), [{"text": ""}])

    def test_default_split_uses_the_given_task_count(self):
        self.assertEqual(len(split_job("count_primes", {"start": 0, "end": 100}, 6)), 6)

    def test_bad_jobs_are_rejected(self):
        for kind, params in [("sort", {}), ("sum", {"numbers": "1,2"}),
                             ("count_primes", {"start": 5, "end": 1}),
                             ("count_primes", {"start": "0", "end": 9}),
                             ("word_count", {"text": 3}),
                             ("sum", {"numbers": [1], "tasks": 0})]:
            with self.assertRaises(JobError, msg=kind):
                split_job(kind, params, 4)

    def test_max_concurrency(self):
        self.assertEqual(max_concurrency([(0, 2), (1, 3), (2.5, 4)]), 2)
        self.assertEqual(max_concurrency([(0, 1), (1, 2)]), 1)    # touching is not overlapping


class MasterCase(JoinLeaveCase):
    """A real master, real workers, and a client endpoint acting as a peer."""

    def setUp(self):
        super().setUp()
        self.master = self.start_master()
        self.accepted, self.results = queue.Queue(), queue.Queue()
        args = SimpleNamespace(id=self.id_("peer1"), port=None, host=None,
                               config=self.config_path, verbose=False)
        self.client = build_node(args, "peer")
        _quiet(self.client)
        self.client.ep.on(MsgType.JOB_ACCEPTED, lambda m: self.accepted.put(m["payload"]))
        self.client.ep.on(MsgType.JOB_RESULT, lambda m: self.results.put(m["payload"]))
        _start_endpoint(self.client)
        self.addCleanup(self.client.ep.stop)
        self._job = 0

    def start_master(self):
        args = SimpleNamespace(id=self.id_("master"), port=None, host=None,
                               config=self.config_path, verbose=False)
        ctx = build_node(args, "master")
        _quiet(ctx)
        master = Master(ctx)
        _start_endpoint(ctx)
        master.membership.start()
        master.start()
        self.addCleanup(master.stop)
        self.addCleanup(Node(ctx, master.membership).crash)
        return master

    def start_worker(self, name, port=None):
        args = SimpleNamespace(id=self.id_(name), port=port, host=None,
                               config=self.config_path, verbose=False)
        ctx = build_node(args, "worker")
        _quiet(ctx)
        worker = Worker(ctx)
        node = Node(ctx, worker.membership)
        _start_endpoint(ctx)
        worker.membership.start()
        self.addCleanup(node.crash)
        self.assertTrue(wait_until(lambda: self.id_(name) in self.alive_workers()))
        return node

    def alive_workers(self):
        return [m.node_id for m in self.master.ctx.members.alive("worker")]

    def submit(self, kind, params):
        self._job += 1
        job_id = "{}job{}".format(self.prefix, self._job)
        self.client.ep.send(self.id_("master"), MsgType.JOB_SUBMIT, job_id=job_id,
                            origin=self.id_("peer1"), kind=kind, params=params)
        return job_id

    def result(self, timeout=15):
        return self.results.get(timeout=timeout)

    def ran_on(self, job_id):
        """worker_id for each task of a finished job."""
        job = self.master.jobs[job_id]
        return [self.master.tasks[t]["interval"][2] for t in job["task_ids"]]


class MasterJobTests(MasterCase):
    def test_sum_job_is_split_dispatched_and_combined(self):
        for i in (1, 2, 3):
            self.start_worker("worker{}".format(i))
        job_id = self.submit("sum", {"numbers": list(range(1, 101)), "tasks": 6})
        self.assertEqual(self.accepted.get(timeout=5), {"job_id": job_id, "task_count": 6})
        r = self.result()
        self.assertEqual((r["job_id"], r["status"], r["result"]), (job_id, "SUCCESS", 5050))
        self.assertGreater(r["elapsed_s"], 0)
        self.assertGreater(len(set(self.ran_on(job_id))), 1)

    def test_word_count_and_count_primes_results(self):
        self.start_worker("worker1")
        self.start_worker("worker2")
        self.submit("word_count", {"text": "the quick brown fox jumps over the lazy dog " * 5})
        self.assertEqual(self.result()["result"], 45)
        self.submit("count_primes", {"start": 0, "end": 50_000, "tasks": 5})
        self.assertEqual(self.result()["result"], count_primes(0, 50_000))

    def test_default_split_is_two_tasks_per_live_worker(self):
        for i in (1, 2, 3):
            self.start_worker("worker{}".format(i))
        self.submit("count_primes", {"start": 0, "end": 1000})
        self.assertEqual(self.accepted.get(timeout=5)["task_count"], 6)
        self.assertEqual(self.result()["result"], count_primes(0, 1000))

    def test_bad_job_fails_without_being_accepted(self):
        job_id = self.submit("sort", {"numbers": [3, 1]})
        r = self.result(timeout=5)
        self.assertEqual((r["job_id"], r["status"]), (job_id, "FAILED"))
        self.assertIn("unknown job kind", r["result"])
        self.assertTrue(self.accepted.empty())

    def test_tasks_run_in_parallel_across_workers(self):
        for i in (1, 2, 3):
            self.start_worker("worker{}".format(i))
        job_id = self.submit("count_primes", dict(SLOW, tasks=3))
        self.assertEqual(self.result()["status"], "SUCCESS")
        proof = self.master.jobs[job_id]["proof"]
        self.assertEqual(proof["max_concurrent"], 3)
        self.assertEqual(len(proof["workers"]), 3)
        self.assertGreater(proof["parallelism"], 1.5)

    def test_job_waits_for_a_worker_then_runs(self):
        job_id = self.submit("sum", {"numbers": [1, 2, 3], "tasks": 3})
        self.assertEqual(self.accepted.get(timeout=5)["task_count"], 3)
        time.sleep(0.3)
        self.assertTrue(self.results.empty())
        self.start_worker("worker1")
        self.assertEqual(self.result()["result"], 6)
        self.assertEqual(set(self.ran_on(job_id)), {self.id_("worker1")})


class MasterMembershipTests(MasterCase):
    def test_worker_crash_mid_job_requeues_its_tasks(self):
        w1 = self.start_worker("worker1")
        self.start_worker("worker2")
        job_id = self.submit("count_primes", dict(SLOW, tasks=6))
        self.assertTrue(wait_until(lambda: self.id_("worker1") in self.master.busy))
        w1.crash()                                         # kill -9
        r = self.result()
        self.assertEqual((r["status"], r["result"]), ("SUCCESS", count_primes(**SLOW)))
        self.assertGreaterEqual(self.master.requeued, 1)
        self.assertNotIn(self.id_("worker1"), self.ran_on(job_id))

    def test_worker_leave_mid_job_requeues_its_tasks(self):
        w1 = self.start_worker("worker1")
        self.start_worker("worker2")
        self.submit("count_primes", dict(SLOW, tasks=4))
        self.assertTrue(wait_until(lambda: self.id_("worker1") in self.master.busy))
        w1.leave()                                         # Ctrl+C
        self.assertEqual(self.result()["result"], count_primes(**SLOW))
        self.assertEqual(self.master.ctx.members.get(self.id_("worker1")).status, Status.LEFT)
        self.assertGreaterEqual(self.master.requeued, 1)

    def test_worker_joining_mid_job_gets_tasks(self):
        self.start_worker("worker1")
        job_id = self.submit("count_primes", dict(SLOW, tasks=6))
        self.assertTrue(wait_until(lambda: self.id_("worker1") in self.master.busy))
        self.start_worker("worker4", port=free_port())   # not in the config
        self.assertEqual(self.result()["result"], count_primes(**SLOW))
        self.assertIn(self.id_("worker4"), self.ran_on(job_id))


class FakeWorkerTests(MasterCase):
    """A stand-in worker whose replies the test controls."""

    def fake_worker(self, reply):
        node = self.start("worker1", "worker")
        node.ctx.ep.on(MsgType.TASK_ASSIGN, lambda m: reply(node.ctx, m["payload"]))
        self.assertTrue(wait_until(lambda: self.id_("worker1") in self.alive_workers()))
        return node

    @staticmethod
    def send_result(ctx, task, status, result):
        ctx.ep.send(ctx.cfg.master_id, MsgType.TASK_RESULT, task_id=task["task_id"],
                    job_id=task["job_id"], worker_id=ctx.node_id, status=status,
                    result=result, elapsed_s=0.0)

    def test_duplicate_task_results_are_ignored(self):
        def reply_twice(ctx, task):
            self.send_result(ctx, task, "SUCCESS", 10)
            self.send_result(ctx, task, "SUCCESS", 999)    # duplicate
        self.fake_worker(reply_twice)
        job_id = self.submit("sum", {"numbers": [10], "tasks": 1})
        self.assertEqual(self.result()["result"], 10)
        self.assertTrue(wait_until(lambda: self.master.duplicates == 1))
        self.assertEqual(self.master.jobs[job_id]["result"], 10)
        time.sleep(0.2)
        self.assertTrue(self.results.empty())             # only one JOB_RESULT

    def test_failed_task_is_retried_then_fails_the_job(self):
        calls = []

        def always_fail(ctx, task):
            calls.append(task["task_id"])
            self.send_result(ctx, task, "FAILED", "boom")
        self.fake_worker(always_fail)
        job_id = self.submit("sum", {"numbers": [1], "tasks": 1})
        r = self.result()
        self.assertEqual((r["job_id"], r["status"]), (job_id, "FAILED"))
        self.assertIn("boom", r["result"])
        self.assertEqual(len(calls), 3)

    def test_failed_task_succeeds_on_retry(self):
        calls = []

        def fail_once(ctx, task):
            calls.append(task["task_id"])
            self.send_result(ctx, task, "FAILED" if len(calls) == 1 else "SUCCESS", 7)
        self.fake_worker(fail_once)
        self.submit("sum", {"numbers": [7], "tasks": 1})
        r = self.result()
        self.assertEqual((r["status"], r["result"]), ("SUCCESS", 7))
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
