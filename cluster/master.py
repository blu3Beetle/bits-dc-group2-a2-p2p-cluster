"""Cluster master. Owner: Kashif.

    python -m cluster.master --id master

What it does:
  * JOB_SUBMIT: checks the job, replies JOB_ACCEPTED with the task count,
    splits the job into tasks and puts them on the task queue.
  * Dispatch: a dispatcher thread hands each queued task to an idle worker
    from ctx.members.alive("worker") with TASK_ASSIGN. A worker runs one task
    at a time, so a fast worker simply comes back for more (pull-style load
    balancing). A worker that joins at runtime is idle, so it gets work on
    the next dispatch round without any extra code.
  * TASK_RESULT: stores each partial result. When every task of a job is
    done, the partial results are combined and one JOB_RESULT goes to the
    job's origin peer.
  * Worker lost (leave, crash, hang): Sohail's MembershipService calls
    requeue_worker_tasks(), which puts that worker's unfinished tasks back at
    the front of the queue for the remaining workers.
  * Parallel proof: when a job finishes, the master logs a timeline of its
    tasks (worker, start, end) and the largest number of tasks that ran at the
    same time, which shows the tasks overlapping across workers.

Job kinds, agreed with Abhirup's worker (cluster/worker.py):

    kind          job params                        split into tasks of
    count_primes  {"start": int, "end": int}        sub-ranges [a, b)
    sum           {"numbers": [number, ...]}        chunks of the list
    word_count    {"text": str}                     chunks of the words

Every job also takes an optional "tasks": int, the number of tasks to split
into. Without it, the master uses 2 tasks per live worker. All three kinds
combine by adding the partial results.

Threads: handlers run on the endpoint's dispatcher thread, dispatch runs on
its own thread, and requeue_worker_tasks() may run on the heartbeat thread.
All job and task state is guarded by self._lock, and no message is ever sent
while holding it.
"""

import threading
import time
from collections import deque

from common import MsgType, TransportError, build_node, node_arg_parser, run_until_stopped
from joinleave.service import MembershipService

KINDS = ("count_primes", "sum", "word_count")
MAX_ATTEMPTS = 3            # a task that FAILS this many times fails its job
TASKS_PER_WORKER = 2        # default split when the job gives no "tasks"
DISPATCH_POLL_S = 0.5       # dispatcher re-checks even without a wake-up


class JobError(ValueError):
    """A JOB_SUBMIT that cannot be run: unknown kind or bad params."""


# -- splitting -----------------------------------------------------------------

def _chunks(items, parts):
    """Split a list into `parts` nearly equal, contiguous, non-empty chunks."""
    parts = max(1, min(parts, len(items)))
    size, extra = divmod(len(items), parts)
    out, i = [], 0
    for p in range(parts):
        step = size + (1 if p < extra else 0)
        out.append(items[i:i + step])
        i += step
    return out


def split_job(kind, params, default_tasks):
    """Return the list of task params for a job, or raise JobError."""
    if kind not in KINDS:
        raise JobError("unknown job kind {!r}, expected one of {}".format(kind, ", ".join(KINDS)))
    if not isinstance(params, dict):
        raise JobError("params must be an object")

    tasks = params.get("tasks", default_tasks)
    if isinstance(tasks, bool) or not isinstance(tasks, int) or tasks < 1:
        raise JobError("tasks must be a positive integer")

    if kind == "count_primes":
        start, end = params.get("start"), params.get("end")
        if not all(isinstance(v, int) and not isinstance(v, bool) for v in (start, end)):
            raise JobError("count_primes needs integer start and end")
        if end < start:
            raise JobError("count_primes needs start <= end")
        if end == start:
            return [{"start": start, "end": end}]
        return [{"start": c.start, "end": c.stop} for c in _chunks(range(start, end), tasks)]

    if kind == "sum":
        numbers = params.get("numbers")
        if not isinstance(numbers, list):
            raise JobError("sum needs a list of numbers")
        if not numbers:
            return [{"numbers": []}]
        return [{"numbers": c} for c in _chunks(numbers, tasks)]

    text = params.get("text")
    if not isinstance(text, str):
        raise JobError("word_count needs a text string")
    words = text.split()
    if not words:
        return [{"text": ""}]
    return [{"text": " ".join(c)} for c in _chunks(words, tasks)]


def combine(kind, results):
    """Combine the partial results of a job's tasks, in task order."""
    return sum(results)


def max_concurrency(intervals):
    """Largest number of (start, end) intervals that overlap at one instant."""
    events = sorted([(s, 1) for s, _ in intervals] + [(e, -1) for _, e in intervals],
                    key=lambda ev: (ev[0], ev[1]))
    running = best = 0
    for _, delta in events:
        running += delta
        best = max(best, running)
    return best


# -- the master ----------------------------------------------------------------

class Master:
    def __init__(self, ctx):
        self.ctx = ctx
        self.membership = MembershipService(ctx, on_worker_lost=self.requeue_worker_tasks)

        self._lock = threading.RLock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = None

        self.jobs = {}              # job_id -> job record, see on_job_submit
        self.tasks = {}             # task_id -> task record
        self.queue = deque()        # task_ids waiting for a worker
        self.busy = {}              # worker_id -> task_id it is running
        self.requeued = 0           # tasks put back after a worker was lost
        self.duplicates = 0         # TASK_RESULTs ignored as duplicate or late

        ctx.ep.on(MsgType.JOB_SUBMIT, self.on_job_submit)
        ctx.ep.on(MsgType.TASK_RESULT, self.on_task_result)
        # A new or returning worker is idle: dispatch to it straight away.
        ctx.members.on_join(lambda member: self._wake.set() if member.role == "worker" else None)

    # -- lifecycle ---------------------------------------------------------------

    def start(self):
        self._thread = threading.Thread(target=self._dispatch_loop, daemon=True,
                                        name="{}-dispatch".format(self.ctx.node_id))
        self._thread.start()
        self.ctx.log.info("master ready: job kinds %s, %d task(s) per worker by default",
                          ", ".join(KINDS), TASKS_PER_WORKER)

    def stop(self):
        self._stop.set()
        self._wake.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)

    # -- JOB_SUBMIT --------------------------------------------------------------

    def on_job_submit(self, msg):
        payload = msg["payload"]
        job_id, origin, kind, params = (payload["job_id"], payload["origin"],
                                        payload["kind"], payload["params"])
        workers = len(self.ctx.members.alive("worker"))

        with self._lock:
            if job_id in self.jobs:
                self.ctx.log.warning("JOB_SUBMIT %s from %s is a duplicate; ignored", job_id, origin)
                return

        try:
            task_params = split_job(kind, params, TASKS_PER_WORKER * max(1, workers))
        except JobError as exc:
            self.ctx.log.warning("JOB REJECTED %s from %s: %s", job_id, origin, exc)
            self._send_job_result(origin, job_id, "FAILED", "rejected: {}".format(exc), 0.0)
            return

        now = time.monotonic()
        with self._lock:
            job = {
                "job_id": job_id, "origin": origin, "kind": kind,
                "task_ids": [], "results": {}, "status": "RUNNING",
                "received": now, "finished": None, "result": None, "proof": None,
            }
            for i, tp in enumerate(task_params, 1):
                task_id = "{}-t{}".format(job_id, i)
                job["task_ids"].append(task_id)
                self.tasks[task_id] = {
                    "task_id": task_id, "job_id": job_id, "kind": kind, "params": tp,
                    "state": "QUEUED", "worker": None, "attempts": 0,
                    "started": {},          # worker_id -> when it was assigned
                    "interval": None,       # (start, end, worker) of the accepted run
                }
                self.queue.append(task_id)
            self.jobs[job_id] = job

        self.ctx.log.info("JOB RECEIVED %s from %s: kind=%s params=%s -> %d task(s), %d live worker(s)",
                          job_id, origin, kind, _short(params), len(task_params), workers)
        try:
            self.ctx.ep.send(origin, MsgType.JOB_ACCEPTED, job_id=job_id, task_count=len(task_params))
        except TransportError as exc:
            # Keep running it: the origin may still be reachable for JOB_RESULT.
            self.ctx.log.warning("could not send JOB_ACCEPTED for %s to %s: %s", job_id, origin, exc)
        if not workers:
            self.ctx.log.warning("no live workers: job %s waits in the queue until one joins", job_id)
        self._wake.set()

    # -- dispatch ------------------------------------------------------------------

    def _dispatch_loop(self):
        while not self._stop.is_set():
            self._wake.wait(DISPATCH_POLL_S)
            self._wake.clear()
            if not self._stop.is_set():
                self.dispatch()

    def dispatch(self):
        """Give each idle live worker one queued task."""
        while True:
            with self._lock:
                alive = [m.node_id for m in self.ctx.members.alive("worker")]
                idle = [w for w in alive if w not in self.busy]
                if not idle:
                    return
                task_id = self._next_queued()      # only pop once a worker is free
                if task_id is None:
                    return
                worker = idle[0]
                task = self.tasks[task_id]
                task["state"] = "RUNNING"
                task["worker"] = worker
                task["started"][worker] = time.monotonic()
                self.busy[worker] = task_id
                assign = dict(task_id=task_id, job_id=task["job_id"],
                              kind=task["kind"], params=task["params"])
                waiting = len(self.queue)

            try:
                self.ctx.ep.send(worker, MsgType.TASK_ASSIGN, **assign)
            except TransportError as exc:
                self.ctx.log.warning("TASK_ASSIGN %s to %s failed (%s); requeued", task_id, worker, exc)
                with self._lock:
                    if self.busy.get(worker) == task_id:
                        del self.busy[worker]
                    if task["state"] == "RUNNING" and task["worker"] == worker:
                        task["state"], task["worker"] = "QUEUED", None
                        self.queue.appendleft(task_id)
                # The membership service marks the worker dead; until then,
                # don't spin on it.
                return
            self.ctx.log.info("DISPATCH %s -> %s (%s %s), %d task(s) still queued",
                              task_id, worker, assign["kind"], _short(assign["params"]), waiting)

    def _next_queued(self):
        """Pop the next task that still needs running. Caller holds the lock."""
        while self.queue:
            task_id = self.queue.popleft()
            task = self.tasks[task_id]
            if task["state"] == "QUEUED" and self.jobs[task["job_id"]]["status"] == "RUNNING":
                return task_id
        return None

    # -- TASK_RESULT ---------------------------------------------------------------

    def on_task_result(self, msg):
        p = msg["payload"]
        task_id, worker, status, result = p["task_id"], p["worker_id"], p["status"], p["result"]
        now = time.monotonic()
        finished_job = None

        with self._lock:
            if self.busy.get(worker) == task_id:
                del self.busy[worker]               # the worker is free again
            task = self.tasks.get(task_id)
            job = self.jobs.get(task["job_id"]) if task else None

            if task is None or task["state"] == "DONE" or job["status"] != "RUNNING":
                self.duplicates += 1
                self.ctx.log.info("DUPLICATE/LATE TASK_RESULT %s from %s ignored", task_id, worker)
                self._wake.set()
                return

            if status != "SUCCESS":
                task["attempts"] += 1
                if task["attempts"] >= MAX_ATTEMPTS:
                    job["status"] = "FAILED"
                    job["finished"] = now
                    job["result"] = "task {} failed {} times, last error: {}".format(
                        task_id, task["attempts"], result)
                    finished_job = job
                    self.ctx.log.error("TASK FAILED %s on %s (%s); attempt %d of %d, job %s FAILED",
                                       task_id, worker, result, task["attempts"], MAX_ATTEMPTS, job["job_id"])
                else:
                    if task["worker"] == worker:
                        task["state"], task["worker"] = "QUEUED", None
                        self.queue.appendleft(task_id)
                    self.ctx.log.warning("TASK FAILED %s on %s (%s); attempt %d of %d, retrying",
                                         task_id, worker, result, task["attempts"], MAX_ATTEMPTS)
            else:
                # The first SUCCESS wins. It may come from a worker the task was
                # taken from (wrongly thought dead); a later copy is a duplicate.
                started = task["started"].get(worker, job["received"])
                task["state"] = "DONE"
                task["interval"] = (started, now, worker)
                job["results"][task_id] = result
                done, total = len(job["results"]), len(job["task_ids"])
                self.ctx.log.info("RESULT %s from %s = %s (worker time %.3fs), job %s %d/%d done",
                                  task_id, worker, result, p["elapsed_s"], job["job_id"], done, total)
                if done == total:
                    job["status"] = "SUCCESS"
                    job["finished"] = now
                    job["result"] = combine(job["kind"], [job["results"][t] for t in job["task_ids"]])
                    finished_job = job
        self._wake.set()

        if finished_job is not None:
            self._finish(finished_job)

    def _finish(self, job):
        elapsed = round(job["finished"] - job["received"], 4)
        if job["status"] == "SUCCESS":
            job["proof"] = self._parallel_proof(job)
        self.ctx.log.info("JOB DONE %s: status=%s result=%s elapsed=%.3fs",
                          job["job_id"], job["status"], job["result"], elapsed)
        self._send_job_result(job["origin"], job["job_id"], job["status"], job["result"], elapsed)

    def _send_job_result(self, origin, job_id, status, result, elapsed_s):
        try:
            self.ctx.ep.send(origin, MsgType.JOB_RESULT, job_id=job_id, status=status,
                             result=result, elapsed_s=elapsed_s)
        except TransportError as exc:
            self.ctx.log.warning("could not send JOB_RESULT for %s to %s: %s", job_id, origin, exc)

    # -- parallel execution proof ------------------------------------------------------

    def _parallel_proof(self, job):
        """Log the job's task timeline and how many tasks overlapped in time."""
        with self._lock:
            runs = [(t, self.tasks[t]["interval"]) for t in job["task_ids"]]
        t0 = job["received"]
        wall = job["finished"] - t0
        busy = sum(end - start for _, (start, end, _) in runs)
        workers = sorted({w for _, (_, _, w) in runs})
        per_worker = {w: sum(1 for _, (_, _, x) in runs if x == w) for w in workers}
        overlap = max_concurrency([(s, e) for _, (s, e, _) in runs])
        proof = {
            "tasks": len(runs), "workers": workers, "tasks_per_worker": per_worker,
            "max_concurrent": overlap, "wall_s": round(wall, 4), "busy_s": round(busy, 4),
            "parallelism": round(busy / wall, 2) if wall > 0 else 0.0,
            "timeline": [(t, w, round(s - t0, 3), round(e - t0, 3)) for t, (s, e, w) in runs],
        }

        log = self.ctx.log
        log.info("PARALLEL PROOF job %s: timeline (seconds since JOB_SUBMIT)", job["job_id"])
        for task_id, worker, start, end in proof["timeline"]:
            log.info("PARALLEL PROOF   %-28s %-10s %7.3f -> %7.3f", task_id, worker, start, end)
        log.info("PARALLEL PROOF job %s: %d task(s) on %d worker(s) %s, max %d running at once, "
                 "task time %.3fs in wall time %.3fs = %.2fx parallelism",
                 job["job_id"], proof["tasks"], len(workers), per_worker, overlap,
                 busy, wall, proof["parallelism"])
        return proof

    # -- worker lost -------------------------------------------------------------------

    def requeue_worker_tasks(self, worker_id):
        """Called by MembershipService (any thread) when a worker leaves or fails."""
        with self._lock:
            self.busy.pop(worker_id, None)
            lost = [t for t in self.tasks.values()
                    if t["state"] == "RUNNING" and t["worker"] == worker_id]
            for task in lost:
                task["state"], task["worker"] = "QUEUED", None
                self.queue.appendleft(task["task_id"])
            self.requeued += len(lost)
        if lost:
            self.ctx.log.warning("REQUEUE %d task(s) of lost worker %s: %s",
                                 len(lost), worker_id, ", ".join(t["task_id"] for t in lost))
        else:
            self.ctx.log.info("worker %s lost while idle; nothing to requeue", worker_id)
        self._wake.set()


def _short(value, limit=80):
    text = str(value)
    return text if len(text) <= limit else text[:limit - 3] + "..."


def main():
    args = node_arg_parser("Cluster master").parse_args()
    ctx = build_node(args, "master")
    node = Master(ctx)
    ctx.ep.start()
    node.membership.start()
    node.start()

    def on_stop():
        node.stop()
        node.membership.stop()

    run_until_stopped(ctx, on_stop=on_stop)


if __name__ == "__main__":
    main()
