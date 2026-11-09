"""Bridge from a peer into the cluster. Owner: Chandra Hasitha.

Every peer has a Bridge. It turns a peer's request into a JOB_SUBMIT for the
master, and delivers the JOB_RESULT back to the peer.

Flow (see README, "Request flow"):

    peer ──JOB_SUBMIT──▶ master      submit()            job state: SUBMITTED
    peer ◀─JOB_ACCEPTED─ master      on_job_accepted()   job state: RUNNING
    peer ◀─JOB_RESULT─── master      on_job_result()     job state: DONE / FAILED

Between JOB_ACCEPTED and JOB_RESULT the master splits the job into tasks,
sends TASK_ASSIGN to the workers, collects TASK_RESULT and combines them.
The bridge does not see any of that; it only talks to the master.
"""

import threading
import time
import uuid

from common import MsgType


class Bridge:
    def __init__(self, ctx):
        self.ctx = ctx

        # job_id -> {kind, params, status, task_count, result, elapsed_s,
        #            submitted_at, accepted_at, finished_at, done}
        self.jobs = {}

        # Optional hook. A peer may set `bridge.result_callback = fn`;
        # it is called as fn(job_id, status, result, elapsed_s) when a
        # JOB_RESULT arrives. Leave it as None to only use self.jobs.
        self.result_callback = None
        self._lock = threading.Lock()

        ctx.ep.on(MsgType.JOB_ACCEPTED, self.on_job_accepted)
        ctx.ep.on(MsgType.JOB_RESULT, self.on_job_result)

        ctx.log.info(
            "[BRIDGE] %s ready: jobs go to master '%s', results come back here",
            ctx.node_id, ctx.cfg.master_id,
        )

    def submit(self, kind, params):
        """Send a job to the master and return its job_id."""
        ctx = self.ctx
        master_id = ctx.cfg.master_id

        # Unique id: peer id + short random suffix, so two peers (or two
        # jobs from the same peer) can never collide at the master.
        job_id = f"{ctx.node_id}-{uuid.uuid4().hex[:8]}"

        # Record the job BEFORE sending, so a very fast JOB_ACCEPTED or
        # JOB_RESULT can never arrive for a job we do not know about.
        with self._lock:
            self.jobs[job_id] = {
                "kind": kind,
                "params": params,
                "status": "SUBMITTED",
                "task_count": None,
                "result": None,
                "elapsed_s": None,
                "submitted_at": time.time(),
                "accepted_at": None,
                "finished_at": None,
                "done": threading.Event(),
            }

        ctx.log.info(
            "[BRIDGE] STEP 1/3 submit: %s sends JOB_SUBMIT to master '%s' "
            "(job_id=%s, kind=%s, params=%s)",
            ctx.node_id, master_id, job_id, kind, params,
        )

        try:
            # Keyword arguments must match the JOB_SUBMIT schema exactly:
            # job_id, origin, kind, params.
            ctx.ep.send(
                master_id,
                MsgType.JOB_SUBMIT,
                job_id=job_id,
                origin=ctx.node_id,
                kind=kind,
                params=params,
            )
        except Exception as exc:
            # Master down or unreachable: mark the job failed and wake any
            # waiter, instead of leaving the peer waiting forever.
            with self._lock:
                job = self.jobs[job_id]
                job["status"] = "FAILED"
                job["result"] = f"could not reach master '{master_id}': {exc}"
                job["finished_at"] = time.time()
                job["done"].set()
            ctx.log.error(
                "[BRIDGE] submit FAILED for job %s: master '%s' unreachable (%s). "
                "Check that the master is running; the peer can retry.",
                job_id, master_id, exc,
            )
            return job_id

        ctx.log.info(
            "[BRIDGE] JOB_SUBMIT delivered for job %s; waiting for JOB_ACCEPTED "
            "from '%s'", job_id, master_id,
        )
        return job_id

    def on_job_accepted(self, msg):
        ctx = self.ctx
        payload = msg.payload
        job_id = payload["job_id"]
        task_count = payload["task_count"]

        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                ctx.log.warning(
                    "[BRIDGE] JOB_ACCEPTED from '%s' for unknown job %s "
                    "(not submitted by this bridge); ignored",
                    msg.sender, job_id,
                )
                return
            job["status"] = "RUNNING"
            job["task_count"] = task_count
            job["accepted_at"] = time.time()
            wait_s = job["accepted_at"] - job["submitted_at"]

        ctx.log.info(
            "[BRIDGE] STEP 2/3 accepted: master '%s' accepted job %s and split "
            "it into %s task(s) for the workers (acknowledged in %.3fs). "
            "Job is now RUNNING on the cluster; waiting for JOB_RESULT.",
            msg.sender, job_id, task_count, wait_s,
        )

    def on_job_result(self, msg):
        ctx = self.ctx
        payload = msg.payload
        job_id = payload["job_id"]
        status = payload["status"]
        result = payload["result"]
        elapsed_s = payload["elapsed_s"]

        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                ctx.log.warning(
                    "[BRIDGE] JOB_RESULT from '%s' for unknown job %s "
                    "(not submitted by this bridge); ignored",
                    msg.sender, job_id,
                )
                return
            job["status"] = "DONE" if str(status).lower() in ("done", "ok", "success", "completed") else "FAILED"
            job["result"] = result
            job["elapsed_s"] = elapsed_s
            job["finished_at"] = time.time()
            round_trip_s = job["finished_at"] - job["submitted_at"]
            tasks = job["task_count"]
            kind = job["kind"]
            final_status = job["status"]
            job["done"].set()   # wake anyone waiting on this job

        ctx.log.info(
            "[BRIDGE] STEP 3/3 result: job %s (%s) finished with status '%s'. "
            "Cluster time %.3fs for %s task(s), round trip at %s %.3fs. "
            "Result: %s",
            job_id, kind, status, elapsed_s if elapsed_s is not None else -1.0,
            tasks, ctx.node_id, round_trip_s, result,
        )

        # Hand the result back to the peer, if it registered a hook.
        callback = self.result_callback
        if callback is not None:
            try:
                callback(job_id, final_status, result, elapsed_s)
            except Exception:
                ctx.log.exception(
                    "[BRIDGE] result_callback raised for job %s; result is "
                    "still stored in bridge.jobs", job_id,
                )
