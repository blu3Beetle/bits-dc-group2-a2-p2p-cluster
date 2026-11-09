"""
Cluster worker. Owner: Abhirup.

    python -m cluster.worker --id worker1

Each TASK_ASSIGN from the master runs on its own thread, because handlers
share one dispatcher thread and a long task inside the handler would block
every other message. The result goes back to the master as TASK_RESULT.

Supported task kinds (the master must split jobs into these):

    sum           params {"numbers": [int, ...]}     result: their sum
    word_count    params {"text": str}               result: number of words
    count_primes  params {"start": int, "end": int}  result: primes in [start, end)

count_primes is CPU heavy, so it is the one to use for the 1 vs 3 workers
speedup demo. Status is "SUCCESS" or "FAILED".
"""

import threading
import time

from common import MsgType, TransportError, build_node, node_arg_parser, run_until_stopped
from joinleave.service import MembershipService


def count_primes(start, end):
    """Plain trial division, kept simple on purpose so it costs real CPU time."""
    count = 0
    for n in range(max(start, 2), end):
        i = 2
        while i * i <= n:
            if n % i == 0:
                break
            i += 1
        else:
            count += 1
    return count


class Worker:
    def __init__(self, ctx):
        self.ctx = ctx
        self.membership = MembershipService(ctx)

        ctx.ep.on(MsgType.TASK_ASSIGN, self.on_task_assign)

    def on_task_assign(self, msg):
        threading.Thread(target=self.run_task, args=(msg,), daemon=True).start()

    def run_task(self, msg):
        # Messages are dicts: the task fields are inside msg["payload"].
        payload = msg["payload"]
        task_id = payload["task_id"]
        job_id = payload["job_id"]
        kind = payload["kind"]
        params = payload["params"]

        self.ctx.log.info("TASK START %s (job %s, kind %s)", task_id, job_id, kind)
        start = time.perf_counter()
        try:
            if kind == "sum":
                result = sum(params["numbers"])
            elif kind == "word_count":
                result = len(params["text"].split())
            elif kind == "count_primes":
                result = count_primes(params["start"], params["end"])
            else:
                raise ValueError("Unsupported task kind: {}".format(kind))
            status = "SUCCESS"
        except Exception as exc:
            result = str(exc)
            status = "FAILED"
        elapsed_s = round(time.perf_counter() - start, 4)

        self.ctx.log.info("TASK END   %s (job %s) %s in %.3fs, result %s",
                          task_id, job_id, status, elapsed_s, result)
        try:
            self.ctx.ep.send(
                self.ctx.cfg.master_id,
                MsgType.TASK_RESULT,
                task_id=task_id,
                job_id=job_id,
                worker_id=self.ctx.node_id,
                status=status,
                result=result,
                elapsed_s=elapsed_s,
            )
        except TransportError as exc:
            # The master may have gone away. It requeues our tasks when it
            # comes back, so the result is only logged here.
            self.ctx.log.warning("could not return result of %s to the master: %s", task_id, exc)


def main():
    args = node_arg_parser("Cluster worker").parse_args()
    ctx = build_node(args, "worker")
    node = Worker(ctx)
    ctx.ep.start()
    node.membership.start()
    run_until_stopped(ctx, on_stop=node.membership.stop)


if __name__ == "__main__":
    main()
