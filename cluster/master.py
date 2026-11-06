"""Cluster master. Owner: Kashif.

    python -m cluster.master --id master

To do:
  * Split each JOB_SUBMIT into tasks and put them on a task queue.
  * Dispatch tasks with TASK_ASSIGN to ctx.members.alive("worker").
  * Collect TASK_RESULTs and send one JOB_RESULT to the job's origin.
  * Produce the parallel execution proof, such as overlapping task times
    across workers and the speedup with 1, 2 and 3 workers.

Sohail's join/leave work calls requeue_worker_tasks() when a worker is lost.

Stub: the methods below are the agreed interface and hold no logic yet.
"""

from common import MsgType, build_node, node_arg_parser, run_until_stopped
from joinleave.service import MembershipService


class Master:
    def __init__(self, ctx):
        self.ctx = ctx
        self.membership = MembershipService(ctx)
        ctx.ep.on(MsgType.JOB_SUBMIT, self.on_job_submit)
        ctx.ep.on(MsgType.TASK_RESULT, self.on_task_result)

    def on_job_submit(self, msg):
        raise NotImplementedError("Kashif: reply JOB_ACCEPTED, split and queue tasks")

    def on_task_result(self, msg):
        raise NotImplementedError("Kashif: collect, then send JOB_RESULT when complete")

    def requeue_worker_tasks(self, worker_id):
        raise NotImplementedError("Kashif: put this worker's unfinished tasks back")


def main():
    args = node_arg_parser("Cluster master").parse_args()
    ctx = build_node(args, "master")
    node = Master(ctx)
    ctx.ep.start()
    node.membership.start()
    run_until_stopped(ctx, on_stop=node.membership.stop)


if __name__ == "__main__":
    main()
