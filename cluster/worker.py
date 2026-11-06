"""Cluster worker. Owner: Abhirup.

    python -m cluster.worker --id worker1

To do:
  * Execute each TASK_ASSIGN and return a TASK_RESULT to the master.
  * Run tasks on a separate thread. Handlers share one dispatcher thread, so a
    task run inside the handler would block every other message.

Stub: the methods below are the agreed interface and hold no logic yet.
"""

from common import MsgType, build_node, node_arg_parser, run_until_stopped
from joinleave.service import MembershipService


class Worker:
    def __init__(self, ctx):
        self.ctx = ctx
        self.membership = MembershipService(ctx)
        ctx.ep.on(MsgType.TASK_ASSIGN, self.on_task_assign)

    def on_task_assign(self, msg):
        raise NotImplementedError("Abhirup: run the task, reply with TASK_RESULT")


def main():
    args = node_arg_parser("Cluster worker").parse_args()
    ctx = build_node(args, "worker")
    node = Worker(ctx)
    ctx.ep.start()
    node.membership.start()
    run_until_stopped(ctx, on_stop=node.membership.stop)


if __name__ == "__main__":
    main()
