"""
Cluster worker. Owner: Abhirup.

    python -m cluster.worker --id worker1

To do:
  * Execute each TASK_ASSIGN and return a TASK_RESULT to the master.
  * Run tasks on a separate thread. Handlers share one dispatcher thread, so a
    task run inside the handler would block every other message.

Stub: the methods below are the agreed interface and hold no logic yet.
"""

from common import MsgType, build_node, node_arg_parser, run_until_stopped
from joinleave.service import MembershipService

import threading
import time


class Worker:
    def __init__(self, ctx):
        self.ctx = ctx
        self.membership = MembershipService(ctx)

        ctx.ep.on(MsgType.TASK_ASSIGN, self.on_task_assign)

    def on_task_assign(self, msg):
        def run_task():
            start = time.time()

            task_id = msg["task_id"]
            job_id = msg["job_id"]
            kind = msg["kind"]
            params = msg["params"]

            try:
                # Kashif and Abhirup must agree on supported kinds

                if kind == "sum":
                    result = sum(params["numbers"])

                elif kind == "word_count":
                    result = len(params["text"].split())

                else:
                    raise ValueError(
                        f"Unsupported job kind: {kind}"
                    )

                status = "SUCCESS"

            except Exception as exc:
                result = str(exc)
                status = "FAILED"

            elapsed_s = round(time.time() - start, 4)

            self.ctx.ep.send(
                "master",
                MsgType.TASK_RESULT,
                task_id=task_id,
                job_id=job_id,
                worker_id=self.ctx.node_id,
                status=status,
                result=result,
                elapsed_s=elapsed_s,
            )

        threading.Thread(
            target=run_task,
            daemon=True
        ).start()


def main():
    args = node_arg_parser("Cluster worker").parse_args()

    ctx = build_node(args, "worker")

    node = Worker(ctx)

    ctx.ep.start()
    node.membership.start()

    run_until_stopped(
        ctx,
        on_stop=node.membership.stop
    )


if __name__ == "__main__":
    main()