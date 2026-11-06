"""Join and leave handling for every node. Owner: Sohail.

To do:
  * start(): announce with JOIN (peers to the seeds, workers to the master)
    and run a thread that sends HEARTBEAT and calls ctx.members.sweep().
  * stop(): send LEAVE so others see a graceful exit rather than a crash.
  * Handle JOIN, JOIN_ACK, LEAVE and HEARTBEAT using ctx.members.
  * React through ctx.members.on_join / on_leave / on_fail, for example by
    calling Master.requeue_worker_tasks() when a worker is lost.

Interval and timeout values are in ctx.cfg.timing.

Stub: start() and stop() only log, so that nodes can run before this is built.
"""

from common import MsgType


class MembershipService:
    def __init__(self, ctx):
        self.ctx = ctx
        ctx.ep.on(MsgType.JOIN, self.on_join)
        ctx.ep.on(MsgType.JOIN_ACK, self.on_join_ack)
        ctx.ep.on(MsgType.LEAVE, self.on_leave)
        ctx.ep.on(MsgType.HEARTBEAT, self.on_heartbeat)

    def start(self):
        self.ctx.log.warning("TODO(Sohail): membership start is not implemented")

    def stop(self):
        self.ctx.log.warning("TODO(Sohail): membership stop is not implemented")

    def on_join(self, msg):
        raise NotImplementedError("Sohail: add the node and reply with JOIN_ACK")

    def on_join_ack(self, msg):
        raise NotImplementedError("Sohail: merge the member list and add routes")

    def on_leave(self, msg):
        raise NotImplementedError("Sohail: mark the node as left")

    def on_heartbeat(self, msg):
        raise NotImplementedError("Sohail: handle a restart if incarnation is newer")
