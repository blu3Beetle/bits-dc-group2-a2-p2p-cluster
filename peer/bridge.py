"""Bridge from a peer into the cluster. Owner: Chandra Hasitha.

Every peer has a Bridge. It turns a peer's request into a JOB_SUBMIT for the
master, and delivers the JOB_RESULT back to the peer.

Stub: the methods below are the agreed interface and hold no logic yet.
"""

from common import MsgType


class Bridge:
    def __init__(self, ctx):
        self.ctx = ctx
        ctx.ep.on(MsgType.JOB_ACCEPTED, self.on_job_accepted)
        ctx.ep.on(MsgType.JOB_RESULT, self.on_job_result)

    def submit(self, kind, params):
        """Send a job to the master and return its job_id."""
        raise NotImplementedError("Hasitha: send JOB_SUBMIT to ctx.cfg.master_id")

    def on_job_accepted(self, msg):
        raise NotImplementedError("Hasitha: record that the job is running")

    def on_job_result(self, msg):
        raise NotImplementedError("Hasitha: hand the result back to the peer")
