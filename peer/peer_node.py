"""Peer node. Owner: Sri Harsha (core). Chandra Hasitha runs peers 3 and 4.

All peers run this same file:

    python -m peer.peer_node --id peer1

To do:
  * Discovery: learn other peers from the seeds and gossip, never from a
    central list. Add each discovered peer to ctx.ep with add_route().
  * Resource registry: which resources this peer holds and who holds others.
  * Direct messaging: flood RESOURCE_QUERY with a ttl, answer with
    RESOURCE_HIT, then fetch directly with RESOURCE_FETCH / RESOURCE_DATA.

Stub: the handlers below are the agreed interface and hold no logic yet.
"""

from common import MsgType, build_node, node_arg_parser, run_until_stopped
from joinleave.service import MembershipService
from peer.bridge import Bridge


class PeerNode:
    def __init__(self, ctx):
        self.ctx = ctx
        self.membership = MembershipService(ctx)
        self.bridge = Bridge(ctx)

        ctx.ep.on(MsgType.GOSSIP, self.on_gossip)
        ctx.ep.on(MsgType.RESOURCE_ANNOUNCE, self.on_resource_announce)
        ctx.ep.on(MsgType.RESOURCE_QUERY, self.on_resource_query)
        ctx.ep.on(MsgType.RESOURCE_HIT, self.on_resource_hit)
        ctx.ep.on(MsgType.RESOURCE_FETCH, self.on_resource_fetch)
        ctx.ep.on(MsgType.RESOURCE_DATA, self.on_resource_data)

    def on_gossip(self, msg):
        raise NotImplementedError("Sri Harsha: merge members and add routes")

    def on_resource_announce(self, msg):
        raise NotImplementedError("Sri Harsha: update the resource registry")

    def on_resource_query(self, msg):
        raise NotImplementedError("Sri Harsha: reply if held, forward while ttl > 0")

    def on_resource_hit(self, msg):
        raise NotImplementedError("Sri Harsha: choose a holder and fetch")

    def on_resource_fetch(self, msg):
        raise NotImplementedError("Sri Harsha: reply with RESOURCE_DATA")

    def on_resource_data(self, msg):
        raise NotImplementedError("Sri Harsha: store the fetched resource")


def main():
    args = node_arg_parser("Peer node").parse_args()
    ctx = build_node(args, "peer")
    node = PeerNode(ctx)
    ctx.ep.start()
    node.membership.start()
    run_until_stopped(ctx, on_stop=node.membership.stop)


if __name__ == "__main__":
    main()
