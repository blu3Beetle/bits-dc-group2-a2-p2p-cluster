"""Startup and shutdown shared by every node's entry point."""

import argparse
import logging
import signal
import threading
import time
from dataclasses import dataclass

from common.config import Config, load_config
from common.logger import get_logger
from common.membership import MembershipTable
from common.transport import Endpoint


@dataclass
class NodeContext:
    """Everything a node needs, built once at startup."""
    node_id: str
    role: str
    cfg: Config
    ep: Endpoint
    members: MembershipTable
    log: object

    @property
    def incarnation(self):
        """Current value: it rises if this node refutes a false rumour."""
        return self.members.get(self.node_id).incarnation


def node_arg_parser(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--id", required=True, help="node id, such as peer1 or worker2")
    parser.add_argument("--port", type=int, help="required for a node not in the config")
    parser.add_argument("--host", help="override the configured host")
    parser.add_argument("--config", help="path to a topology file")
    parser.add_argument("--verbose", action="store_true", help="log every message sent and received")
    return parser


def build_node(args, role):
    """Create a node's endpoint and table. The caller starts the endpoint."""
    cfg = load_config(args.config)
    log = get_logger(args.id, level=logging.DEBUG if args.verbose else logging.INFO)
    host, port = cfg.listen_address(args.id, host=args.host, port=args.port)
    routes = {n: cfg.address(n) for n in cfg.bootstrap_ids() if n != args.id}
    ep = Endpoint(args.id, host, port, routes=routes, logger=log)

    # Milliseconds since the epoch: each restart gets a larger incarnation.
    incarnation = int(time.time() * 1000)
    members = MembershipTable(args.id, cfg.timing["failure_timeout_s"])
    members.add(args.id, host, port, role, incarnation)

    ep.on_any(lambda msg: members.touch(msg["sender"]))
    ep.on_disconnect(members.mark_dead)
    return NodeContext(args.id, role, cfg, ep, members, log)


def run_until_stopped(ctx, on_stop=None):
    """Block until Ctrl+C or SIGTERM, then run on_stop and close the endpoint."""
    stop = threading.Event()

    def request_stop(_signum, _frame):
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    # A timed wait, because an untimed one ignores Ctrl+C on Windows.
    while not stop.wait(0.2):
        pass

    ctx.log.info("shutting down")
    try:
        if on_stop is not None:
            on_stop()
    finally:
        ctx.ep.stop()
