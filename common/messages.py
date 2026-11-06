"""The single source of truth for the wire protocol.

Every message has this envelope:

    {"type": ..., "sender": ..., "msg_id": ..., "ts": ..., "payload": {...}}

Each payload must contain exactly the keys listed in SCHEMA. Unknown types,
missing keys and extra keys are all rejected, so a mismatch between two
teammates' code fails loudly at the sender rather than silently at the
receiver. To add or change a field, change SCHEMA here.

`ts` is wall-clock time for logs only. Never use it to order events.
"""

import time
import uuid


class MsgType:
    # Membership (peers and workers)
    JOIN = "JOIN"
    JOIN_ACK = "JOIN_ACK"
    LEAVE = "LEAVE"
    HEARTBEAT = "HEARTBEAT"
    GOSSIP = "GOSSIP"

    # Peer to peer
    RESOURCE_ANNOUNCE = "RESOURCE_ANNOUNCE"
    RESOURCE_QUERY = "RESOURCE_QUERY"
    RESOURCE_HIT = "RESOURCE_HIT"
    RESOURCE_FETCH = "RESOURCE_FETCH"
    RESOURCE_DATA = "RESOURCE_DATA"

    # Peer to cluster
    JOB_SUBMIT = "JOB_SUBMIT"
    JOB_ACCEPTED = "JOB_ACCEPTED"
    JOB_RESULT = "JOB_RESULT"

    # Inside the cluster
    TASK_ASSIGN = "TASK_ASSIGN"
    TASK_RESULT = "TASK_RESULT"


SCHEMA = {
    MsgType.JOIN: ("host", "port", "role", "incarnation"),
    MsgType.JOIN_ACK: ("members",),
    MsgType.LEAVE: ("reason",),
    MsgType.HEARTBEAT: ("incarnation",),
    MsgType.GOSSIP: ("members",),

    MsgType.RESOURCE_ANNOUNCE: ("resources",),
    MsgType.RESOURCE_QUERY: ("query_id", "name", "origin", "ttl"),
    MsgType.RESOURCE_HIT: ("query_id", "name", "holder"),
    MsgType.RESOURCE_FETCH: ("name",),
    MsgType.RESOURCE_DATA: ("name", "found", "content"),

    MsgType.JOB_SUBMIT: ("job_id", "origin", "kind", "params"),
    MsgType.JOB_ACCEPTED: ("job_id", "task_count"),
    MsgType.JOB_RESULT: ("job_id", "status", "result", "elapsed_s"),

    MsgType.TASK_ASSIGN: ("task_id", "job_id", "kind", "params"),
    MsgType.TASK_RESULT: ("task_id", "job_id", "worker_id", "status", "result", "elapsed_s"),
}

_ENVELOPE = {"type", "sender", "msg_id", "ts", "payload"}


class SchemaError(ValueError):
    """A message does not match the protocol."""


def validate(msg):
    """Return msg unchanged, or raise SchemaError."""
    if not isinstance(msg, dict):
        raise SchemaError("message must be a dict, got {}".format(type(msg).__name__))

    keys = set(msg)
    if keys != _ENVELOPE:
        raise SchemaError("bad envelope: missing {}, unexpected {}".format(
            sorted(_ENVELOPE - keys), sorted(keys - _ENVELOPE)))

    msg_type = msg["type"]
    if not isinstance(msg_type, str) or msg_type not in SCHEMA:
        raise SchemaError("unknown message type {!r}".format(msg_type))

    if not isinstance(msg["sender"], str) or not msg["sender"]:
        raise SchemaError("{}: sender must be a non-empty string".format(msg_type))

    payload = msg["payload"]
    if not isinstance(payload, dict):
        raise SchemaError("{}: payload must be a dict".format(msg_type))

    expected = set(SCHEMA[msg_type])
    actual = set(payload)
    if actual != expected:
        raise SchemaError("{}: missing {}, unexpected {}".format(
            msg_type, sorted(expected - actual), sorted(actual - expected)))
    return msg


def make_message(msg_type, sender, /, **payload):
    """Build and validate a message."""
    return validate({
        "type": msg_type,
        "sender": sender,
        "msg_id": uuid.uuid4().hex[:12],
        "ts": time.time(),
        "payload": payload,
    })
