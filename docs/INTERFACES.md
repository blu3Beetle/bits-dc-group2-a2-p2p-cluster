# Team Interface Guide

How the parts fit together. If your code and this guide disagree, raise it with the team rather than working around it.

## Who owns what

Each file below is a stub: the classes and handlers exist, and you fill in the logic. Replace each `NotImplementedError` with your code.

| Owner | Files | Part |
|---|---|---|
| Anirudh | `common/`, `config/`, `docs/GROUP-2-DRAFT.md` | Shared layer, repo, submission doc, recording |
| Kashif | `cluster/master.py` | Task queue, dispatch, result collection, parallel proof |
| Abhirup | `cluster/worker.py` | Task execution, result return |
| Sri Harsha | `peer/peer_node.py` | P2P messaging, discovery, resource registry |
| Chandra Hasitha | `peer/bridge.py`, running peers 3 and 4 | Peer request into the cluster |
| Sohail | `joinleave/service.py`, `tests/`, `README.md` | Join/leave, demo scenarios, test cases, comparison |

All four peers run the same `peer/peer_node.py` with a different `--id`.

## Running

By default every node runs on one machine (`config/nodes.json`, all on 127.0.0.1). For three machines, see "Running across lab nodes" below. Each node is a separate process in its own CLI, started from the repo root:

```bash
python3 -m cluster.master --id master
```

```bash
python3 -m cluster.worker --id worker1
```

```bash
python3 -m peer.peer_node --id peer1
```

A node that joins later is not in `config/nodes.json`, so give it a free port:

```bash
python3 -m cluster.worker --id worker4 --port 6204
```

Logs go to the terminal and to `logs/<id>.log`. Copy logs you want as evidence into `outputs/logs/`. Add `--verbose` to log every message sent and received.

Run the tests:

```bash
python3 -m unittest discover -s tests -t .
```

## Running across lab nodes

`config/nodes.multinode.json` maps the same nodes onto three machines. The code is identical, and only the config changes. The demo ran this way on three lab VMs, after opening ports 6000-6300 in each VM's firewalld.

1. Replace the masked IPs in that file (`10.3.20.xxx` and so on) with the current lab IPs, since they change when the lab restarts.
2. Copy the repo and the file to all three machines.
3. Start each node on the machine that file assigns it to:

```bash
python3 -m cluster.master --id master --config config/nodes.multinode.json
```

A node that joins later also needs `--host` with its machine's IP, otherwise it advertises `127.0.0.1` and replies cannot reach it.

## Rules

1. Do not open sockets yourself. Use `ctx.ep`.
2. Do not hardcode hosts or ports. They come from `config/nodes.json`.
3. To add a message or a field, change `SCHEMA` in `common/messages.py`. Tell the team, because it affects everyone.
4. Handlers share one thread. Run anything slow on your own thread.
5. Commit your own work under your own GitHub account. Marks depend on individual contribution.

## Using the common module

`build_node()` gives every node a context, `ctx`:

| Field | Use |
|---|---|
| `ctx.ep` | Send and receive messages |
| `ctx.members` | Which nodes are alive |
| `ctx.cfg` | Topology, seeds, timing |
| `ctx.log` | Logger for the terminal and log file |
| `ctx.node_id`, `ctx.role`, `ctx.incarnation` | This node's identity |

```python
# Receive: msg has type, sender, msg_id, ts and payload
ctx.ep.on(MsgType.RESOURCE_FETCH, self.on_resource_fetch)

# Send: keyword arguments must match SCHEMA exactly
ctx.ep.send("peer2", MsgType.RESOURCE_FETCH, name="dataset-a")

# Send to many: returns the ids that could not be reached
failed = ctx.ep.broadcast(["peer2", "peer3"], MsgType.GOSSIP, members=ctx.members.snapshot())

# Who is alive
workers = ctx.members.alive("worker")

# Message counters for the comparison section
ctx.ep.stats()
```

`send()` raises `SchemaError` for a malformed message and `TransportError` if the target cannot be reached. Handle `TransportError`, because nodes can leave at any time.

A sender's address is learned automatically when it first connects to you, so you can always reply to `msg["sender"]`. To contact a node you only heard about through gossip, call `ctx.ep.add_route(node_id, host, port)` first.

## Messages

`common/messages.py` is the authority on these.

### Membership (Sohail)

| Type | Direction | Payload |
|---|---|---|
| `JOIN` | New node to seed or master | `host`, `port`, `role`, `incarnation` |
| `JOIN_ACK` | Reply to `JOIN` | `members` |
| `LEAVE` | Leaving node to others | `reason` |
| `HEARTBEAT` | Node to the nodes tracking it | `incarnation` |
| `GOSSIP` | Peer to peer | `members` |

`members` is the list from `ctx.members.snapshot()`. Apply one with `ctx.members.merge(...)`.

### Peer to peer (Sri Harsha)

| Type | Direction | Payload |
|---|---|---|
| `RESOURCE_ANNOUNCE` | Peer to peers | `resources` |
| `RESOURCE_QUERY` | Peer to peers, forwarded while `ttl` > 0 | `query_id`, `name`, `origin`, `ttl` |
| `RESOURCE_HIT` | Holder to `origin` | `query_id`, `name`, `holder` |
| `RESOURCE_FETCH` | Requester to holder | `name` |
| `RESOURCE_DATA` | Holder to requester | `name`, `found`, `content` |

### Peer to cluster (Chandra Hasitha and Kashif)

| Type | Direction | Payload |
|---|---|---|
| `JOB_SUBMIT` | Peer to master | `job_id`, `origin`, `kind`, `params` |
| `JOB_ACCEPTED` | Master to peer | `job_id`, `task_count` |
| `JOB_RESULT` | Master to `origin` | `job_id`, `status`, `result`, `elapsed_s` |

### Inside the cluster (Kashif and Abhirup)

| Type | Direction | Payload |
|---|---|---|
| `TASK_ASSIGN` | Master to worker | `task_id`, `job_id`, `kind`, `params` |
| `TASK_RESULT` | Worker to master | `task_id`, `job_id`, `worker_id`, `status`, `result`, `elapsed_s` |

Kashif and Abhirup need to agree on the job `kind` values and their `params`.

## Request flow

1. A peer finds a resource by querying other peers directly, then fetches it from the holder.
2. For heavy work on it, the peer's bridge sends `JOB_SUBMIT` to the master.
3. The master splits the job and sends `TASK_ASSIGN` to several workers.
4. Workers run in parallel and return `TASK_RESULT`.
5. The master combines the results and sends `JOB_RESULT` to the peer that asked.

## Membership events

`build_node()` already records direct contact and closed channels in `ctx.members`. React to changes with:

```python
ctx.members.on_join(lambda member: ...)    # new, restarted, or wrongly thought dead
ctx.members.on_leave(lambda member: ...)   # graceful exit: LEAVE received
ctx.members.on_fail(lambda member: ...)    # crash or silence
```

These callbacks may run on the heartbeat thread rather than the handler thread, so use a lock around any state they share with your handlers.

Ctrl+C is a graceful leave. `kill -9 <pid>` is a crash.
