# Distributed Resource Sharing and Processing System

Distributed Computing (CCZG 526), Lab Assignment II, Group 2.

A Python system that combines two architectures:

- **Peer-to-peer:** four peer nodes find each other and share resources directly, with no central coordinator.
- **Cluster computing:** a master splits heavy jobs into tasks and runs them in parallel on worker nodes.

A peer that needs heavy processing hands the job to the cluster through its bridge, and the result comes back to that peer. Nodes can join and leave at runtime, and the system keeps working.

## Architecture

```
   P2P overlay (no coordinator)                  Cluster

   peer1 <------> peer2                       +--> worker1
     ^  \        /  ^          JOB_SUBMIT     |
     |   \      /   |      ---------------> master --> worker2
     |    \    /    |      <---------------    |
     v     \  /     v          JOB_RESULT     +--> worker3
   peer3 <------> peer4

   Peers talk to each other directly.    The master splits each job into
   Any peer's bridge can send a job      tasks and runs them on the
   to the master.                        workers in parallel.
```

| Role | Count | What it does |
|---|---|---|
| Peer | 4 | Discovers other peers, keeps a resource registry, queries and fetches resources directly from other peers |
| Master | 1 | Accepts jobs from peers, splits them into tasks, dispatches to workers, combines results |
| Worker | 3 (more can join) | Runs tasks and returns results to the master |

Request flow:

1. A peer finds a resource by querying other peers (`RESOURCE_QUERY`, forwarded with a TTL), then fetches it from the holder (`RESOURCE_FETCH` / `RESOURCE_DATA`).
2. For heavy work, the peer's bridge sends `JOB_SUBMIT` to the master.
3. The master splits the job and sends `TASK_ASSIGN` to several workers.
4. Workers run in parallel and return `TASK_RESULT`.
5. The master combines the results and sends `JOB_RESULT` to the peer that asked.

## Requirements

- Python 3.9 or newer. There are no third-party packages to install.
- Linux, macOS or Windows. All nodes communicate over TCP.

## Repository layout

| Path | Contents | Owner |
|---|---|---|
| `common/` | Shared layer: TCP transport, message schema, membership table, config, logging, node startup | Anirudh |
| `config/nodes.json` | Node ids, roles, ports, seed peers and timing for a single machine | Anirudh |
| `config/nodes.multinode.json` | The same nodes mapped onto three machines | Anirudh |
| `peer/peer_node.py` | Peer: discovery, resource registry, direct messaging | Sri Harsha |
| `peer/bridge.py` | Bridge from a peer into the cluster; runs peers 3 and 4 | Chandra Hasitha |
| `cluster/master.py` | Task queue, dispatch, result collection, parallel execution proof | Kashif |
| `cluster/worker.py` | Task execution and result return | Abhirup |
| `joinleave/service.py` | Node join, heartbeat, leave and failure detection, used by every node | Sohail |
| `demo/join_leave_demo.py` | Scripted node addition and removal demo with measured timings | Sohail |
| `tests/` | Unit and integration tests | Sohail (common: Anirudh, peer: Sri Harsha) |
| `docs/INTERFACES.md` | Team contract: message formats and how the parts fit | Anirudh |
| `docs/GROUP-2-DRAFT.md` | Draft of the submission document | Anirudh |
| `outputs/` | Logs and screenshots kept as evidence | All |

## Running the system

Every node is a separate process. Open one terminal per node and run each command from the repository root:

```bash
# Cluster
python3 -m cluster.master  --id master
python3 -m cluster.worker  --id worker1
python3 -m cluster.worker  --id worker2
python3 -m cluster.worker  --id worker3

# Peers
python3 -m peer.peer_node  --id peer1
python3 -m peer.peer_node  --id peer2
python3 -m peer.peer_node  --id peer3
python3 -m peer.peer_node  --id peer4
```

Nodes can start in any order: a node keeps retrying its JOIN until the node it joins is up.

Each peer started in a terminal has a console. Type `help` for the full list; the main commands are:

| Command | What it does |
|---|---|
| `peers` | Live peers this node knows |
| `registry` | Who holds which resource |
| `find dataset-c` | Search the overlay and fetch from the first holder |
| `get dataset-d` | Fetch directly if the holder is known, otherwise search |
| `submit <kind> <json>` | Send a job to the cluster through the bridge |
| `stats` | Message counters |

peer1 to peer4 start with `dataset-a` to `dataset-d`.

Useful options:

| Option | Use |
|---|---|
| `--port 6204` | Required for a node that is not in the config, such as a worker added later |
| `--host <ip>` | Address this node advertises; needed for a late joiner when running across machines |
| `--config <file>` | Use another topology file, such as `config/nodes.multinode.json` |
| `--verbose` | Log every message sent and received |

Logs go to the terminal and to `logs/<id>.log`. Copy the ones you want as evidence into `outputs/logs/`.

### On the cloud lab

Ports are closed between the lab nodes, so the whole system runs on **one lab VM**, with every node as its own process on `127.0.0.1`. The lab VM's IP address changes each time the lab is stopped and started. That has no effect on this single-VM setup, because nothing uses the external IP.

`config/nodes.multinode.json` is provided for running across machines if the ports are opened. It has not been run that way. To use it, put the current lab IPs into the file each time the lab restarts, copy the repository to each machine, and start every node with `--config config/nodes.multinode.json`.

## Adding and removing nodes

Every node runs the membership service in `joinleave/service.py`.

| Role | Joins | Tracks (sends heartbeats to) |
|---|---|---|
| Peer | The seed peers (`peer1`, `peer2`) | Every other live peer |
| Worker | The master | The master |
| Master | Nobody | Every live worker |

The P2P overlay and the cluster each keep their own membership. Peers do not track workers, and workers do not track each other.

**Joining.** A new node sends `JOIN`. The receiver adds it and replies `JOIN_ACK` with its member list. A new peer then announces itself to every peer on that list, so all peers learn about it without a central registry.

**Leaving and failing.** There are three ways a node can disappear, and each is reported differently:

| What happens | How to cause it | How it is detected | Status |
|---|---|---|---|
| Graceful leave | `Ctrl+C` | The node sends `LEAVE` before it exits | `LEFT` |
| Crash | `kill -9 <pid>` | Its TCP channel closes, so it is detected at once | `DEAD` |
| Hang or network cut | Process frozen, socket still open | No heartbeat within `failure_timeout_s` (3.5s) | `DEAD` |

When the master loses a worker, it is told through `on_worker_lost`, so the worker's unfinished tasks can be requeued on the remaining workers.

**Restarting.** A node picks a new incarnation number each time it starts, so a restarted node always overrides old news that it was dead. If the master restarts, its workers notice and join it again on their own.

Heartbeat interval and failure timeout are set in the `timing` section of `config/nodes.json`.

### Demo: add and remove a worker

With the master and three workers running:

```bash
# Add a worker that is not in the config
python3 -m cluster.worker --id worker4 --port 6204

# Remove a worker by crashing it
kill -9 <pid of worker2>

# Remove a worker gracefully: press Ctrl+C in its terminal
```

The master's terminal shows each change with the current set of live workers:

```
MEMBER JOINED worker4 (worker). Alive workers: ['worker1', 'worker2', 'worker3', 'worker4']
MEMBER FAILED worker2 (worker): crashed or silent, last heard 1.0s ago. Alive workers: ['worker1', 'worker3', 'worker4']
LEAVE from worker3: graceful shutdown
MEMBER LEFT worker3 (worker): graceful leave. Alive workers: ['worker1', 'worker4']
```

The same works for peers: start `peer5` with `--port 6005` to add one, or stop any peer to remove it. The remaining peers keep sharing resources, even when a seed peer is gone.

### Scripted demo with timings

`demo/join_leave_demo.py` runs the whole add/remove sequence and measures, from the nodes' own logs, how quickly the others notice each change:

```bash
python3 -m demo.join_leave_demo p2p        # peers
python3 -m demo.join_leave_demo cluster    # master and workers
python3 -m demo.join_leave_demo all
```

Stop any running nodes first, because the demo uses the configured ports. It needs Linux or macOS.

| Scenario | Steps |
|---|---|
| `p2p` | 4 peers form the overlay; peer5 joins and fetches dataset-b; seed peer2 is killed; peer6 joins and still gets dataset-b from peer5's copy; peer4 leaves with Ctrl+C; dataset-d, which had no copy, is no longer found |
| `cluster` | master and 3 workers start; worker4 joins; worker2 is killed; worker1 is frozen with SIGSTOP and detected by timeout, then resumed with SIGCONT; worker3 leaves with Ctrl+C |

A typical run on one machine:

| Event | Detected in |
|---|---|
| New peer known by all peers | 15 ms |
| New worker known by the master | 44 ms, including process start-up |
| Crash (`kill -9`), peer or worker | under 1 ms |
| Graceful leave (Ctrl+C) | under 1 ms |
| Hang (`SIGSTOP`) | 3.65 s, set by `failure_timeout_s` |

Logs and a summary are saved in `outputs/logs/` as `joinleave-<scenario>-*`.

## Tests

Run the whole suite:

```bash
python3 -m unittest discover -s tests -t .
```

Run only the join and leave tests, with each test's name:

```bash
python3 -m unittest tests.test_joinleave -v
```

| File | What it covers |
|---|---|
| `tests/test_joinleave.py` | Join, late join on a new port, graceful leave, crash, silent node, restart, master restart, 4-peer discovery, late peer, seed peer crash |
| `tests/test_peer.py` | Peer discovery, gossip, resource registry, query and fetch, replication, runtime sharing |
| `tests/test_worker.py` | Worker task kinds, failures, long tasks not blocking, crash during a task |
| `tests/test_common_membership.py` | Membership table rules: ALIVE, DEAD, LEFT and incarnations |
| `tests/test_common_messages.py` | Message schema validation |
| `tests/test_common_transport.py` | TCP framing, ordering, reconnects and disconnect reporting |
| `tests/test_common_node_startup.py` | Each node type starts as its own process and writes its log |

The join and leave tests start real nodes on `127.0.0.1` with free ports and short timings (0.1s heartbeat, 0.5s timeout), so they finish in under 10 seconds.

## Message protocol

Every message is a JSON envelope `{type, sender, msg_id, ts, payload}`, sent with a 4-byte length prefix over TCP. `common/messages.py` defines each payload's exact fields and rejects anything else. See [docs/INTERFACES.md](docs/INTERFACES.md) for the full list of messages and which nodes send them.

## Team

| Member | Part |
|---|---|
| Anirudh Gudipati | Shared common module, repo setup, submission document, recording |
| Shaik Baleeghuddin Kashif | Cluster master: task queue, dispatch, result collection, parallel execution proof |
| Vinjanampati Sri Harsha | Peer nodes 1 and 2: direct P2P messaging, peer discovery, resource registry |
| Nerella Chandra Hasitha | Peer nodes 3 and 4, and the bridge from a peer into the cluster |
| Abhirup Bhattacharjee | Cluster workers: task execution and result return |
| Sohail Quazi | Node join and leave, add/remove demo, test cases, P2P vs cluster comparison, README |
