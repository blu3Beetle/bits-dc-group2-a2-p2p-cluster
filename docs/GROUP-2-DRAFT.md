# Distributed Resource Sharing and Processing System using Peer-to-Peer and Cluster Computing

**Course:** Distributed Computing (CCZG 526)
**Assignment:** Lab Assignment II

> Placeholder, to be completed last. The final version is exported to `docs/GROUP-2.pdf` and submitted through Taxila only.

---

## Group Number

2

## Video Recording Link

`[PLACEHOLDER: Google Drive link to the recorded call]`

## GitHub Repo

https://github.com/blu3Beetle/bits-dc-group2-a2-p2p-cluster

## Source Code Zip

`[PLACEHOLDER: Google Drive link to the final zip]`

## Contribution Table

| Sl. No. | Name (as appears in Canvas) | ID No. | Contribution |
|---|---|---|---|
| 1 | Anirudh Gudipati | 2025MT03510 | `[PLACEHOLDER]` |
| 2 | Shaik Baleeghuddin Kashif | 2026NS03020 | `[PLACEHOLDER]` |
| 3 | Vinjanampati Sri Harsha | 2026NS03002 | `[PLACEHOLDER]` |
| 4 | Nerella Chandra Hasitha | 2025MT03502 | `[PLACEHOLDER]` |
| 5 | Abhirup Bhattacharjee | 2025MT03584 | `[PLACEHOLDER]` |
| 6 | Sohail Quazi | 2025MT03580 | `[PLACEHOLDER]` |

## Language Used for the Implementation

Python 3

## Deployment Note

`[PLACEHOLDER: ports are closed between lab nodes, so all nodes ran as separate OS processes in separate CLIs on one lab VM, communicating only over TCP. Add the VM's core count and what this limits. Say that config/nodes.multinode.json is provided for multi-node use but was not run across separate machines.]`

## System Overview

`[PLACEHOLDER: architecture diagram, node roles, request flow]`

## Requirement Coverage

| Requirement | Where shown |
|---|---|
| At least 4 peer nodes | `[PLACEHOLDER]` |
| Direct peer communication with no central coordinator | `[PLACEHOLDER]` |
| Resource/request sharing between peers | `[PLACEHOLDER]` |
| Cluster with master and multiple workers | `[PLACEHOLDER]` |
| Tasks distributed among workers | `[PLACEHOLDER]` |
| Parallel execution of multiple tasks | `[PLACEHOLDER]` |
| Peer request processed using cluster resources | `[PLACEHOLDER]` |
| Addition or removal of a node, with analysis | `joinleave/service.py`; `demo/join_leave_demo.py`; `tests/test_joinleave.py`; Node Addition and Removal Analysis section below |

## Test Cases

All automated tests run with `python3 -m unittest discover -s tests -t .`. The membership tests start real nodes over TCP on `127.0.0.1` with short timings (0.1 s heartbeat, 0.5 s timeout). The demo cases (D1 to D10) run with `python3 -m demo.join_leave_demo all`, using the real timings (1 s heartbeat, 3.5 s timeout), and their logs are in `outputs/logs/`.

**Node join and leave** (`tests/test_joinleave.py`)

| # | Test case | Expected result | Result |
|---|---|---|---|
| 1 | Start the master and 3 workers | Each worker joins the master; workers track only the master | Pass |
| 2 | Run a healthy cluster for 3 timeout periods | No node is reported failed | Pass |
| 3 | Start a worker before the master | The worker keeps retrying and joins once the master is up | Pass |
| 4 | Add worker4 at runtime on a port not in the config | The master adds it to its live workers | Pass |
| 5 | Stop a worker with Ctrl+C | The master records it as LEFT, never as FAILED, and is notified to requeue its tasks | Pass |
| 6 | Kill a worker (`kill -9`) | The master marks it DEAD within the timeout, from the closed channel | Pass |
| 7 | Freeze a worker so it sends nothing but keeps its socket open | The master marks it DEAD only after the heartbeat timeout | Pass |
| 8 | Remove one worker | The other workers stay alive and are not affected | Pass |
| 9 | Crash a worker, then restart it | It rejoins with a higher incarnation and is ALIVE again | Pass |
| 10 | Crash the master, then restart it | Workers notice, and all rejoin the new master on their own | Pass |
| 11 | Start 4 peers | Every peer finds the other 3 through the seeds | Pass |
| 12 | Start peers and the cluster together | Peers do not track the master or workers | Pass |
| 13 | Add peer5 at runtime | All 4 existing peers learn about it | Pass |
| 14 | Stop a peer with Ctrl+C | All other peers record it as LEFT | Pass |
| 15 | Crash seed peer1 | The others mark it DEAD and keep working with each other | Pass |

**Peer-to-peer** (`tests/test_peer.py`, Sri Harsha)

| # | Test case | Expected result | Result |
|---|---|---|---|
| 16 | Four peers discover each other; gossip spreads a late peer | Every peer knows every other | Pass |
| 17 | Peers announce their resources; a peer departs | Every registry fills; the departed peer's entries are dropped | Pass |
| 18 | Query a resource; query a missing one; send duplicate queries | Holder found and fetched directly; missing returns nothing; duplicates answered once | Pass |
| 19 | Fetch a resource, then its original holder leaves | The fetched copy is still available | Pass |
| 20 | Share a resource at runtime | Other peers can find it | Pass |

**Cluster worker** (`tests/test_worker.py`, against a real master endpoint)

| # | Test case | Expected result | Result |
|---|---|---|---|
| 21 | Assign `sum`, `word_count` and `count_primes` tasks | Correct result, status SUCCESS, with task, job and worker ids | Pass |
| 22 | Assign an unknown task kind, or a task with bad params | Status FAILED with the error; the worker keeps taking tasks | Pass |
| 23 | Assign a long task, then a short one to the same worker | The short one returns first: tasks do not block each other | Pass |
| 24 | Keep a worker busy with a long task | The master never marks it failed: heartbeats continue | Pass |
| 25 | Assign tasks to two workers | Each task is done by the worker it was sent to | Pass |
| 26 | Crash a worker during a task | The master marks it DEAD and is notified to requeue; no result arrives | Pass |

**Cluster master** (Kashif)

| # | Test case | Expected result | Result |
|---|---|---|---|
| 27 | `[PENDING: master test cases: job split, dispatch, result collection, requeue, parallel speedup]` | | |

**Demo scenarios with measured timings** (`demo/join_leave_demo.py`)

| # | Scenario | Expected result | Measured |
|---|---|---|---|
| D1 | Start 4 peers as separate processes | Every peer knows all 4 | 56 ms, including process start-up |
| D2 | peer5 joins at runtime | All 4 peers learn it | 15 ms |
| D3 | peer5 fetches dataset-b from peer2 | 137,744 bytes fetched directly | 1 ms |
| D4 | `kill -9` seed peer2 | All remaining peers mark it FAILED | under 1 ms |
| D5 | peer6 joins after the crash and asks for dataset-b | Served by peer5's replica | 3 ms to join, 2 ms to fetch |
| D6 | Ctrl+C on peer4 | All remaining peers record LEFT | under 1 ms |
| D7 | Search for dataset-d, whose only holder left | Not found | Not found, as expected |
| D8 | worker4 joins the master at runtime | Added to live workers | 44 ms, including process start-up |
| D9 | `kill -9` worker2; `SIGSTOP` worker1; then `SIGCONT` | Crash detected at once; hang detected by timeout; resumed worker is live again | under 1 ms; 3.65 s; under 1 ms |
| D10 | Ctrl+C on worker3 | The master records LEFT | under 1 ms |

## Node Addition and Removal Analysis

Every node runs the same membership service (`joinleave/service.py`). It keeps two separate memberships, one for each architecture:

| Role | Joins through | Sends heartbeats to |
|---|---|---|
| Peer | The seed peers (`peer1`, `peer2`) | Every other live peer |
| Worker | The master | The master |
| Master | Nobody | Every live worker |

**How a node joins.** A new node sends `JOIN`. The receiver adds it and replies with `JOIN_ACK`, which carries its member list. A new peer then sends `JOIN` to each peer on that list, and gossip spreads it to any peer it missed, so no central registry is needed. A new worker only needs the master. Every node gets a new incarnation number when it starts, so a restarted node always overrides old reports that it was dead.

**How a node leaves.** The system tells three cases apart. The times below were measured with the default settings (1 s heartbeat, 3.5 s failure timeout):

| How the node left | Example | How others detect it | Measured detection time | Recorded as |
|---|---|---|---|---|
| Graceful | Ctrl+C | It sends `LEAVE` before exiting | under 1 ms | LEFT |
| Crash | `kill -9` | Its TCP channel closes | under 1 ms | DEAD |
| Hang | Process frozen, socket open | No heartbeat for 3.5 s | 3.65 s | DEAD |

The hang case is the only one that needs the timeout, and it is the one that matters for failures a closed socket would not reveal, such as a frozen process or a network cut. The extra 0.15 s comes from the failure check running once per heartbeat interval.

**Effect of adding a node.**
- *P2P:* peer5 was known to all 4 peers within 15 ms, and it could fetch resources at once. Joining needs at least one seed to be up, but it does not have to be a particular seed: peer6 joined after seed peer2 had crashed.
- *Cluster:* worker4 was in the master's live set within 44 ms of starting. In the agreed design the master dispatches tasks to its live set (`ctx.members.alive("worker")`), so a new worker gets work without being configured anywhere.

**Effect of removing a node.**
- *P2P, data:* a resource survives a removal only if another peer holds a copy. dataset-b survived the crash of peer2 because peer5 had fetched it, so peer6 got it from peer5 2 ms later. dataset-d was lost when peer4 left, because no other peer had fetched it. Availability depends on replication, not on any central server.
- *P2P, structure:* the remaining peers carry on with no reconfiguration. Losing a seed only affects nodes that join later, and only if every seed is down.
- *Cluster, a worker:* capacity drops by one worker. The master is notified through `on_worker_lost`, so the worker's unfinished tasks can be sent to the remaining workers. `[PENDING: requeue measurement once Kashif's master runs jobs, e.g. job completion time with and without a worker killed mid-job]`
- *Cluster, the master:* the cluster cannot accept or run jobs until the master is back. It is a single point of failure, which the P2P side does not have. When it restarts, the workers detect the loss and rejoin it on their own (test 10).

**False suspicion.** A frozen worker (`SIGSTOP`) was declared dead after 3.65 s, but it was alive. When it resumed (`SIGCONT`), its next message put it back in the live set in under 1 ms. A shorter timeout detects failures faster but declares slow nodes dead more often. A worker wrongly declared dead may still return results for tasks the master has already reassigned, so the master must ignore duplicate `TASK_RESULT`s.

**Cost.** Membership traffic measured over 20 s with all 8 nodes running: 4 peers sent 12 heartbeats/s plus 2 gossip messages/s, and the master and 3 workers sent 6 heartbeats/s together.

## Peer-to-Peer vs Cluster Computing Comparison

Both halves were built in this system and run on the same transport, so the comparison uses measurements from it.

| Aspect | Peer-to-peer (peers) | Cluster (master and workers) |
|---|---|---|
| **Communication** | Any peer talks to any other directly: query flooding, direct fetch, gossip. No coordinator. | Star: every message goes between the master and a worker. Workers never talk to each other. |
| **Membership traffic** | Every peer heartbeats every other: P×(P−1) messages/s, plus gossip. Measured: 12 + 2 = 14/s for 4 peers. | Workers and master exchange heartbeats: 2×W messages/s. Measured: 6/s for 3 workers. |
| **Resource sharing** | Each peer holds and serves its own resources. Found by flooding a query with a TTL; fetched in 1–2 ms. Copies made by fetching keep data available after a peer leaves. | Resources are compute capacity, handed out by the master. Workers hold no shared data; the master decides what each one runs. |
| **Scalability** | No bottleneck node, but full-mesh heartbeats grow with P²: 10 peers would need about 95 messages/s. Larger overlays need partial views or gossip-only failure detection. | Grows linearly: 10 workers need 20 messages/s. The master becomes the limit, since every task and result passes through it. |
| **Processing** | Each peer only processes its own requests; there is no way to split one large job across peers. | Designed for heavy work: the master splits a job into tasks that run in parallel. `[PENDING: speedup with 1, 2 and 3 workers from Kashif's parallel proof]` |
| **Fault tolerance** | No single point of failure. A crash was detected by every peer in under 1 ms, and the rest kept working. | A worker failure is absorbed by requeueing its tasks. The master is a single point of failure. |
| **Joining** | A new peer needs any one seed; known by all peers in 15 ms. | A new worker needs only the master; usable in 44 ms. |

**Summary.** P2P is better for sharing and finding data with no central point that can fail, at the cost of more coordination traffic as it grows. The cluster is better for splitting heavy computation and has simpler, cheaper coordination, at the cost of depending on the master. This system uses each for what it does best: peers share and find data among themselves, and a peer passes heavy processing to the cluster through its bridge.

## Additional Features Added Other Than Mentioned in the Problem Statement

`[PLACEHOLDER]`

## Screenshots and Logs

`[PLACEHOLDER: evidence from outputs/]`

## References

`[PLACEHOLDER]`
