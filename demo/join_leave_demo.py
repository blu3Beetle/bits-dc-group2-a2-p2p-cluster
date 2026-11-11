"""Node addition and removal demo, with timings. Owner: Sohail.

    python3 -m demo.join_leave_demo p2p        # peers: join, crash, leave
    python3 -m demo.join_leave_demo cluster    # master and workers: join, crash, hang, leave
    python3 -m demo.join_leave_demo all

Each scenario starts the nodes from config/nodes.json as real OS processes,
adds and removes nodes, and measures from the nodes' own logs how long the
rest of the system took to notice. Logs and a summary are saved in
outputs/logs/ as joinleave-<scenario>-*.log and joinleave-<scenario>-summary.txt.

Removals use real signals, the same as in a terminal:
    crash  kill -9 (SIGKILL)  the TCP channel closes with no LEAVE
    hang   SIGSTOP            the process freezes with its socket open
    leave  Ctrl+C (SIGINT)    the node sends LEAVE, then exits

The P2P scenario also needs to ask a peer for a resource. A peer's console
only runs in a terminal, so the late joiners (peer5, peer6) run inside this
script, where their get() can be called directly. They are ordinary
PeerNodes on their own ports, talking to the others over TCP.

Needs Linux or macOS (SIGSTOP), and the configured ports free.
"""

import argparse
import datetime
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "outputs", "logs")
WAIT_S = 15
LOG_TIME = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3})")


class Demo:
    def __init__(self, scenario):
        self.scenario = scenario
        self.log_dir = tempfile.mkdtemp(prefix="joinleave-")
        os.environ["DC_LOG_DIR"] = self.log_dir     # for nodes built in this process
        self.procs = {}
        self.local = {}
        self.results = []                           # (step, measured, note)
        self.lines = []

    # -- output -----------------------------------------------------------

    def say(self, text=""):
        print(text, flush=True)
        self.lines.append(text)

    def step(self, title):
        self.say()
        self.say("== " + title)

    def record(self, step, seconds, note="", missing="not seen"):
        if seconds is None:
            measured = missing
        elif seconds < 0.001:
            measured = "<1 ms"          # logs are stamped to the millisecond
        elif seconds < 1:
            measured = "{:.0f} ms".format(seconds * 1000)
        else:
            measured = "{:.2f} s".format(seconds)
        self.results.append((step, measured, note))
        self.say("   -> {}: {}{}".format(step, measured, "  ({})".format(note) if note else ""))

    # -- nodes ------------------------------------------------------------

    def spawn(self, module, node_id, *extra):
        cmd = [sys.executable, "-u", "-m", module, "--id", node_id] + list(extra)
        self.procs[node_id] = subprocess.Popen(
            cmd, cwd=ROOT, env=dict(os.environ, DC_LOG_DIR=self.log_dir),
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.say("   started {} as process {}".format(node_id, self.procs[node_id].pid))

    def start_local_peer(self, node_id, port):
        from common import build_node
        from peer.peer_node import PeerNode, demo_resources
        ctx = build_node(SimpleNamespace(id=node_id, port=port, host=None, config=None,
                                         verbose=False), "peer")
        for handler in list(ctx.log.handlers):      # file log only, keep the demo output readable
            if not hasattr(handler, "baseFilename"):
                ctx.log.removeHandler(handler)
        node = PeerNode(ctx, demo_resources(node_id))
        ctx.ep.start()
        node.membership.start()
        node.start()
        self.local[node_id] = node
        self.say("   started {} on port {} (in this script)".format(node_id, port))
        return node

    def signal(self, node_id, sig, label):
        self.say("   {} {} (pid {})".format(label, node_id, self.procs[node_id].pid))
        os.kill(self.procs[node_id].pid, sig)
        return time.time()

    def stop_all(self):
        for node in self.local.values():
            node.stop()
            node.ctx.ep.stop()
        for proc in self.procs.values():
            if proc.poll() is None:
                try:
                    os.kill(proc.pid, signal.SIGCONT)
                except OSError:
                    pass
                proc.send_signal(signal.SIGINT)
        for proc in self.procs.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    # -- reading the nodes' logs ------------------------------------------

    def log_lines(self, node_id):
        path = os.path.join(self.log_dir, node_id + ".log")
        try:
            with open(path, encoding="utf-8") as handle:
                return handle.read().splitlines()
        except OSError:
            return []

    def first_match(self, node_id, pattern, after):
        """Wall-clock time of the first log line matching pattern at or after `after`."""
        regex = re.compile(pattern)
        for line in self.log_lines(node_id):
            stamp = LOG_TIME.match(line)
            if not stamp or not regex.search(line):
                continue
            when = datetime.datetime.strptime(stamp.group(1), "%Y-%m-%d %H:%M:%S.%f").timestamp()
            if when >= after - 0.001:
                return when
        return None

    def wait_all(self, observers, pattern, after, timeout=WAIT_S):
        """Wait until every observer logs pattern. Return the latest time, or None."""
        deadline = time.time() + timeout
        while True:
            times = [self.first_match(n, pattern, after) for n in observers]
            if all(t is not None for t in times):
                return max(times)
            if time.time() > deadline:
                missing = [n for n, t in zip(observers, times) if t is None]
                self.say("   !! not seen by {}".format(missing))
                return None
            time.sleep(0.05)

    def measure(self, step, observers, pattern, after, note=""):
        seen = self.wait_all(observers, pattern, after)
        self.record(step, None if seen is None else seen - after, note)

    # -- saving -----------------------------------------------------------

    def save(self):
        os.makedirs(OUT_DIR, exist_ok=True)
        prefix = "joinleave-{}-".format(self.scenario)
        for name in sorted(os.listdir(self.log_dir)):
            if name.endswith(".log"):
                shutil.copy(os.path.join(self.log_dir, name), os.path.join(OUT_DIR, prefix + name))
        summary = os.path.join(OUT_DIR, prefix + "summary.txt")
        with open(summary, "w", encoding="utf-8") as handle:
            handle.write("Join/leave demo: {} scenario, run {}\n".format(
                self.scenario, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            handle.write("\n".join(self.lines) + "\n\nMeasured\n")
            for step, measured, note in self.results:
                handle.write("  {:<58} {:>9}  {}\n".format(step, measured, note))
        self.say()
        self.say("Saved logs and summary to outputs/logs/{}*".format(prefix))


# -- scenarios ------------------------------------------------------------------

def p2p(demo):
    peers = ["peer1", "peer2", "peer3", "peer4"]

    demo.step("1. Start 4 peers. They find each other through the seeds, with no central list")
    started = time.time()
    for p in peers:
        demo.spawn("peer.peer_node", p, "--no-console")
    demo.measure("4-peer overlay formed (every peer knows all 4)", peers,
                 r"Alive peers: \[('peer\d', ){3}'peer\d'\]", started,
                 "includes process start-up")

    demo.step("2. ADD: peer5 joins at runtime on port 6005 (not in the config)")
    t = time.time()
    peer5 = demo.start_local_peer("peer5", 6005)
    demo.measure("peer5 known by all 4 existing peers", peers, r"MEMBER JOINED peer5", t)
    demo.say("   peer5 sees: {}".format(peer5.live_peers()))

    demo.step("3. peer5 fetches dataset-b (held only by peer2), so it becomes a second holder")
    t = time.time()
    content = peer5.get("dataset-b")
    demo.record("peer5 fetched dataset-b from peer2", time.time() - t,
                "{} bytes".format(len(content) if content else 0))

    demo.step("4. REMOVE by crash: kill -9 peer2 (a seed peer, and the original holder of dataset-b)")
    t = demo.signal("peer2", signal.SIGKILL, "kill -9")
    demo.measure("peer2 crash detected by all remaining peers",
                 ["peer1", "peer3", "peer4", "peer5"], r"MEMBER FAILED peer2", t,
                 "TCP channel closed, no timeout needed")

    demo.step("5. ADD: peer6 joins after the crash and asks for dataset-b")
    t = time.time()
    peer6 = demo.start_local_peer("peer6", 6006)
    demo.measure("peer6 known by all live peers", ["peer1", "peer3", "peer4"],
                 r"MEMBER JOINED peer6", t)
    t = time.time()
    content = peer6.get("dataset-b")
    holder = peer6.origin_of.get("dataset-b")
    demo.record("peer6 fetched dataset-b after its original holder crashed",
                time.time() - t if content else None,
                "served by {} (the replica)".format(holder) if content else "lost")

    demo.step("6. REMOVE gracefully: Ctrl+C on peer4 (sole holder of dataset-d)")
    t = demo.signal("peer4", signal.SIGINT, "Ctrl+C")
    demo.measure("peer4 LEAVE seen by all remaining peers",
                 ["peer1", "peer3", "peer5", "peer6"], r"MEMBER LEFT peer4", t,
                 "LEAVE message, recorded as LEFT not FAILED")

    demo.step("7. Effect of removal: dataset-d had no replica")
    t = time.time()
    content = peer6.get("dataset-d", timeout=3)
    demo.record("peer6 searched for dataset-d", None if content is None else time.time() - t,
                "its only holder left, no replica existed" if content is None else "found",
                missing="not found")

    demo.step("8. Final state")
    demo.say("   peer6 sees live peers: {}".format(peer6.live_peers()))
    for name, holders in peer6.registry().items():
        demo.say("   registry  {:<14} {}".format(name, ", ".join(holders)))
    stats = peer6.ctx.ep.stats()
    demo.say("   peer6 messages sent: {}".format(stats["sent"]))


def cluster(demo):
    workers = ["worker1", "worker2", "worker3"]

    demo.step("1. Start the master and 3 workers")
    started = time.time()
    demo.spawn("cluster.master", "master")
    for w in workers:
        demo.spawn("cluster.worker", w)
    for w in workers:
        demo.wait_all(["master"], r"MEMBER JOINED {} ".format(w), started)
    demo.measure("3 workers joined the master", ["master"],
                 r"Alive workers: \['worker1', 'worker2', 'worker3'\]", started)

    demo.step("2. ADD: worker4 joins at runtime on port 6204 (not in the config)")
    t = time.time()
    demo.spawn("cluster.worker", "worker4", "--port", "6204")
    demo.measure("worker4 known by the master", ["master"], r"MEMBER JOINED worker4", t,
                 "includes process start-up")

    demo.step("3. REMOVE by crash, mid-job: peer1 submits a job, then kill -9 worker2 while it runs a task")
    peer1 = demo.start_local_peer("peer1", None)
    submitted = time.time()
    job_id = peer1.bridge.submit("count_primes", {"start": 0, "end": 2_000_000, "tasks": 12})
    demo.say("   peer1 submitted job {}: count_primes over [0, 2,000,000) in 12 tasks".format(job_id))
    demo.wait_all(["master"], r"DISPATCH \S+ -> worker2 ", submitted)
    time.sleep(0.2)                                   # let worker2 get into its task
    t = demo.signal("worker2", signal.SIGKILL, "kill -9")
    demo.measure("worker2 crash detected by the master", ["master"], r"MEMBER FAILED worker2", t,
                 "TCP channel closed")
    demo.measure("worker2's unfinished task requeued", ["master"], r"REQUEUE \d+ task\(s\) of lost worker worker2", t)
    job = peer1.bridge.jobs[job_id]
    finished = job["done"].wait(60)
    demo.record("job finished despite the crash", time.time() - submitted if finished else None,
                "status {}, result {}".format(job["status"], job["result"]) if finished else "no result",
                missing="did not finish")

    demo.step("4. REMOVE by hang: SIGSTOP worker1 (frozen, socket still open)")
    t = demo.signal("worker1", signal.SIGSTOP, "SIGSTOP")
    demo.measure("worker1 hang detected by the master", ["master"], r"MEMBER FAILED worker1", t,
                 "heartbeat timeout, failure_timeout_s = 3.5")

    demo.step("5. worker1 resumes (SIGCONT): it was not really dead")
    t = demo.signal("worker1", signal.SIGCONT, "SIGCONT")
    demo.measure("worker1 back in the master's live set", ["master"],
                 r"MEMBER JOINED worker1", t, "its next message revives it")

    demo.step("6. REMOVE gracefully: Ctrl+C on worker3")
    t = demo.signal("worker3", signal.SIGINT, "Ctrl+C")
    demo.measure("worker3 LEAVE seen by the master", ["master"], r"MEMBER LEFT worker3", t,
                 "LEAVE message, recorded as LEFT not FAILED")

    demo.step("7. Final state")
    time.sleep(0.5)
    alive = [l for l in demo.log_lines("master") if "Alive workers" in l]
    demo.say("   master: {}".format(alive[-1].split("Alive workers: ")[-1] if alive else "?"))
    for line in demo.log_lines("master"):
        if "REQUEUE" in line or "nothing to requeue" in line:
            demo.say("   master: " + line.split("] ", 1)[-1].split(None, 1)[-1])


SCENARIOS = {"p2p": p2p, "cluster": cluster}


def main():
    parser = argparse.ArgumentParser(description="Node addition and removal demo")
    parser.add_argument("scenario", choices=sorted(SCENARIOS) + ["all"])
    args = parser.parse_args()
    sys.path.insert(0, ROOT)
    for name in (sorted(SCENARIOS) if args.scenario == "all" else [args.scenario]):
        demo = Demo(name)
        demo.say("#### Join/leave demo: {} ####".format(name))
        try:
            SCENARIOS[name](demo)
        finally:
            demo.stop_all()
        demo.save()


if __name__ == "__main__":
    main()
