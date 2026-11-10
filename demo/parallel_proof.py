"""Parallel execution proof for the cluster, with timings. Owner: Kashif.

    python3 -m demo.parallel_proof
    python3 -m demo.parallel_proof --end 3000000 --tasks 12

Runs the full request flow, peer -> master -> workers -> master -> peer, with
real OS processes from config/nodes.json:

  1. Start the master and worker1, and send a count_primes job from peer1
     through its bridge (peer/bridge.py). Time it.
  2. Start worker2 at runtime and send the same job. Time it.
  3. Start worker3 at runtime and send the same job. Time it.
  4. Send the job again and kill -9 one worker part way through. The master
     requeues that worker's tasks and the job still finishes, with the same
     result.

The same job (same range, same number of tasks) is used every time, so the
times are directly comparable: speedup = time with 1 worker / time with N.
The master logs a PARALLEL PROOF timeline for every job, showing which
worker ran each task and when; overlapping times are the proof that tasks
ran in parallel.

Logs and a summary are saved in outputs/logs/ as parallel-proof-*.log and
parallel-proof-summary.txt. Needs the configured ports free.
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
JOB_TIMEOUT_S = 300
PROOF = re.compile(r"PARALLEL PROOF job (\S+): (\d+) task\(s\) on (\d+) worker\(s\) (\{.*\}), "
                   r"max (\d+) running at once, task time ([\d.]+)s in wall time ([\d.]+)s "
                   r"= ([\d.]+)x parallelism")


class Proof:
    def __init__(self, end, tasks):
        self.end, self.tasks = end, tasks
        self.log_dir = tempfile.mkdtemp(prefix="parallel-proof-")
        os.environ["DC_LOG_DIR"] = self.log_dir
        self.procs = {}
        self.lines = []
        self.runs = []          # (label, workers, job_id, status, result, cluster_s, round_trip_s)
        self.bridge = None
        self.client = None

    def say(self, text=""):
        print(text, flush=True)
        self.lines.append(text)

    # -- nodes ------------------------------------------------------------------

    def spawn(self, module, node_id):
        cmd = [sys.executable, "-u", "-m", module, "--id", node_id]
        self.procs[node_id] = subprocess.Popen(
            cmd, cwd=ROOT, env=dict(os.environ, DC_LOG_DIR=self.log_dir),
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.say("   started {} as process {}".format(node_id, self.procs[node_id].pid))

    def master_log(self):
        try:
            with open(os.path.join(self.log_dir, "master.log"), encoding="utf-8") as handle:
                return handle.read()
        except FileNotFoundError:
            return ""

    def wait_for(self, text, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if text in self.master_log():
                return True
            time.sleep(0.05)
        raise SystemExit("timed out waiting for the master to log: " + text)

    def add_worker(self, node_id):
        self.spawn("cluster.worker", node_id)
        self.wait_for("MEMBER JOINED {} ".format(node_id))
        self.say("   master sees {} as alive".format(node_id))

    def start_client(self):
        """peer1's bridge, run in this script so submit() can be called directly."""
        from common import build_node
        from peer.bridge import Bridge
        ctx = build_node(SimpleNamespace(id="peer1", port=None, host=None, config=None,
                                         verbose=False), "peer")
        for handler in list(ctx.log.handlers):      # file log only, keep the output readable
            if not hasattr(handler, "baseFilename"):
                ctx.log.removeHandler(handler)
        self.bridge = Bridge(ctx)
        ctx.ep.start()
        self.client = ctx

    def stop_all(self):
        if self.client is not None:
            self.client.ep.stop()
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
        for proc in self.procs.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    # -- jobs -------------------------------------------------------------------

    def run_job(self, label, workers, during=None):
        params = {"start": 0, "end": self.end, "tasks": self.tasks}
        job_id = self.bridge.submit("count_primes", params)
        job = self.bridge.jobs[job_id]
        if during is not None:
            during(job_id)
        if not job["done"].wait(JOB_TIMEOUT_S):
            raise SystemExit("job {} did not finish in {}s".format(job_id, JOB_TIMEOUT_S))
        round_trip = job["finished_at"] - job["submitted_at"]
        self.runs.append((label, workers, job_id, job["status"], job["result"],
                          job["elapsed_s"], round_trip))
        self.say("   job {}: {} result={} cluster time {:.3f}s (round trip at peer1 {:.3f}s)".format(
            job_id, job["status"], job["result"], job["elapsed_s"], round_trip))
        self.wait_for("PARALLEL PROOF job {}:".format(job_id) if job["status"] == "DONE"
                      else "JOB DONE {}".format(job_id))
        return job_id

    def kill_mid_job(self, victim):
        def during(job_id):
            self.wait_for("DISPATCH {}-t".format(job_id))
            time.sleep(max(0.3, self.base / 3 / 3))   # about a third of the 3-worker time
            self.say("   kill -9 {} (pid {}) while job {} runs".format(
                victim, self.procs[victim].pid, job_id))
            os.kill(self.procs[victim].pid, signal.SIGKILL)
        return during

    # -- run --------------------------------------------------------------------

    def run(self):
        self.say("Parallel execution proof: count_primes over [0, {:,}) split into {} tasks".format(
            self.end, self.tasks))
        self.say("Host: {} CPU core(s), Python {}".format(os.cpu_count(), sys.version.split()[0]))

        self.say()
        self.say("== Run 1: master + 1 worker")
        self.spawn("cluster.master", "master")
        self.wait_for("master ready")
        self.add_worker("worker1")
        self.start_client()
        self.run_job("1 worker", 1)
        self.base = self.runs[-1][5]

        self.say()
        self.say("== Run 2: worker2 joins at runtime -> 2 workers")
        self.add_worker("worker2")
        self.run_job("2 workers", 2)

        self.say()
        self.say("== Run 3: worker3 joins at runtime -> 3 workers")
        self.add_worker("worker3")
        self.run_job("3 workers", 3)

        self.say()
        self.say("== Run 4: 3 workers, worker3 killed mid-job (kill -9)")
        job_id = self.run_job("3 workers, 1 killed", 3, during=self.kill_mid_job("worker3"))
        requeues = [l for l in self.master_log().splitlines()
                    if "REQUEUE" in l and job_id in l]
        for line in requeues:
            self.say("   master: " + line.split("WARNING", 1)[-1].strip())

        self.summarise()

    def summarise(self):
        proofs = {m.group(1): m for m in PROOF.finditer(self.master_log())}
        base = self.runs[0][5]
        results = {r[4] for r in self.runs}

        self.say()
        self.say("== Results")
        self.say("| Run | Workers | Cluster time | Speedup | Max tasks at once | Task time / wall time | Tasks per worker |")
        self.say("|---|---|---|---|---|---|---|")
        for label, workers, job_id, status, result, cluster_s, _rt in self.runs:
            p = proofs.get(job_id)
            self.say("| {} | {} | {:.3f} s | {:.2f}x | {} | {}x | {} |".format(
                label, workers, cluster_s, base / cluster_s if cluster_s else 0,
                p.group(5) if p else "-", p.group(8) if p else "-", p.group(4) if p else "-"))
        self.say()
        self.say("All runs returned the same result: {} ({})".format(
            "yes" if len(results) == 1 else "NO", ", ".join(str(r) for r in sorted(results, key=str))))
        killed, normal = self.runs[3][5], self.runs[2][5]
        self.say("Worker killed mid-job: job still {} in {:.3f}s vs {:.3f}s with no failure "
                 "(+{:.3f}s for detection and requeue)".format(
                     self.runs[3][3], killed, normal, killed - normal))

        for job_id in (r[2] for r in self.runs):
            self.say()
            self.say("-- master timeline for {}".format(job_id))
            for line in self.master_log().splitlines():
                if "PARALLEL PROOF" in line and job_id in line:   # task ids contain the job id
                    self.say("   " + line.split("INFO", 1)[-1].strip())

    def save(self):
        os.makedirs(OUT_DIR, exist_ok=True)
        for name in os.listdir(self.log_dir):
            if name.endswith(".log"):
                shutil.copy(os.path.join(self.log_dir, name),
                            os.path.join(OUT_DIR, "parallel-proof-" + name))
        path = os.path.join(OUT_DIR, "parallel-proof-summary.txt")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("# Generated by demo/parallel_proof.py on {}\n\n".format(
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            handle.write("\n".join(self.lines) + "\n")
        print("\nSaved {} and parallel-proof-*.log in {}".format(os.path.basename(path), OUT_DIR))


def main():
    parser = argparse.ArgumentParser(description="Cluster parallel execution proof")
    parser.add_argument("--end", type=int, default=2_000_000, help="count primes in [0, end)")
    parser.add_argument("--tasks", type=int, default=12, help="tasks to split the job into")
    args = parser.parse_args()

    proof = Proof(args.end, args.tasks)
    try:
        proof.run()
    finally:
        proof.stop_all()
    proof.save()


if __name__ == "__main__":
    main()
