"""Each node entry point must start as its own process."""

import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NODES = [
    ("peer.peer_node", "peer1"),
    ("cluster.master", "master"),
    ("cluster.worker", "worker1"),
]


class NodeStartupTests(unittest.TestCase):
    def start(self, module, node_id, log_dir):
        env = dict(os.environ, DC_LOG_DIR=log_dir)
        process = subprocess.Popen(
            [sys.executable, "-u", "-m", module, "--id", node_id, "--port", "0"],
            cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.addCleanup(process.stdout.close)
        self.addCleanup(process.wait)
        self.addCleanup(process.kill)
        return process

    def test_nodes_start_and_write_their_logs(self):
        with tempfile.TemporaryDirectory() as log_dir:
            for module, node_id in NODES:
                with self.subTest(module=module):
                    process = self.start(module, node_id, log_dir)
                    line = process.stdout.readline()
                    self.assertIn("listening on", line)
                    process.terminate()
                    process.wait(timeout=5)
                    self.assertTrue(os.path.exists(os.path.join(log_dir, node_id + ".log")))
                    if os.name != "nt":     # terminate() is a hard kill on Windows.
                        self.assertEqual(process.returncode, 0)


if __name__ == "__main__":
    unittest.main()
