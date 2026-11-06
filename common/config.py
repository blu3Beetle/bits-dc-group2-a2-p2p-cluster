"""Loads config/nodes.json, the only place hosts and ports are defined.

The file lists the nodes present at startup. A node that joins later is not
listed there: start it with --id and --port, and it is announced at runtime.
"""

import json
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG = os.path.join(REPO_ROOT, "config", "nodes.json")
DEFAULT_HOST = "127.0.0.1"
ROLES = ("peer", "master", "worker")


class ConfigError(RuntimeError):
    """The topology file is missing, malformed or inconsistent."""


class Config:
    def __init__(self, data):
        try:
            self.nodes = data["nodes"]
            self.seeds = list(data["seeds"])
            self.timing = dict(data["timing"])
        except (KeyError, TypeError) as exc:
            raise ConfigError("config is missing section {}".format(exc))
        self._check()

    def _check(self):
        seen = {}
        for node_id, node in self.nodes.items():
            for field in ("role", "host", "port"):
                if field not in node:
                    raise ConfigError("{} is missing {!r}".format(node_id, field))
            if node["role"] not in ROLES:
                raise ConfigError("{} has unknown role {!r}".format(node_id, node["role"]))
            address = (node["host"], node["port"])
            if address in seen:
                raise ConfigError("{} and {} share address {}:{}".format(
                    seen[address], node_id, *address))
            seen[address] = node_id

        for seed in self.seeds:
            if self.nodes.get(seed, {}).get("role") != "peer":
                raise ConfigError("seed {!r} is not a configured peer".format(seed))
        masters = self.ids("master")
        if len(masters) != 1:
            raise ConfigError("expected exactly one master, found {}".format(masters))

    def ids(self, role=None):
        """Configured node ids, optionally for one role."""
        return [n for n, node in self.nodes.items() if role is None or node["role"] == role]

    @property
    def master_id(self):
        return self.ids("master")[0]

    def address(self, node_id):
        """(host, port) of a configured node."""
        if node_id not in self.nodes:
            raise ConfigError("{!r} is not in the config; pass --port for a new node".format(node_id))
        node = self.nodes[node_id]
        return node["host"], node["port"]

    def listen_address(self, node_id, host=None, port=None):
        """(host, port) this node should use. Explicit values override the config."""
        if node_id in self.nodes:
            configured_host, configured_port = self.address(node_id)
            return host or configured_host, configured_port if port is None else port
        if port is None:
            raise ConfigError("{!r} is not in the config; pass --port for a new node".format(node_id))
        return host or DEFAULT_HOST, port

    def bootstrap_ids(self):
        """Nodes anyone may contact at startup: the seeds and the master.

        Peers learn every other peer from each other, not from this file.
        """
        return self.seeds + [self.master_id]


def load_config(path=None):
    """Read the topology from path, $DC_CONFIG, or config/nodes.json."""
    path = path or os.environ.get("DC_CONFIG") or DEFAULT_CONFIG
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return Config(json.load(handle))
    except (OSError, ValueError) as exc:
        raise ConfigError("cannot read {}: {}".format(path, exc))
