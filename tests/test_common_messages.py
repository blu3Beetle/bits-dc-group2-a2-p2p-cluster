import json
import os
import tempfile
import unittest

from common import MsgType, SCHEMA, SchemaError, make_message, validate
from common.config import REPO_ROOT, Config, ConfigError, load_config


class MessageTests(unittest.TestCase):
    def test_valid_message_round_trips_through_json(self):
        msg = make_message(MsgType.RESOURCE_FETCH, "peer1", name="a")
        self.assertEqual(validate(json.loads(json.dumps(msg))), msg)

    def test_every_type_can_be_built(self):
        for msg_type, fields in SCHEMA.items():
            make_message(msg_type, "peer1", **{field: 1 for field in fields})

    def test_missing_field_rejected(self):
        with self.assertRaisesRegex(SchemaError, "missing"):
            make_message(MsgType.RESOURCE_QUERY, "peer1", name="a")

    def test_extra_field_rejected(self):
        with self.assertRaisesRegex(SchemaError, "unexpected"):
            make_message(MsgType.RESOURCE_FETCH, "peer1", name="a", extra=1)

    def test_unknown_type_rejected(self):
        with self.assertRaisesRegex(SchemaError, "unknown message type"):
            make_message("NOPE", "peer1")

    def test_bad_envelope_rejected(self):
        msg = make_message(MsgType.RESOURCE_FETCH, "peer1", name="a")
        del msg["ts"]
        with self.assertRaisesRegex(SchemaError, "envelope"):
            validate(msg)

    def test_unhashable_type_rejected(self):
        msg = make_message(MsgType.RESOURCE_FETCH, "peer1", name="a")
        msg["type"] = ["JOIN"]
        with self.assertRaises(SchemaError):
            validate(msg)

    def test_non_dict_rejected(self):
        with self.assertRaises(SchemaError):
            validate(["JOIN"])


def topology(**overrides):
    data = {
        "nodes": {
            "peer1": {"role": "peer", "host": "127.0.0.1", "port": 6001},
            "master": {"role": "master", "host": "127.0.0.1", "port": 6100},
        },
        "seeds": ["peer1"],
        "timing": {"heartbeat_interval_s": 1.0, "failure_timeout_s": 3.5},
    }
    data.update(overrides)
    return data


class ConfigTests(unittest.TestCase):
    def test_repo_config_loads_with_required_nodes(self):
        cfg = load_config()
        self.assertGreaterEqual(len(cfg.ids("peer")), 4)
        self.assertGreaterEqual(len(cfg.ids("worker")), 2)
        self.assertEqual(cfg.master_id, "master")

    def test_multinode_config_has_the_same_nodes(self):
        multinode = load_config(os.path.join(REPO_ROOT, "config", "nodes.multinode.json"))
        self.assertEqual(set(multinode.ids()), set(load_config().ids()))
        self.assertEqual(multinode.timing, load_config().timing)

    def test_lookup(self):
        cfg = Config(topology())
        self.assertEqual(cfg.address("peer1"), ("127.0.0.1", 6001))
        self.assertEqual(cfg.bootstrap_ids(), ["peer1", "master"])

    def test_new_node_needs_a_port(self):
        cfg = Config(topology())
        with self.assertRaises(ConfigError):
            cfg.listen_address("peer9")
        self.assertEqual(cfg.listen_address("peer9", port=6009), ("127.0.0.1", 6009))

    def test_port_override_for_known_node(self):
        self.assertEqual(Config(topology()).listen_address("peer1", port=0), ("127.0.0.1", 0))

    def test_duplicate_address_rejected(self):
        nodes = topology()["nodes"]
        nodes["peer2"] = dict(nodes["peer1"])
        with self.assertRaisesRegex(ConfigError, "share address"):
            Config(topology(nodes=nodes))

    def test_seed_must_be_a_peer(self):
        with self.assertRaisesRegex(ConfigError, "seed"):
            Config(topology(seeds=["master"]))

    def test_exactly_one_master(self):
        nodes = topology()["nodes"]
        del nodes["master"]
        with self.assertRaisesRegex(ConfigError, "one master"):
            Config(topology(nodes=nodes))

    def test_malformed_file_reports_config_error(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "bad.json")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("{not json")
            with self.assertRaises(ConfigError):
                load_config(path)


if __name__ == "__main__":
    unittest.main()
