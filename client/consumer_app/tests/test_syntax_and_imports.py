"""Unit test verifying syntax, imports, configuration translation, and models."""

import hashlib
import unittest
from pathlib import Path


class TestSyntaxAndImports(unittest.TestCase):
    def test_imports(self):
        """Verify all consumer_app modules can be imported without errors."""
        import consumer_app.config as cfg
        import consumer_app.models as models
        import consumer_app.cli as cli
        import consumer_app.utils.wire_protocol as wp
        import consumer_app.utils.cert_gen as cg
        import consumer_app.utils.broker_runner as br
        import consumer_app.scenarios as sc

        self.assertIn("simple", sc.SCENARIO_MAP)
        self.assertIn("group", sc.SCENARIO_MAP)
        self.assertIn("rebalance", sc.SCENARIO_MAP)
        self.assertIn("offsets", sc.SCENARIO_MAP)
        self.assertIn("isolation", sc.SCENARIO_MAP)
        self.assertIn("longpoll", sc.SCENARIO_MAP)
        self.assertIn("headers", sc.SCENARIO_MAP)
        self.assertIn("security", sc.SCENARIO_MAP)

    def test_models_and_json(self):
        from consumer_app.models import ConsumedRecord, ScenarioResult, SuiteReport

        val = b"hello test payload"
        rec = ConsumedRecord(
            topic="test-topic",
            partition=0,
            offset=42,
            key=b"k1",
            value=val,
            headers=[("h1", b"v1")],
        )
        self.assertEqual(rec.sha256_checksum, hashlib.sha256(val).hexdigest())
        d = rec.to_dict()
        self.assertEqual(d["topic"], "test-topic")
        self.assertEqual(d["offset"], 42)
        self.assertEqual(d["headers"], [("h1", "v1")])

        sr = ScenarioResult(
            scenario_name="test_scen",
            success=True,
            duration_ms=12.34,
            records_consumed=1,
            records=[rec],
        )
        report = SuiteReport(scenarios=[sr])
        self.assertTrue(report.all_successful)
        self.assertEqual(report.passed, 1)
        self.assertEqual(report.failed, 0)
        json_str = report.to_json()
        self.assertIn("test_scen", json_str)
        self.assertIn("all_passed", json_str)

    def test_config_translation(self):
        from consumer_app.config import KafkaConsumerSettings

        s = KafkaConsumerSettings(
            bootstrap_server="10.0.0.1:9092",
            group_id="grp-1",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="alice",
            sasl_password="secret-password",
            ssl_cafile="/path/to/ca.crt",
        )
        c_conf = s.to_confluent_config()
        self.assertEqual(c_conf["bootstrap.servers"], "10.0.0.1:9092")
        self.assertEqual(c_conf["group.id"], "grp-1")
        self.assertEqual(c_conf["security.protocol"], "sasl_ssl")
        self.assertEqual(c_conf["sasl.mechanism"], "SCRAM-SHA-256")
        self.assertEqual(c_conf["sasl.username"], "alice")
        self.assertEqual(c_conf["ssl.ca.location"], "/path/to/ca.crt")

        k_conf = s.to_kafka_python_config()
        self.assertEqual(k_conf["bootstrap_servers"], "10.0.0.1:9092")
        self.assertEqual(k_conf["group_id"], "grp-1")
        self.assertEqual(k_conf["security_protocol"], "SASL_SSL")
        self.assertEqual(k_conf["sasl_mechanism"], "SCRAM-SHA-256")
        self.assertEqual(k_conf["sasl_plain_username"], "alice")
        self.assertEqual(k_conf["ssl_cafile"], "/path/to/ca.crt")

    def test_cli_parser(self):
        from consumer_app.cli import build_parser

        parser = build_parser()
        args = parser.parse_args([
            "--scenario", "offsets",
            "--bootstrap-server", "127.0.0.1:9092",
            "--topic", "custom-topic",
            "--json-output",
        ])
        self.assertEqual(args.scenario, "offsets")
        self.assertEqual(args.bootstrap_server, "127.0.0.1:9092")
        self.assertEqual(args.topic, "custom-topic")
        self.assertTrue(args.json_output)


if __name__ == "__main__":
    unittest.main()
