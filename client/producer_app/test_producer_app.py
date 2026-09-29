"""
AeroStream Kafka Producer Test Suite.
Validates code syntax, argument parsing, wire protocol encoding,
and all 8 test scenarios with embedded mock broker.
"""

import json
import socket
import struct
import sys
import unittest
from pathlib import Path

# Add current folder to sys.path
APP_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(APP_DIR))

from config import create_parser
from scenarios.base import generate_payload, parse_size_str
from scenarios.basic import BasicProduceScenario
from scenarios.compression import CompressionScenario
from scenarios.headers import MessageHeadersScenario
from scenarios.idempotence import IdempotentProducerScenario
from scenarios.keys import PartitionRoutingScenario
from scenarios.large import LargePayloadScenario
from scenarios.security import SecurityScenario
from scenarios.transactions import TransactionalProducerScenario
from utils.cert_gen import generate_test_certs
from utils.mock_broker import MockKafkaBroker, put_string
from utils.producer_factory import ProducerOptions, build_confluent_producer
from utils.reporter import ScenarioResult, TestReporter


class TestProducerConfiguration(unittest.TestCase):
    """Verifies CLI argument parsing and defaults."""

    def test_parser_defaults(self):
        parser = create_parser()
        args = parser.parse_args([])
        self.assertEqual(args.scenario, "all")
        self.assertEqual(args.bootstrap_server, "127.0.0.1:9092")
        self.assertEqual(args.topic, "aerostream-test")
        self.assertEqual(args.count, 100)
        self.assertFalse(args.json_output)
        self.assertEqual(args.backend, "confluent")

    def test_parser_custom_args(self):
        parser = create_parser()
        cmd = [
            "--scenario", "compression",
            "--bootstrap-server", "10.0.0.1:9093",
            "--topic", "custom-topic",
            "--count", "50",
            "--json-output",
            "--acks", "all",
            "--compression", "zstd",
            "--linger-ms", "15",
            "--batch-size", "65536",
        ]
        args = parser.parse_args(cmd)
        self.assertEqual(args.scenario, "compression")
        self.assertEqual(args.bootstrap_server, "10.0.0.1:9093")
        self.assertEqual(args.topic, "custom-topic")
        self.assertEqual(args.count, 50)
        self.assertTrue(args.json_output)
        self.assertEqual(args.acks, "all")
        self.assertEqual(args.compression, "zstd")
        self.assertEqual(args.linger_ms, 15)
        self.assertEqual(args.batch_size, 65536)


class TestHelpers(unittest.TestCase):
    """Tests payload generation and size parsing."""

    def test_parse_size_str(self):
        self.assertEqual(parse_size_str("100B"), 100)
        self.assertEqual(parse_size_str("1KB"), 1024)
        self.assertEqual(parse_size_str("64KB"), 64 * 1024)
        self.assertEqual(parse_size_str("1MB"), 1024 * 1024)
        self.assertEqual(parse_size_str("5MB"), 5 * 1024 * 1024)

    def test_generate_payload(self):
        p100 = generate_payload(100, prefix="test")
        self.assertEqual(len(p100), 100)
        self.assertTrue(p100.startswith(b"test:ts="))

        p1k = generate_payload(1024)
        self.assertEqual(len(p1k), 1024)


class TestMockBrokerWireProtocol(unittest.TestCase):
    """Verifies low-level Kafka wire frames against embedded mock broker."""

    @classmethod
    def setUpClass(cls):
        cls.broker = MockKafkaBroker(host="127.0.0.1", port=0)
        cls.port = cls.broker.start()

    @classmethod
    def tearDownClass(cls):
        cls.broker.stop()

    def _send_req(self, payload: bytes) -> bytes:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", self.port))
        s.settimeout(5.0)
        s.sendall(struct.pack(">i", len(payload)) + payload)
        hdr = s.recv(4)
        resp_len = struct.unpack(">i", hdr)[0]
        buf = bytearray()
        while len(buf) < resp_len:
            c = s.recv(resp_len - len(buf))
            buf.extend(c)
        s.close()
        return bytes(buf)

    def test_api_versions_wire(self):
        corr_id = 9001
        # ApiKey=18, version=0, corr_id, client_id
        req = struct.pack(">hhi", 18, 0, corr_id) + put_string("test-client")
        resp = self._send_req(req)
        resp_corr_id, error_code, num_keys = struct.unpack_from(">ihi", resp, 0)
        self.assertEqual(resp_corr_id, corr_id)
        self.assertEqual(error_code, 0)
        self.assertGreaterEqual(num_keys, 10)

    def test_metadata_wire(self):
        corr_id = 9002
        req = struct.pack(">hhi", 3, 0, corr_id) + put_string("test-client")
        # Topics array: count=1, "aerostream-test"
        req += struct.pack(">i", 1) + put_string("aerostream-test")
        resp = self._send_req(req)
        resp_corr_id = struct.unpack_from(">i", resp, 0)[0]
        self.assertEqual(resp_corr_id, corr_id)

    def test_init_producer_id_wire(self):
        corr_id = 9003
        req = struct.pack(">hhi", 22, 0, corr_id) + put_string("test-client")
        resp = self._send_req(req)
        resp_corr_id, throttle, err_code, pid, epoch = struct.unpack_from(">iihqh", resp, 0)
        self.assertEqual(resp_corr_id, corr_id)
        self.assertEqual(err_code, 0)
        self.assertGreaterEqual(pid, 1000)
        self.assertEqual(epoch, 0)


class TestScenariosEndToEnd(unittest.TestCase):
    """Executes all 8 scenarios end-to-end against embedded mock broker."""

    @classmethod
    def setUpClass(cls):
        cls.broker = MockKafkaBroker(host="127.0.0.1", port=0)
        cls.port = cls.broker.start()
        cls.bootstrap = f"127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.broker.stop()

    def test_scenario_1_basic_produce(self):
        sc = BasicProduceScenario(bootstrap_servers=self.bootstrap, default_count=20, timeout_sec=5.0)
        results = sc.run(count=20)
        self.assertTrue(len(results) >= 2)
        for r in results:
            self.assertTrue(r.success, f"Subtest {r.subtest} failed: {r.error}")

    def test_scenario_2_compression(self):
        sc = CompressionScenario(bootstrap_servers=self.bootstrap, default_count=10, timeout_sec=5.0)
        results = sc.run(count=10)
        self.assertEqual(len(results), 5)
        for r in results:
            self.assertTrue(r.success, f"Codec {r.subtest} failed: {r.error}")

    def test_scenario_3_headers(self):
        sc = MessageHeadersScenario(bootstrap_servers=self.bootstrap, default_count=10, timeout_sec=5.0)
        results = sc.run(count=10)
        self.assertEqual(len(results), 2)
        for r in results:
            self.assertTrue(r.success, f"Headers subtest {r.subtest} failed: {r.error}")

    def test_scenario_4_partition_keys(self):
        sc = PartitionRoutingScenario(bootstrap_servers=self.bootstrap, default_count=15, timeout_sec=5.0)
        results = sc.run(count=15)
        self.assertEqual(len(results), 2)
        for r in results:
            self.assertTrue(r.success, f"Key routing {r.subtest} failed: {r.error}")

    def test_scenario_5_large_payloads(self):
        sc = LargePayloadScenario(bootstrap_servers=self.bootstrap, default_count=5, timeout_sec=10.0)
        results = sc.run(count=2)
        self.assertEqual(len(results), 5)
        for r in results:
            self.assertTrue(r.success, f"Large payload {r.subtest} failed: {r.error}")

    def test_scenario_6_idempotence(self):
        sc = IdempotentProducerScenario(bootstrap_servers=self.bootstrap, default_count=10, timeout_sec=5.0)
        results = sc.run(count=10)
        self.assertEqual(len(results), 2)
        for r in results:
            self.assertTrue(r.success, f"Idempotence {r.subtest} failed: {r.error}")

    def test_scenario_7_transactions(self):
        sc = TransactionalProducerScenario(bootstrap_servers=self.bootstrap, default_count=5, timeout_sec=5.0)
        results = sc.run(count=5)
        self.assertEqual(len(results), 3)
        for r in results:
            self.assertTrue(r.success, f"Transactions {r.subtest} failed: {r.error}")

    def test_scenario_8_security_sasl(self):
        sc = SecurityScenario(bootstrap_servers=self.bootstrap, default_count=5, timeout_sec=5.0)
        results = sc.run(count=5)
        self.assertTrue(len(results) >= 1)
        for r in results:
            self.assertTrue(r.success, f"Security {r.subtest} failed: {r.error}")


class TestCertGeneration(unittest.TestCase):
    """Verifies self-signed CA and client cert generation."""

    def test_generate_certs(self):
        bundle = generate_test_certs(common_name="test-principal", host="127.0.0.1")
        try:
            self.assertTrue(Path(bundle.ca_cert_path).is_file())
            self.assertTrue(Path(bundle.client_cert_path).is_file())
            self.assertTrue(Path(bundle.client_key_path).is_file())
        finally:
            bundle.cleanup()


class TestReporterOutput(unittest.TestCase):
    """Tests JSON serialization of results."""

    def test_json_reporting(self):
        rep = TestReporter(json_output=True)
        r = ScenarioResult(
            scenario="Basic Produce",
            subtest="acks_1",
            success=True,
            messages_sent=100,
            bytes_sent=10240,
            duration_sec=0.5,
        )
        rep.record(r)
        d = r.to_dict()
        self.assertEqual(d["scenario"], "Basic Produce")
        self.assertEqual(d["messages_sent"], 100)
        self.assertIn("throughput_msgs_sec", d)


if __name__ == "__main__":
    unittest.main()
