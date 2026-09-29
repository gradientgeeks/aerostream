"""
Scenario 8: Security & Authentication.
Validates authenticated and encrypted producer connections using:
  1. SASL/PLAIN (username/password)
  2. SASL/SCRAM-SHA-256 (RFC 5802 challenge-response)
  3. TLS/SSL (encrypted transport with server certificate verification)
  4. Mutual TLS (mTLS) with client certificate Subject CN principal
"""

from __future__ import annotations

import logging
import os
from typing import Any, List, Optional, Tuple

from scenarios.base import BaseScenario, generate_payload
from utils.cert_gen import generate_test_certs
from utils.producer_factory import ProducerOptions
from utils.reporter import ScenarioResult

logger = logging.getLogger(__name__)


class SecurityScenario(BaseScenario):
    """Exercises Kafka SASL authentication and SSL/TLS transport encryption."""

    def run(self, count: Optional[int] = None, **kwargs: Any) -> List[ScenarioResult]:
        n = count or 10
        results: List[ScenarioResult] = []

        # Extract user parameters or apply defaults compatible with AeroStream broker
        user_sec_proto = kwargs.get("security_protocol")
        user_sasl_mech = kwargs.get("sasl_mechanism")
        user_username = kwargs.get("sasl_username") or "aerostream"
        user_password = kwargs.get("sasl_password") or "aerostream123"
        ca_cert = kwargs.get("ca_cert")
        client_cert = kwargs.get("client_cert")
        client_key = kwargs.get("client_key")

        # -------------------------------------------------------------
        # 1. SASL/PLAIN Authentication
        # -------------------------------------------------------------
        if not user_sec_proto or "SASL" in user_sec_proto.upper():
            records_plain = [
                (f"sec-plain-{i}".encode("utf-8"), generate_payload(128, prefix=f"sasl-plain-{i}"), None)
                for i in range(n)
            ]
            opts_plain = ProducerOptions(
                bootstrap_servers=self.bootstrap_servers,
                client_id="aerostream-sasl-plain",
                acks=1,
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_username=user_username,
                sasl_password=user_password,
                request_timeout_ms=10000,
            )
            r_plain = self.produce_batch(
                opts_plain,
                records_plain,
                subtest_name="sasl_plain_authentication",
                scenario_name="Security & Authentication",
            )
            r_plain.details["mechanism"] = "PLAIN"
            r_plain.details["username"] = user_username
            results.append(r_plain)

        # -------------------------------------------------------------
        # 2. SASL/SCRAM-SHA-256 Authentication
        # -------------------------------------------------------------
        if not user_sec_proto or (user_sasl_mech and "SCRAM" in user_sasl_mech.upper()):
            records_scram = [
                (f"sec-scram-{i}".encode("utf-8"), generate_payload(128, prefix=f"sasl-scram-{i}"), None)
                for i in range(n)
            ]
            opts_scram = ProducerOptions(
                bootstrap_servers=self.bootstrap_servers,
                client_id="aerostream-sasl-scram",
                acks=1,
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="SCRAM-SHA-256",
                sasl_username=user_username,
                sasl_password=user_password,
                request_timeout_ms=10000,
            )
            r_scram = self.produce_batch(
                opts_scram,
                records_scram,
                subtest_name="sasl_scram_sha_256",
                scenario_name="Security & Authentication",
            )
            r_scram.details["mechanism"] = "SCRAM-SHA-256"
            r_scram.details["username"] = user_username
            results.append(r_scram)

        # -------------------------------------------------------------
        # 3. TLS / SSL Encryption & mTLS Client Certificates
        # -------------------------------------------------------------
        if user_sec_proto and "SSL" in user_sec_proto.upper():
            # Standard SSL / TLS
            records_tls = [
                (f"sec-tls-{i}".encode("utf-8"), generate_payload(128, prefix=f"tls-{i}"), None)
                for i in range(n)
            ]
            opts_tls = ProducerOptions(
                bootstrap_servers=self.bootstrap_servers,
                client_id="aerostream-tls-client",
                acks=1,
                security_protocol="SSL",
                ca_cert_path=ca_cert,
                request_timeout_ms=10000,
            )
            r_tls = self.produce_batch(
                opts_tls,
                records_tls,
                subtest_name="tls_server_verification",
                scenario_name="Security & Authentication",
            )
            results.append(r_tls)

            # Mutual TLS (mTLS) if client cert/key are provided
            if client_cert and client_key:
                records_mtls = [
                    (f"sec-mtls-{i}".encode("utf-8"), generate_payload(128, prefix=f"mtls-{i}"), None)
                    for i in range(n)
                ]
                opts_mtls = ProducerOptions(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id="aerostream-mtls-client",
                    acks=1,
                    security_protocol="SSL",
                    ca_cert_path=ca_cert,
                    client_cert_path=client_cert,
                    client_key_path=client_key,
                    request_timeout_ms=10000,
                )
                r_mtls = self.produce_batch(
                    opts_mtls,
                    records_mtls,
                    subtest_name="mtls_client_certificate_auth",
                    scenario_name="Security & Authentication",
                )
                r_mtls.details["client_cert"] = client_cert
                results.append(r_mtls)

        return results
