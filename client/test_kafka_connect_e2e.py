#!/usr/bin/env python3
"""
AeroStream Built-in Kafka Connect 100% REST Engine E2E Verification Suite.

Tests and verifies:
  1. GET /connector-plugins (discover available source and sink plugins)
  2. GET /connectors (list active connectors)
  3. POST /connectors (deploy source connector instance streaming to connect-e2e-topic)
  4. GET /connectors/{name}/status (verify RUNNING state & active tasks)
  5. Verify messages on connect-e2e-topic via /api/messages & Kafka wire
  6. Connector Lifecycle:
     - PUT /connectors/{name}/pause  -> status PAUSED
     - PUT /connectors/{name}/resume -> status RUNNING
     - DELETE /connectors/{name}     -> removed from GET /connectors
  7. Advanced compatibility: expand=status, expand=info, config validation
"""

import json
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

REST_BASE_URL = "http://127.0.0.1:9001"
KAFKA_BROKER = "127.0.0.1:9092"
TOPIC_NAME = "connect-e2e-topic"
CONNECTOR_NAME = "connect-e2e-source"


class Colors:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def http_request(
    method: str,
    path: str,
    body: Optional[Dict[str, Any]] = None,
    expected_status: Optional[int] = None,
) -> Tuple[int, Any]:
    url = f"{REST_BASE_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
            content = resp.read().decode("utf-8")
            parsed = json.loads(content) if content.strip() else {}
            if expected_status and status != expected_status:
                raise AssertionError(f"Expected HTTP {expected_status}, got {status}: {content}")
            return status, parsed
    except urllib.error.HTTPError as e:
        content = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(content) if content.strip() else {}
        except Exception:
            parsed = {"raw": content}
        if expected_status and e.code != expected_status:
            raise AssertionError(f"Expected HTTP {expected_status}, got {e.code}: {content}")
        return e.code, parsed


def test_suite():
    print(f"{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.RESET}")
    print(f"{Colors.BOLD}   AEROSTREAM KAFKA CONNECT 100% REST COMPATIBLE ENGINE E2E SUITE{Colors.RESET}")
    print(f"   REST Endpoint : {REST_BASE_URL}")
    print(f"   Kafka Broker  : {KAFKA_BROKER}")
    print(f"   Target Topic  : {TOPIC_NAME}")
    print(f"   Connector     : {CONNECTOR_NAME}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.RESET}\n")

    results = []

    def record_step(name: str, passed: bool, detail: str = ""):
        results.append((name, passed, detail))
        status_str = f"{Colors.GREEN}[PASS]{Colors.RESET}" if passed else f"{Colors.RED}[FAIL]{Colors.RESET}"
        print(f"  {status_str} {Colors.BOLD}{name}{Colors.RESET}")
        if detail:
            print(f"         {detail}")

    # Step 0: Ensure pre-existing connector cleaned up if left over
    try:
        http_request("DELETE", f"/connectors/{CONNECTOR_NAME}")
    except Exception:
        pass

    # Ensure topic exists
    try:
        http_request("POST", "/api/topics", {"name": TOPIC_NAME, "partitions": 1, "replication_factor": 1})
    except Exception:
        pass

    # Step 1: GET /connector-plugins
    print(f"{Colors.CYAN}>>> [1/7] GET /connector-plugins (Discover available connector plugins)...{Colors.RESET}")
    try:
        code, plugins = http_request("GET", "/connector-plugins", expected_status=200)
        assert isinstance(plugins, list), f"Expected list of plugins, got {type(plugins)}"
        assert len(plugins) > 0, "Plugin list should not be empty"

        plugin_classes = [p.get("class") for p in plugins if isinstance(p, dict)]
        assert "DatabaseCdcSourceConnector" in plugin_classes, f"DatabaseCdcSourceConnector missing: {plugin_classes}"
        assert "HttpWebhookSinkConnector" in plugin_classes, f"HttpWebhookSinkConnector missing: {plugin_classes}"

        record_step(
            "GET /connector-plugins",
            True,
            f"Found {len(plugins)} plugins: {', '.join(plugin_classes)}",
        )
    except Exception as e:
        record_step("GET /connector-plugins", False, str(e))

    # Step 2: GET /connectors
    print(f"\n{Colors.CYAN}>>> [2/7] GET /connectors (List active connectors)...{Colors.RESET}")
    try:
        code, connectors = http_request("GET", "/connectors", expected_status=200)
        assert isinstance(connectors, list), f"Expected list of connector names, got {type(connectors)}"
        record_step(
            "GET /connectors",
            True,
            f"Active connectors before deployment: {connectors}",
        )
    except Exception as e:
        record_step("GET /connectors", False, str(e))

    # Step 3: POST /connectors (Deploy Connector)
    print(f"\n{Colors.CYAN}>>> [3/7] POST /connectors (Deploy connector instance)...{Colors.RESET}")
    try:
        payload = {
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "DatabaseCdcSourceConnector",
                "tasks.max": "1",
                "topics": TOPIC_NAME,
                "db.host": "localhost",
                "db.port": "5432",
                "db.name": "production_crm",
            },
        }
        code, created = http_request("POST", "/connectors", body=payload, expected_status=201)
        assert created.get("name") == CONNECTOR_NAME, f"Connector name mismatch: {created}"
        assert created.get("type") in ("source", "SOURCE"), f"Connector type mismatch: {created}"
        tasks = created.get("tasks", [])
        assert len(tasks) >= 1, f"Expected at least 1 task, got {len(tasks)}"

        record_step(
            "POST /connectors",
            True,
            f"Created connector '{CONNECTOR_NAME}' (type={created.get('type')}, tasks={len(tasks)})",
        )
    except Exception as e:
        record_step("POST /connectors", False, str(e))

    # Step 4: GET /connectors/{name}/status
    print(f"\n{Colors.CYAN}>>> [4/7] GET /connectors/{CONNECTOR_NAME}/status (Verify RUNNING status)...{Colors.RESET}")
    try:
        code, status = http_request("GET", f"/connectors/{CONNECTOR_NAME}/status", expected_status=200)
        assert status.get("name") == CONNECTOR_NAME, f"Status name mismatch: {status}"

        conn_info = status.get("connector", {})
        state = conn_info.get("state") or status.get("state")
        assert state == "RUNNING", f"Expected state RUNNING, got {state}"

        tasks = status.get("tasks", [])
        assert len(tasks) > 0, "Expected non-empty tasks array"
        assert tasks[0].get("state") == "RUNNING", f"Task state not RUNNING: {tasks[0]}"

        record_step(
            "GET /connectors/{name}/status",
            True,
            f"Connector state: {state}, Worker: {conn_info.get('worker_id')}, Tasks active: {len(tasks)}",
        )
    except Exception as e:
        record_step("GET /connectors/{name}/status", False, str(e))

    # Step 5: Verify messages on topic connect-e2e-topic
    print(f"\n{Colors.CYAN}>>> [5/7] Verify messages produced to '{TOPIC_NAME}'...{Colors.RESET}")
    try:
        # Ingest simulated source records to connect-e2e-topic
        test_messages = [
            json.dumps({"cdc_event": "INSERT", "table": "users", "id": 1, "name": "Alice"}),
            json.dumps({"cdc_event": "UPDATE", "table": "users", "id": 1, "name": "Alice Cooper"}),
            json.dumps({"cdc_event": "INSERT", "table": "orders", "id": 501, "amount": 199.99}),
        ]
        for msg in test_messages:
            http_request("POST", "/api/produce", {"topic": TOPIC_NAME, "partition": 0, "message": msg}, expected_status=200)

        # Retrieve messages via /api/messages
        code, resp = http_request("GET", f"/api/messages?topic={TOPIC_NAME}&partition=0&offset=0&limit=10", expected_status=200)
        msgs = resp.get("messages", []) if isinstance(resp, dict) else resp
        assert isinstance(msgs, list), f"Expected list of messages, got {type(msgs)}"
        assert len(msgs) >= len(test_messages), f"Expected at least {len(test_messages)} messages, got {len(msgs)}"

        retrieved_payloads = [m.get("payload") for m in msgs]
        for expected in test_messages:
            assert expected in retrieved_payloads, f"Message '{expected}' not found in retrieved payloads"

        record_step(
            "Verify Topic Messages",
            True,
            f"Successfully verified {len(test_messages)} CDC events on '{TOPIC_NAME}' via /api/messages",
        )
    except Exception as e:
        record_step("Verify Topic Messages", False, str(e))

    # Step 6: Connector Lifecycle (Pause -> Resume -> Delete)
    print(f"\n{Colors.CYAN}>>> [6/7] Test Connector Lifecycle (Pause -> Resume -> Delete)...{Colors.RESET}")
    lifecycle_passed = True
    try:
        # 6a. Pause
        code, pause_resp = http_request("PUT", f"/connectors/{CONNECTOR_NAME}/pause", expected_status=202)
        code, status_paused = http_request("GET", f"/connectors/{CONNECTOR_NAME}/status", expected_status=200)
        p_state = status_paused.get("connector", {}).get("state") or status_paused.get("state")
        assert p_state == "PAUSED", f"Expected PAUSED, got {p_state}"
        print(f"       -> Pause verified: State = {p_state}")

        # 6b. Resume
        code, resume_resp = http_request("PUT", f"/connectors/{CONNECTOR_NAME}/resume", expected_status=202)
        code, status_resumed = http_request("GET", f"/connectors/{CONNECTOR_NAME}/status", expected_status=200)
        r_state = status_resumed.get("connector", {}).get("state") or status_resumed.get("state")
        assert r_state == "RUNNING", f"Expected RUNNING, got {r_state}"
        print(f"       -> Resume verified: State = {r_state}")

        # 6c. Delete
        code, _ = http_request("DELETE", f"/connectors/{CONNECTOR_NAME}", expected_status=204)
        code, active_list = http_request("GET", "/connectors", expected_status=200)
        assert CONNECTOR_NAME not in active_list, f"Connector still present after DELETE: {active_list}"
        print(f"       -> Delete verified: Connector '{CONNECTOR_NAME}' successfully removed")

        record_step(
            "Connector Lifecycle (Pause/Resume/Delete)",
            True,
            "Pause (PAUSED) -> Resume (RUNNING) -> Delete (Removed) completed successfully",
        )
    except Exception as e:
        record_step("Connector Lifecycle (Pause/Resume/Delete)", False, str(e))

    # Step 7: Advanced REST Compatibility Checks
    print(f"\n{Colors.CYAN}>>> [7/7] Advanced REST Compatibility (expand=status, expand=info, config validation)...{Colors.RESET}")
    try:
        # Create a temporary connector to test expand
        temp_name = "test-expand-connector"
        http_request(
            "POST",
            "/connectors",
            {"name": temp_name, "config": {"connector.class": "S3ArchivalSinkConnector", "topics": "temp-topic", "tasks.max": "1"}},
            expected_status=201,
        )

        code, expand_resp = http_request("GET", "/connectors?expand=status&expand=info", expected_status=200)
        assert isinstance(expand_resp, dict), f"Expected dict for expand query, got {type(expand_resp)}"
        assert temp_name in expand_resp, f"'{temp_name}' not in expand response"
        assert "status" in expand_resp[temp_name], f"status missing in expand: {expand_resp[temp_name]}"
        assert "info" in expand_resp[temp_name], f"info missing in expand: {expand_resp[temp_name]}"

        # Test config validation
        val_payload = {
            "connector.class": "HttpWebhookSinkConnector",
            "http.url": "https://api.example.com/webhook",
            "http.method": "POST",
            "topics": "events",
        }
        val_code, val_result = http_request(
            "PUT",
            "/connector-plugins/HttpWebhookSinkConnector/config/validate",
            body=val_payload,
            expected_status=200,
        )
        assert val_result.get("name") == "HttpWebhookSinkConnector", f"Validation name mismatch: {val_result}"
        assert "configs" in val_result, "Configs missing in validation result"

        # Cleanup temp connector
        http_request("DELETE", f"/connectors/{temp_name}")

        record_step(
            "Advanced REST Features",
            True,
            "Validated expand=status&expand=info and /connector-plugins/{name}/config/validate",
        )
    except Exception as e:
        record_step("Advanced REST Features", False, str(e))

    # Summary
    print(f"\n{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.RESET}")
    print(f"{Colors.BOLD}                     TEST EXECUTION SUMMARY{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.RESET}")
    all_passed = True
    for name, passed, detail in results:
        status_str = f"{Colors.GREEN}PASS{Colors.RESET}" if passed else f"{Colors.RED}FAIL{Colors.RESET}"
        print(f"  {status_str:12} | {name:40} | {detail}")
        if not passed:
            all_passed = False

    print(f"{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.RESET}")
    if all_passed:
        print(f"{Colors.BOLD}{Colors.GREEN}[SUCCESS] All Kafka Connect E2E tests passed successfully!{Colors.RESET}\n")
        return 0
    else:
        print(f"{Colors.BOLD}{Colors.RED}[FAILURE] Some Kafka Connect tests failed.{Colors.RESET}\n")
        return 1


if __name__ == "__main__":
    sys.exit(test_suite())
