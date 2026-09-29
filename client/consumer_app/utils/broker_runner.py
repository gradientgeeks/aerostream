"""Ephemeral AeroMQ rust-broker manager for integration and self-tests."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
from typing import Optional


class EphemeralBroker:
    """Manages an ephemeral rust-broker subprocess for integration testing."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        data_port: int = 19091,
        kafka_port: int = 19093,
        config_path: Optional[Path] = None,
        custom_args: Optional[list[str]] = None,
    ):
        self.host = host
        self.data_port = data_port
        self.kafka_port = kafka_port
        self.config_path = config_path
        self.custom_args = custom_args or []
        self.tmp_dir: Optional[str] = None
        self.proc: Optional[subprocess.Popen] = None

    @classmethod
    def find_broker_binary(cls) -> Optional[Path]:
        repo_root = Path(__file__).resolve().parents[3]
        release_bin = repo_root / "rust-broker" / "target" / "release" / "rust-broker"
        debug_bin = repo_root / "rust-broker" / "target" / "debug" / "rust-broker"

        if release_bin.exists() and debug_bin.exists():
            return release_bin if release_bin.stat().st_mtime >= debug_bin.stat().st_mtime else debug_bin
        if release_bin.exists():
            return release_bin
        if debug_bin.exists():
            return debug_bin
        return None

    def start(self, timeout_sec: float = 10.0):
        binary = self.find_broker_binary()
        if not binary:
            raise FileNotFoundError("rust-broker binary not found in target/release or target/debug")

        self.tmp_dir = tempfile.mkdtemp(prefix="aeromq-consumer-test-")
        cmd = [
            str(binary),
            "--host", self.host,
            "--data-port", str(self.data_port),
            "--kafka-port", str(self.kafka_port),
            "--storage-dir", self.tmp_dir,
        ]
        if self.config_path:
            cmd.extend(["--config", str(self.config_path)])
        cmd.extend(self.custom_args)

        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Wait for port to be accepting connections
        start_time = time.time()
        while time.time() - start_time < timeout_sec:
            if self.proc.poll() is not None:
                _, err = self.proc.communicate()
                raise RuntimeError(f"rust-broker exited early with code {self.proc.returncode}: {err}")
            try:
                with socket.create_connection((self.host, self.kafka_port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.1)

        self.stop()
        raise TimeoutError(f"rust-broker did not start listening on {self.host}:{self.kafka_port} within {timeout_sec}s")

    def stop(self):
        if self.proc:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=2.0)
            except Exception:
                try:
                    self.proc.kill()
                    self.proc.wait(timeout=1.0)
                except Exception:
                    pass
            self.proc = None

        if self.tmp_dir and os.path.exists(self.tmp_dir):
            try:
                shutil.rmtree(self.tmp_dir, ignore_errors=True)
            except Exception:
                pass
            self.tmp_dir = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
