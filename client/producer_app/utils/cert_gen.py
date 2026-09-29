"""
AeroStream Certificate Generator for TLS and mTLS Testing.
Generates ephemeral CA, server, and client certificates for authentication scenarios.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TlsCertBundle:
    temp_dir: str
    ca_cert_path: str
    ca_key_path: str
    server_cert_path: str
    server_key_path: str
    client_cert_path: str
    client_key_path: str

    def cleanup(self) -> None:
        try:
            import shutil
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        except Exception:
            pass


def generate_test_certs(common_name: str = "aerostream-client", host: str = "127.0.0.1") -> TlsCertBundle:
    """
    Generates a full test certificate bundle:
    - Root CA (ca.crt, ca.key)
    - Server cert (server.crt, server.key) with SAN for host
    - Client cert (client.crt, client.key) with Subject CN for mTLS principal
    """
    tmp_dir = tempfile.mkdtemp(prefix="aerostream_tls_")
    ca_key = os.path.join(tmp_dir, "ca.key")
    ca_crt = os.path.join(tmp_dir, "ca.crt")
    server_key = os.path.join(tmp_dir, "server.key")
    server_csr = os.path.join(tmp_dir, "server.csr")
    server_crt = os.path.join(tmp_dir, "server.crt")
    client_key = os.path.join(tmp_dir, "client.key")
    client_csr = os.path.join(tmp_dir, "client.csr")
    client_crt = os.path.join(tmp_dir, "client.crt")

    # 1. Generate CA
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", ca_key, "-out", ca_crt, "-days", "30",
         "-subj", "/CN=AeroStream-Test-CA/O=AeroStream"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    # 2. Generate Server Certificate
    subprocess.run(
        ["openssl", "req", "-newkey", "rsa:2048", "-nodes",
         "-keyout", server_key, "-out", server_csr,
         "-subj", f"/CN={host}/O=AeroStream"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    ext_conf = f"subjectAltName=IP:{host},DNS:localhost,DNS:{host}\n"
    ext_file = os.path.join(tmp_dir, "server_ext.cnf")
    with open(ext_file, "w") as f:
        f.write(ext_conf)

    subprocess.run(
        ["openssl", "x509", "-req", "-in", server_csr, "-CA", ca_crt, "-CAkey", ca_key,
         "-CAcreateserial", "-out", server_crt, "-days", "30", "-extfile", ext_file],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    # 3. Generate Client Certificate for mTLS
    subprocess.run(
        ["openssl", "req", "-newkey", "rsa:2048", "-nodes",
         "-keyout", client_key, "-out", client_csr,
         "-subj", f"/CN={common_name}/O=AeroStream"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    client_ext_file = os.path.join(tmp_dir, "client_ext.cnf")
    with open(client_ext_file, "w") as f:
        f.write("extendedKeyUsage=clientAuth\n")

    subprocess.run(
        ["openssl", "x509", "-req", "-in", client_csr, "-CA", ca_crt, "-CAkey", ca_key,
         "-CAcreateserial", "-out", client_crt, "-days", "30", "-extfile", client_ext_file],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    return TlsCertBundle(
        temp_dir=tmp_dir,
        ca_cert_path=ca_crt,
        ca_key_path=ca_key,
        server_cert_path=server_crt,
        server_key_path=server_key,
        client_cert_path=client_crt,
        client_key_path=client_key,
    )
