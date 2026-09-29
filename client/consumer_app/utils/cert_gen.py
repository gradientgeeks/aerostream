"""TLS and mTLS certificate generator using Python's cryptography library."""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Tuple

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def generate_test_pki(output_dir: Path) -> Tuple[Path, Path, Path, Path, Path]:
    """Generates a complete test PKI for TLS/mTLS:

    - ca.crt / ca.key: Root CA
    - broker.crt / broker.key: Server cert (with SAN 127.0.0.1, localhost)
    - client.crt / client.key: Client cert (with CN=client-alice for mTLS principal)

    Returns:
        (ca_crt, broker_crt, broker_key, client_crt, client_key)
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    ca_crt_path = output_dir / "ca.crt"
    ca_key_path = output_dir / "ca.key"
    broker_crt_path = output_dir / "broker.crt"
    broker_key_path = output_dir / "broker.key"
    client_crt_path = output_dir / "client.crt"
    client_key_path = output_dir / "client.key"

    # 1. Root CA
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
    ca_name = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "AeroStream Test CA"),
        x509.NameAttribute(NameOID.COMMON_NAME, "AeroStream Root CA"),
    ])
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256(), default_backend())
    )

    # 2. Broker Cert
    broker_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
    broker_name = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "AeroStream Broker"),
        x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1"),
    ])
    broker_cert = (
        x509.CertificateBuilder()
        .subject_name(broker_name)
        .issuer_name(ca_name)
        .public_key(broker_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.IPAddress(__import__("ipaddress").ip_address("127.0.0.1")),
            ]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256(), default_backend())
    )

    # 3. Client Cert (for mTLS authentication)
    client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
    client_name = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "AeroStream Client"),
        x509.NameAttribute(NameOID.COMMON_NAME, "alice"),
    ])
    client_cert = (
        x509.CertificateBuilder()
        .subject_name(client_name)
        .issuer_name(ca_name)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(ca_key, hashes.SHA256(), default_backend())
    )

    # Write PEM files
    def write_pem(path: Path, data: bytes):
        with open(path, "wb") as f:
            f.write(data)

    write_pem(ca_crt_path, ca_cert.public_bytes(serialization.Encoding.PEM))
    write_pem(ca_key_path, ca_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ))

    write_pem(broker_crt_path, broker_cert.public_bytes(serialization.Encoding.PEM))
    write_pem(broker_key_path, broker_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ))

    write_pem(client_crt_path, client_cert.public_bytes(serialization.Encoding.PEM))
    write_pem(client_key_path, client_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ))

    return ca_crt_path, broker_crt_path, broker_key_path, client_crt_path, client_key_path
