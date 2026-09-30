//! Shared TLS/mTLS building blocks used by the data-plane listener (`net::server`), the Kafka
//! wire-protocol listener (`net::kafka_server`), and this broker's own outbound TLS clients
//! (`net::client`, `grpc`, `topology`).

use std::error::Error;
use std::io;
use std::path::Path;
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

/// Load a PEM certificate chain from disk.
pub fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let bytes = std::fs::read(path)?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &bytes[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificates found in {:?}", path),
        ));
    }
    Ok(certs)
}

/// Load a PEM private key from disk.
pub fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let bytes = std::fs::read(path)?;
    rustls_pemfile::private_key(&mut &bytes[..])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("no private key found in {:?}", path)))
}

fn load_root_store(ca_file: &Path) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for c in load_certs(ca_file)? {
        roots
            .add(c)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    }
    Ok(roots)
}

/// Build a `TlsAcceptor` for a server listener. When `require_client_cert` is set, clients must
/// present a certificate signed by `ca_file` (mutual TLS) or the handshake fails; otherwise this
/// is ordinary server-authenticated TLS.
pub fn build_acceptor(
    cert_file: &Path,
    key_file: &Path,
    ca_file: Option<&Path>,
    require_client_cert: bool,
) -> Result<TlsAcceptor, Box<dyn Error + Send + Sync>> {
    let certs = load_certs(cert_file)?;
    let key = load_key(key_file)?;

    let builder = ServerConfig::builder();
    let config = if require_client_cert {
        let ca_path = ca_file.ok_or("tls.require_client_cert is true but tls.ca_file is not set")?;
        let roots = Arc::new(load_root_store(ca_path)?);
        let verifier = WebPkiClientVerifier::builder(roots).build()?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)?
    } else {
        builder.with_no_client_auth().with_single_cert(certs, key)?
    };

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Extracts Subject Common Name (CN) from leaf certificate DER bytes for mTLS identity.
/// Returns None if certificate DER is invalid or does not contain a CN.
pub fn common_name_from_der(der: &[u8]) -> Option<String> {
    let (_, cert) = x509_parser::parse_x509_certificate(der).ok()?;
    cert.subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, DistinguishedName, DnType, Error as RcgenError, IsCa, KeyPair};
    use std::io::Write;
    use tempfile::NamedTempFile;
    use tokio_rustls::rustls::pki_types::ServerName;
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};
    use tokio_rustls::TlsConnector;

    struct Pem {
        cert_pem: String,
        key_pem: String,
    }

    fn write_pem(pem: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(pem.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    struct TestCa {
        cert: rcgen::Certificate,
        key: KeyPair,
        pem: String,
    }

    fn make_ca() -> Result<TestCa, RcgenError> {
        let mut ca_params = CertificateParams::new(Vec::new())?;
        ca_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate()?;
        let ca_cert = ca_params.self_signed(&ca_key)?;
        let pem = ca_cert.pem();
        Ok(TestCa { cert: ca_cert, key: ca_key, pem })
    }

    /// Build a leaf cert (for `localhost`, or with the given `cn` in its Subject) signed by `ca`.
    fn make_leaf(ca: &TestCa, cn: Option<&str>) -> Result<Pem, RcgenError> {
        let mut leaf_params = if cn.is_some() {
            CertificateParams::new(Vec::new())?
        } else {
            CertificateParams::new(vec!["localhost".to_string()])?
        };
        if let Some(cn) = cn {
            let mut dn = DistinguishedName::new();
            dn.push(DnType::CommonName, cn);
            leaf_params.distinguished_name = dn;
        }
        let leaf_key = KeyPair::generate()?;
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca.cert, &ca.key)?;

        Ok(Pem {
            cert_pem: leaf_cert.pem(),
            key_pem: leaf_key.serialize_pem(),
        })
    }

    fn client_config_no_auth(ca_pem: &str) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut ca_pem.as_bytes()) {
            roots.add(c.unwrap()).unwrap();
        }
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    }

    fn client_config_with_cert(ca_pem: &str, client: &Pem) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        for c in rustls_pemfile::certs(&mut ca_pem.as_bytes()) {
            roots.add(c.unwrap()).unwrap();
        }
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut client.cert_pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut client.key_pem.as_bytes())
            .unwrap()
            .unwrap();
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)
            .unwrap()
    }

    #[test]
    fn common_name_from_der_reads_the_subject_cn() {
        let ca = make_ca().unwrap();
        let leaf = make_leaf(&ca, Some("test-principal")).unwrap();
        let der = rustls_pemfile::certs(&mut leaf.cert_pem.as_bytes())
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(common_name_from_der(der.as_ref()).as_deref(), Some("test-principal"));
    }

    #[tokio::test]
    async fn mtls_handshake_succeeds_and_extracts_client_principal() {
        let ca = make_ca().unwrap();
        let server = make_leaf(&ca, None).unwrap();
        let client = make_leaf(&ca, Some("kafka-client-1")).unwrap();

        let server_cert_file = write_pem(&server.cert_pem);
        let server_key_file = write_pem(&server.key_pem);
        let ca_file = write_pem(&ca.pem);

        let acceptor = build_acceptor(
            server_cert_file.path(),
            server_key_file.path(),
            Some(ca_file.path()),
            /* require_client_cert */ true,
        )
        .expect("build mTLS acceptor");

        let connector = TlsConnector::from(Arc::new(client_config_with_cert(&ca.pem, &client)));

        let (client_io, server_io) = tokio::io::duplex(8 * 1024);
        let server_name = ServerName::try_from("localhost").unwrap();

        let (server_res, client_res) = tokio::join!(
            acceptor.accept(server_io),
            connector.connect(server_name, client_io)
        );
        let server_tls = server_res.expect("server-side mTLS handshake should succeed");
        client_res.expect("client-side mTLS handshake should succeed");

        let (_, session) = server_tls.get_ref();
        let der: Vec<u8> = session.peer_certificates().unwrap().first().unwrap().as_ref().to_vec();
        let principal = common_name_from_der(&der);
        assert_eq!(principal.as_deref(), Some("kafka-client-1"));
    }

    #[tokio::test]
    async fn mtls_handshake_rejects_client_with_no_certificate() {
        let ca = make_ca().unwrap();
        let server = make_leaf(&ca, None).unwrap();

        let server_cert_file = write_pem(&server.cert_pem);
        let server_key_file = write_pem(&server.key_pem);
        let ca_file = write_pem(&ca.pem);

        let acceptor = build_acceptor(
            server_cert_file.path(),
            server_key_file.path(),
            Some(ca_file.path()),
            /* require_client_cert */ true,
        )
        .expect("build mTLS acceptor");

        // A client that trusts the CA but presents no client certificate of its own.
        let connector = TlsConnector::from(Arc::new(client_config_no_auth(&ca.pem)));

        let (client_io, server_io) = tokio::io::duplex(8 * 1024);
        let server_name = ServerName::try_from("localhost").unwrap();

        let (server_res, _client_res) = tokio::join!(
            acceptor.accept(server_io),
            connector.connect(server_name, client_io)
        );
        assert!(server_res.is_err(), "mTLS handshake must fail without a client certificate");
    }
}
