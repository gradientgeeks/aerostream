use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::config::TlsConfig;

/// An outbound data-plane connection (broker-to-broker replication), either
/// plaintext or TLS.
pub enum ClientConn {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for ClientConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientConn::Plain(s) => Pin::new(s).poll_read(cx, buf),
            ClientConn::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientConn {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ClientConn::Plain(s) => Pin::new(s).poll_write(cx, buf),
            ClientConn::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientConn::Plain(s) => Pin::new(s).poll_flush(cx),
            ClientConn::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ClientConn::Plain(s) => Pin::new(s).poll_shutdown(cx),
            ClientConn::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

fn build_connector(tls: &TlsConfig) -> io::Result<TlsConnector> {
    let mut roots = RootCertStore::empty();
    if let Some(ca) = &tls.ca_file {
        let pem = std::fs::read(ca)?;
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut &pem[..]).collect::<Result<Vec<_>, _>>()?;
        for c in certs {
            roots
                .add(c)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        }
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);

    // When a client identity is configured, present it (mTLS) so a peer broker that enforces
    // `tls.require_client_cert` on its own data-plane listener can authenticate us.
    let config = match (&tls.client_cert_file, &tls.client_key_file) {
        (Some(cert_path), Some(key_path)) => {
            let certs = crate::net::tls::load_certs(cert_path)?;
            let key = crate::net::tls::load_key(key_path)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
        }
        _ => builder.with_no_client_auth(),
    };
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Dial a peer broker's data plane, optionally over TLS, and perform the AUTH
/// handshake when a token is configured. `tls_server_name` is the name used to
/// verify the peer's TLS certificate (typically the broker host).
pub async fn connect_data_plane(
    addr: &str,
    tls_server_name: &str,
    tls: &TlsConfig,
    token: &Option<String>,
) -> io::Result<ClientConn> {
    let tcp = TcpStream::connect(addr).await?;

    let mut conn = if tls.enabled {
        let connector = build_connector(tls)?;
        let server_name = ServerName::try_from(tls_server_name.to_string())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid TLS server name"))?;
        let tls_stream = connector.connect(server_name, tcp).await?;
        ClientConn::Tls(Box::new(tls_stream))
    } else {
        ClientConn::Plain(tcp)
    };

    if let Some(t) = token {
        send_auth(&mut conn, t).await?;
    }

    Ok(conn)
}

/// Send the AUTH frame ([magic][cmd=0][len][token]) and verify the broker's
/// status response.
async fn send_auth(conn: &mut ClientConn, token: &str) -> io::Result<()> {
    let token_bytes = token.as_bytes();
    let mut frame = Vec::with_capacity(7 + token_bytes.len());
    frame.extend_from_slice(&[0xAE, 0x01]);
    frame.push(0); // cmd 0 = AUTH
    frame.extend_from_slice(&(token_bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(token_bytes);
    conn.write_all(&frame).await?;

    let mut resp = [0u8; 3];
    conn.read_exact(&mut resp).await?;
    if resp[0] != 0xAE || resp[1] != 0x01 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid auth response magic"));
    }
    if resp[2] != 0 {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "data-plane authentication rejected"));
    }
    Ok(())
}
