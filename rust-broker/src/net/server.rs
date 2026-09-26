use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;
use tracing::{info, error, debug, warn};

use crate::config::BrokerConfig;
use crate::log::LogManager;

pub struct DataServer {
    addr: SocketAddr,
    log_manager: Arc<LogManager>,
    cfg: Arc<BrokerConfig>,
}

impl DataServer {
    pub fn new(addr: SocketAddr, log_manager: Arc<LogManager>, cfg: Arc<BrokerConfig>) -> Self {
        Self { addr, log_manager, cfg }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.addr).await?;

        // Build a TLS acceptor up front if data-plane TLS is enabled.
        let acceptor = if self.cfg.tls.enabled {
            let acc = build_tls_acceptor(&self.cfg)?;
            info!("[AeroMQ Broker] Data Plane TLS enabled (zero-copy sendfile path disabled)");
            Some(acc)
        } else {
            None
        };
        let auth_token = self.cfg.auth.token.clone();

        info!("[AeroMQ Broker] Data Plane TCP Server listening on {}", self.addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let cpu_core = unsafe { libc::sched_getcpu() };
            info!("[AeroMQ Broker] Accepted connection from {} on CPU core {}", peer_addr, cpu_core);
            let log_manager = self.log_manager.clone();
            let acceptor = acceptor.clone();
            let token = auth_token.clone();

            tokio::spawn(async move {
                // Wrap the raw socket in a plaintext or TLS connection.
                let conn = match acceptor {
                    Some(acc) => match acc.accept(stream).await {
                        Ok(tls) => Conn::Tls(tls),
                        Err(e) => {
                            error!("[AeroMQ Broker] TLS handshake failed for {}: {:?}", peer_addr, e);
                            return;
                        }
                    },
                    None => Conn::Plain(stream),
                };

                if let Err(e) = handle_connection(conn, log_manager, token).await {
                    error!("[AeroMQ Broker] Error handling connection from {}: {:?}", peer_addr, e);
                }
            });
        }
    }
}

/// Build a TLS acceptor from the broker's configured cert/key.
fn build_tls_acceptor(cfg: &BrokerConfig) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let cert_path = cfg
        .tls
        .cert_file
        .as_ref()
        .ok_or("tls.enabled is true but tls.cert_file is not set")?;
    let key_path = cfg
        .tls
        .key_file
        .as_ref()
        .ok_or("tls.enabled is true but tls.key_file is not set")?;

    let cert_bytes = std::fs::read(cert_path)?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &cert_bytes[..])
        .collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(format!("no certificates found in {:?}", cert_path).into());
    }

    let key_bytes = std::fs::read(key_path)?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut &key_bytes[..])?
        .ok_or_else(|| format!("no private key found in {:?}", key_path))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A client connection that is either plaintext (supports `sendfile` zero-copy)
/// or TLS (encryption forces a buffered copy through userspace).
enum Conn {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl Conn {
    /// Transfer `bytes` of `file` starting at `position` to the socket.
    /// Plaintext connections use the `sendfile(2)` zero-copy fast path; TLS
    /// connections read the region into memory and write it through the TLS
    /// stream (zero-copy is impossible once encryption is in play).
    async fn send_file_region(
        &mut self,
        file: File,
        position: u64,
        bytes: u32,
    ) -> io::Result<()> {
        match self {
            Conn::Plain(stream) => {
                let socket_fd = stream.as_raw_fd();
                tokio::task::spawn_blocking(move || {
                    let file_fd = file.as_raw_fd();
                    set_blocking(socket_fd, true)?;
                    let mut offset = position as libc::off_t;
                    let mut remaining = bytes as usize;

                    while remaining > 0 {
                        let written = unsafe {
                            libc::sendfile(socket_fd, file_fd, &mut offset, remaining)
                        };
                        if written < 0 {
                            let err = std::io::Error::last_os_error();
                            let _ = set_blocking(socket_fd, false);
                            return Err(err);
                        }
                        if written == 0 {
                            break; // EOF or client disconnected
                        }
                        remaining -= written as usize;
                    }
                    set_blocking(socket_fd, false)?;
                    Ok::<_, std::io::Error>(())
                })
                .await??;
            }
            Conn::Tls(_) => {
                // Read the region into memory off the async runtime, then write
                // it through the TLS stream.
                let data = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
                    let mut f = file;
                    f.seek(SeekFrom::Start(position))?;
                    let mut buf = vec![0u8; bytes as usize];
                    f.read_exact(&mut buf)?;
                    Ok(buf)
                })
                .await??;
                self.write_all(&data).await?;
            }
        }
        Ok(())
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Conn::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Conn::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_flush(cx),
            Conn::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Conn::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

async fn handle_connection(
    mut stream: Conn,
    log_manager: Arc<LogManager>,
    auth_token: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut header = [0u8; 7];

    // Connections start authenticated only when no token is configured.
    let mut authenticated = auth_token.is_none();

    loop {
        // Read Request Header: [magic (2 bytes)] [cmd (1 byte)] [body_len (4 bytes)]
        match stream.read_exact(&mut header).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("[AeroMQ Broker] Client closed connection");
                break;
            }
            Err(e) => return Err(e.into()),
        }

        if !is_valid_magic(header[0], header[1]) {
            return Err("Invalid protocol magic bytes".into());
        }

        let cmd = header[2];
        let body_len = u32::from_be_bytes(header[3..7].try_into().unwrap()) as usize;

        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).await?;

        // Command 0: AUTH handshake. Body is the raw bearer token.
        if cmd == 0 {
            let provided = String::from_utf8_lossy(&body);
            let ok = match &auth_token {
                Some(expected) => provided == *expected,
                None => true, // no auth required; accept any token
            };
            authenticated = ok;
            // Response: [magic (2)] [status (1: 0=ok, 3=auth failed)]
            let status = if ok { 0u8 } else { 3u8 };
            stream.write_all(&[0xAE, 0x01, status]).await?;
            if !ok {
                warn!("[AeroMQ Broker] Rejected connection: invalid auth token");
                break;
            }
            continue;
        }

        // All other commands require authentication.
        if !authenticated {
            warn!("[AeroMQ Broker] Rejected unauthenticated command {}", cmd);
            stream.write_all(&[0xAE, 0x01, 3]).await?; // status 3 = auth failed
            break;
        }

        match cmd {
            1 => {
                // Command 1: Produce/Write
                let req = parse_produce_body(&body)?;

                // Append to partition log
                let part_log = log_manager.get_partition(&req.topic, req.partition).await?;
                let mut log_guard = part_log.lock().await;

                // When producer_id >= 0 and base_sequence >= 0, check ProducerStateTracker
                if let Some((producer_id, epoch, base_sequence, record_count)) =
                    crate::kafka::handlers::extract_batch_producer_info(req.payload)
                {
                    if producer_id >= 0 && base_sequence >= 0 {
                        let next_off = log_guard.next_offset;
                        match log_guard.producer_tracker.check_and_update_sequence(
                            producer_id,
                            epoch,
                            base_sequence,
                            record_count,
                            next_off,
                        ) {
                            crate::log::producer_state::SequenceCheckResult::Duplicate { last_offset } => {
                                // On Duplicate: bypass log append and return success with previous offset
                                stream.write_all(&encode_produce_ack(last_offset)).await?;
                                continue;
                            }
                            crate::log::producer_state::SequenceCheckResult::OutOfOrder { .. } => {
                                // Status 45 = OutOfOrderSequenceNumber
                                stream.write_all(&[0xAE, 0x01, 45]).await?;
                                continue;
                            }
                            crate::log::producer_state::SequenceCheckResult::ValidNext => {}
                        }
                    }
                }

                let offset = log_guard.append(req.payload)?;

                // Send Response: [magic (2)] [status (1: 0=Success)] [offset (8)]
                stream.write_all(&encode_produce_ack(offset)).await?;
            }
            2 => {
                // Command 2: Fetch/Read (Consumer fetch, respects High-Watermark)
                let req = parse_fetch_body(&body)?;

                // Read from partition log with High-Watermark blocking
                let part_log = log_manager.get_partition(&req.topic, req.partition).await?;

                let mut attempts = 0;
                let data_opt = loop {
                    let mut log_guard = part_log.lock().await;
                    let hw = log_guard.high_watermark;

                    if req.start_offset < hw {
                        let res = log_guard.read_from_offset(req.start_offset, req.max_bytes)?;
                        break res;
                    }

                    drop(log_guard);
                    attempts += 1;
                    if attempts >= 10 { // max 1 second wait (10 * 100ms)
                        break None;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                };

                match data_opt {
                    Some((file, position, bytes_to_read)) => {
                        // Send header indicating success with data
                        stream.write_all(&encode_data_header(bytes_to_read)).await?;
                        stream.send_file_region(file, position, bytes_to_read).await?;
                    }
                    None => {
                        // Send header indicating no data (status = 1)
                        stream.write_all(&encode_empty_response()).await?;
                    }
                }
            }
            3 => {
                // Command 3: Replica Fetch/Sync (Replica fetch, bypasses High-Watermark)
                let req = parse_replica_fetch_body(&body)?;

                // Read from partition log (replicate/sync has no HW limit)
                let part_log = log_manager.get_partition(&req.topic, req.partition).await?;
                let mut log_guard = part_log.lock().await;

                // Record replica progress
                log_guard.update_follower_offset(req.replica_id, req.start_offset);

                let data_opt = log_guard.read_from_offset(req.start_offset, req.max_bytes)?;
                drop(log_guard);

                match data_opt {
                    Some((file, position, bytes_to_read)) => {
                        stream.write_all(&encode_data_header(bytes_to_read)).await?;
                        stream.send_file_region(file, position, bytes_to_read).await?;
                    }
                    None => {
                        stream.write_all(&encode_empty_response()).await?;
                    }
                }
            }
            _ => return Err(format!("Unknown protocol command: {}", cmd).into()),
        }
    }

    Ok(())
}

/// Parsed fields of a Produce (cmd=1) request body:
/// `[topic_len(2)][topic][partition(4)][payload_len(4)][payload]`
#[derive(Debug)]
struct ProduceRequest<'a> {
    topic: String,
    partition: u32,
    payload: &'a [u8],
}

fn parse_produce_body(body: &[u8]) -> Result<ProduceRequest<'_>, Box<dyn std::error::Error>> {
    if body.len() < 10 {
        return Err("Produce body too short".into());
    }
    let topic_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    if body.len() < 10 + topic_len {
        return Err("Produce topic length exceeds body size".into());
    }
    let topic = String::from_utf8(body[2..2 + topic_len].to_vec())?;
    let partition = u32::from_be_bytes(body[2 + topic_len..6 + topic_len].try_into().unwrap());
    let payload_len = u32::from_be_bytes(body[6 + topic_len..10 + topic_len].try_into().unwrap()) as usize;

    if body.len() < 10 + topic_len + payload_len {
        return Err("Produce payload length mismatch".into());
    }
    let payload = &body[10 + topic_len..10 + topic_len + payload_len];

    Ok(ProduceRequest { topic, partition, payload })
}

/// Parsed fields of a Fetch (cmd=2) request body:
/// `[topic_len(2)][topic][partition(4)][start_offset(8)][max_bytes(4)]`
#[derive(Debug)]
struct FetchRequest {
    topic: String,
    partition: u32,
    start_offset: u64,
    max_bytes: u32,
}

fn parse_fetch_body(body: &[u8]) -> Result<FetchRequest, Box<dyn std::error::Error>> {
    if body.len() < 18 {
        return Err("Fetch body too short".into());
    }
    let topic_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    if body.len() < 18 + topic_len {
        return Err("Fetch topic length exceeds body size".into());
    }
    let topic = String::from_utf8(body[2..2 + topic_len].to_vec())?;
    let partition = u32::from_be_bytes(body[2 + topic_len..6 + topic_len].try_into().unwrap());
    let start_offset = u64::from_be_bytes(body[6 + topic_len..14 + topic_len].try_into().unwrap());
    let max_bytes = u32::from_be_bytes(body[14 + topic_len..18 + topic_len].try_into().unwrap());

    Ok(FetchRequest { topic, partition, start_offset, max_bytes })
}

/// Parsed fields of a Replica Fetch (cmd=3) request body:
/// `[replica_id(4)][topic_len(2)][topic][partition(4)][start_offset(8)][max_bytes(4)]`
#[derive(Debug)]
struct ReplicaFetchRequest {
    replica_id: u32,
    topic: String,
    partition: u32,
    start_offset: u64,
    max_bytes: u32,
}

fn parse_replica_fetch_body(body: &[u8]) -> Result<ReplicaFetchRequest, Box<dyn std::error::Error>> {
    if body.len() < 22 {
        return Err("Replica Fetch body too short".into());
    }
    let replica_id = u32::from_be_bytes(body[0..4].try_into().unwrap());
    let topic_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
    if body.len() < 22 + topic_len {
        return Err("Replica Fetch topic length exceeds body size".into());
    }
    let topic = String::from_utf8(body[6..6 + topic_len].to_vec())?;
    let partition = u32::from_be_bytes(body[6 + topic_len..10 + topic_len].try_into().unwrap());
    let start_offset = u64::from_be_bytes(body[10 + topic_len..18 + topic_len].try_into().unwrap());
    let max_bytes = u32::from_be_bytes(body[18 + topic_len..22 + topic_len].try_into().unwrap());

    Ok(ReplicaFetchRequest { replica_id, topic, partition, start_offset, max_bytes })
}

/// Encode a successful Produce response: `[magic(2)][status=0][offset(8)]`.
fn encode_produce_ack(offset: u64) -> Vec<u8> {
    let mut resp = Vec::with_capacity(11);
    resp.extend_from_slice(&[0xAE, 0x01]);
    resp.push(0); // success
    resp.extend_from_slice(&offset.to_be_bytes());
    resp
}

/// Encode a "data follows" Fetch/ReplicaFetch response header:
/// `[magic(2)][status=2][bytes_to_read(4)]`.
fn encode_data_header(bytes_to_read: u32) -> Vec<u8> {
    let mut resp = Vec::with_capacity(7);
    resp.extend_from_slice(&[0xAE, 0x01]);
    resp.push(2); // Success with Data
    resp.extend_from_slice(&bytes_to_read.to_be_bytes());
    resp
}

/// Encode an empty-result Fetch/ReplicaFetch response: `[magic(2)][status=1]`.
fn encode_empty_response() -> [u8; 3] {
    [0xAE, 0x01, 1]
}

/// Validate the 2-byte protocol magic prefix of a request header.
fn is_valid_magic(b0: u8, b1: u8) -> bool {
    b0 == 0xAE && b1 == 0x01
}

fn set_blocking(fd: std::os::unix::io::RawFd, blocking: bool) -> std::io::Result<()> {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        if flags < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let new_flags = if blocking {
            flags & !libc::O_NONBLOCK
        } else {
            flags | libc::O_NONBLOCK
        };
        if libc::fcntl(fd, libc::F_SETFL, new_flags) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_produce_body(topic: &str, partition: u32, payload: &[u8]) -> Vec<u8> {
        let topic_bytes = topic.as_bytes();
        let mut body = Vec::new();
        body.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        body.extend_from_slice(topic_bytes);
        body.extend_from_slice(&partition.to_be_bytes());
        body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        body.extend_from_slice(payload);
        body
    }

    fn encode_fetch_body(topic: &str, partition: u32, start_offset: u64, max_bytes: u32) -> Vec<u8> {
        let topic_bytes = topic.as_bytes();
        let mut body = Vec::new();
        body.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        body.extend_from_slice(topic_bytes);
        body.extend_from_slice(&partition.to_be_bytes());
        body.extend_from_slice(&start_offset.to_be_bytes());
        body.extend_from_slice(&max_bytes.to_be_bytes());
        body
    }

    fn encode_replica_fetch_body(
        replica_id: u32,
        topic: &str,
        partition: u32,
        start_offset: u64,
        max_bytes: u32,
    ) -> Vec<u8> {
        let topic_bytes = topic.as_bytes();
        let mut body = Vec::new();
        body.extend_from_slice(&replica_id.to_be_bytes());
        body.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        body.extend_from_slice(topic_bytes);
        body.extend_from_slice(&partition.to_be_bytes());
        body.extend_from_slice(&start_offset.to_be_bytes());
        body.extend_from_slice(&max_bytes.to_be_bytes());
        body
    }

    #[test]
    fn test_is_valid_magic() {
        assert!(is_valid_magic(0xAE, 0x01));
        assert!(!is_valid_magic(0xAE, 0x02));
        assert!(!is_valid_magic(0x00, 0x01));
        assert!(!is_valid_magic(0, 0));
    }

    #[test]
    fn test_parse_produce_body_round_trip() {
        let body = encode_produce_body("orders", 3, b"hello world");
        let req = parse_produce_body(&body).expect("should parse valid produce body");
        assert_eq!(req.topic, "orders");
        assert_eq!(req.partition, 3);
        assert_eq!(req.payload, b"hello world");
    }

    #[test]
    fn test_parse_produce_body_empty_payload() {
        let body = encode_produce_body("t", 0, b"");
        let req = parse_produce_body(&body).expect("should parse empty payload");
        assert_eq!(req.topic, "t");
        assert!(req.payload.is_empty());
    }

    #[test]
    fn test_parse_produce_body_too_short() {
        let body = vec![0u8; 5];
        let err = parse_produce_body(&body).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn test_parse_produce_body_topic_len_exceeds_body() {
        // Claims a 100-byte topic name but the body is far shorter.
        let mut body = vec![0u8; 10];
        body[0..2].copy_from_slice(&100u16.to_be_bytes());
        let err = parse_produce_body(&body).unwrap_err();
        assert!(err.to_string().contains("topic length exceeds"));
    }

    #[test]
    fn test_parse_produce_body_payload_len_mismatch() {
        let topic_bytes = b"t";
        let mut body = Vec::new();
        body.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        body.extend_from_slice(topic_bytes);
        body.extend_from_slice(&0u32.to_be_bytes()); // partition
        body.extend_from_slice(&1000u32.to_be_bytes()); // claims 1000-byte payload
        // but no payload bytes actually follow
        let err = parse_produce_body(&body).unwrap_err();
        assert!(err.to_string().contains("payload length mismatch"));
    }

    #[test]
    fn test_parse_produce_body_invalid_utf8_topic() {
        let mut body = Vec::new();
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes());
        assert!(parse_produce_body(&body).is_err());
    }

    #[test]
    fn test_parse_fetch_body_round_trip() {
        let body = encode_fetch_body("clicks", 7, 1234, 65536);
        let req = parse_fetch_body(&body).expect("should parse valid fetch body");
        assert_eq!(req.topic, "clicks");
        assert_eq!(req.partition, 7);
        assert_eq!(req.start_offset, 1234);
        assert_eq!(req.max_bytes, 65536);
    }

    #[test]
    fn test_parse_fetch_body_too_short() {
        let body = vec![0u8; 4];
        let err = parse_fetch_body(&body).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn test_parse_fetch_body_topic_len_exceeds_body() {
        let mut body = vec![0u8; 18];
        body[0..2].copy_from_slice(&255u16.to_be_bytes());
        let err = parse_fetch_body(&body).unwrap_err();
        assert!(err.to_string().contains("topic length exceeds"));
    }

    #[test]
    fn test_parse_replica_fetch_body_round_trip() {
        let body = encode_replica_fetch_body(42, "events", 1, 9999, 4096);
        let req = parse_replica_fetch_body(&body).expect("should parse valid replica fetch body");
        assert_eq!(req.replica_id, 42);
        assert_eq!(req.topic, "events");
        assert_eq!(req.partition, 1);
        assert_eq!(req.start_offset, 9999);
        assert_eq!(req.max_bytes, 4096);
    }

    #[test]
    fn test_parse_replica_fetch_body_too_short() {
        let body = vec![0u8; 10];
        let err = parse_replica_fetch_body(&body).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn test_parse_replica_fetch_body_topic_len_exceeds_body() {
        let mut body = vec![0u8; 22];
        body[4..6].copy_from_slice(&255u16.to_be_bytes());
        let err = parse_replica_fetch_body(&body).unwrap_err();
        assert!(err.to_string().contains("topic length exceeds"));
    }

    #[test]
    fn test_encode_produce_ack_format() {
        let resp = encode_produce_ack(999);
        assert_eq!(resp[0], 0xAE);
        assert_eq!(resp[1], 0x01);
        assert_eq!(resp[2], 0); // success status
        assert_eq!(u64::from_be_bytes(resp[3..11].try_into().unwrap()), 999);
        assert_eq!(resp.len(), 11);
    }

    #[test]
    fn test_encode_data_header_format() {
        let resp = encode_data_header(2048);
        assert_eq!(resp[0], 0xAE);
        assert_eq!(resp[1], 0x01);
        assert_eq!(resp[2], 2); // data status
        assert_eq!(u32::from_be_bytes(resp[3..7].try_into().unwrap()), 2048);
        assert_eq!(resp.len(), 7);
    }

    #[test]
    fn test_encode_empty_response_format() {
        let resp = encode_empty_response();
        assert_eq!(resp, [0xAE, 0x01, 1]);
    }
}
