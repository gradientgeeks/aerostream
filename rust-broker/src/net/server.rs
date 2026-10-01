use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;
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

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let std_listener = std::net::TcpListener::bind(self.addr)?;
        std_listener.set_nonblocking(true)?;
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = std_listener.as_raw_fd();
            unsafe {
                let buf_size: libc::c_int = 8 * 1024 * 1024;
                libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &buf_size as *const _ as *const libc::c_void, std::mem::size_of_val(&buf_size) as libc::socklen_t);
                libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &buf_size as *const _ as *const libc::c_void, std::mem::size_of_val(&buf_size) as libc::socklen_t);
            }
        }
        let listener = TcpListener::from_std(std_listener)?;

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
            let _ = stream.set_nodelay(true);
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                let fd = stream.as_raw_fd();
                unsafe {
                    let buf_size: libc::c_int = 8 * 1024 * 1024;
                    libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &buf_size as *const _ as *const libc::c_void, std::mem::size_of_val(&buf_size) as libc::socklen_t);
                    libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &buf_size as *const _ as *const libc::c_void, std::mem::size_of_val(&buf_size) as libc::socklen_t);
                    let quickack: libc::c_int = 1;
                    libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_QUICKACK, &quickack as *const _ as *const libc::c_void, std::mem::size_of_val(&quickack) as libc::socklen_t);
                }
            }
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

/// Build a TLS acceptor from the broker's configured cert/key. When `tls.require_client_cert` is
/// set, this is mutual TLS: peer brokers must present a certificate signed by `tls.ca_file`.
fn build_tls_acceptor(cfg: &BrokerConfig) -> Result<TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
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

    crate::net::tls::build_acceptor(
        cert_path,
        key_path,
        cfg.tls.ca_file.as_deref(),
        cfg.tls.require_client_cert,
    )
}

/// A client connection that is either plaintext (supports `sendfile` zero-copy)
/// or TLS (encryption forces a buffered copy through userspace).
enum Conn {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl Conn {
    /// Transfers `bytes` from `file` starting at `position` to socket.
    /// Uses `sendfile(2)` for plaintext and buffered encryption for TLS.
    async fn send_file_region(
        &mut self,
        file: File,
        position: u64,
        bytes: u32,
    ) -> io::Result<()> {
        match self {
            Conn::Plain(stream) if region_is_page_cached(file.as_raw_fd(), position, bytes) => {
                // Hot data (the normal case for tailing consumers): non-blocking sendfile driven by socket writability.
                // No blocking-pool hop and no fcntl(2) calls to flip the socket between blocking and non-blocking.
                let (socket_fd, file_fd) = (stream.as_raw_fd(), file.as_raw_fd());
                let mut offset = position as libc::off_t;
                let mut remaining = bytes as usize;
                while remaining > 0 {
                    stream.writable().await?;
                    let sent = stream.try_io(tokio::io::Interest::WRITABLE, || {
                        // SAFETY: both fds are open for the duration of the call (`file` is held until the end of this arm).
                        let n = unsafe { libc::sendfile(socket_fd, file_fd, &mut offset, remaining) };
                        if n < 0 { Err(io::Error::last_os_error()) } else { Ok(n as usize) }
                    });
                    match sent {
                        Ok(0) => break, // EOF or client disconnected
                        Ok(n) => remaining -= n,
                        Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => continue,
                        Err(e) => return Err(e),
                    }
                }
                drop(file);
            }
            Conn::Plain(stream) => {
                // Possibly cold data: a disk read may block, so do it on the blocking pool (old path).
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

impl Conn {
    /// Sends `header` followed by the file region. On plaintext connections the header is sent with `MSG_MORE`, so the
    /// kernel coalesces it with the first `sendfile` chunk into one segment instead of a tiny packet of its own.
    async fn send_with_file(&mut self, header: &[u8], file: File, position: u64, bytes: u32) -> io::Result<()> {
        match self {
            Conn::Plain(stream) if bytes > 0 => send_more(stream, header).await?,
            _ => self.write_all(header).await?,
        }
        self.send_file_region(file, position, bytes).await
    }
}

/// Writes all of `buf` with `MSG_MORE` (more data follows immediately), without blocking the runtime.
async fn send_more(stream: &TcpStream, mut buf: &[u8]) -> io::Result<()> {
    let fd = stream.as_raw_fd();
    while !buf.is_empty() {
        stream.writable().await?;
        let sent = stream.try_io(tokio::io::Interest::WRITABLE, || {
            // SAFETY: `buf` is a valid slice and `fd` is an open socket for the duration of the call.
            let n = unsafe { libc::send(fd, buf.as_ptr() as *const libc::c_void, buf.len(), libc::MSG_MORE | libc::MSG_NOSIGNAL) };
            if n < 0 { Err(io::Error::last_os_error()) } else { Ok(n as usize) }
        });
        match sent {
            Ok(n) => buf = &buf[n..],
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// True if reading the region would not block on disk: probes the first byte, the last byte and one byte per 4 MiB with
/// `preadv2(RWF_NOWAIT)`, which fails with EAGAIN when a page is not in the page cache. Any failure (old kernel,
/// unsupported filesystem) answers false, which just selects the blocking-pool path.
fn region_is_page_cached(fd: std::os::unix::io::RawFd, position: u64, bytes: u32) -> bool {
    const RWF_NOWAIT: libc::c_int = 0x8;
    if bytes == 0 {
        return true;
    }
    let probe = |offset: u64| -> bool {
        let mut b = [0u8; 1];
        let iov = libc::iovec { iov_base: b.as_mut_ptr() as *mut libc::c_void, iov_len: 1 };
        // SAFETY: one valid 1-byte iovec into a live stack buffer.
        unsafe { libc::preadv2(fd, &iov, 1, offset as libc::off_t, RWF_NOWAIT) >= 0 }
    };
    let end = position + bytes as u64;
    let mut off = position;
    while off < end {
        if !probe(off) {
            return false;
        }
        off += 4 << 20;
    }
    probe(end - 1)
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
    // Connections start authenticated only when no token is configured.
    let mut authenticated = auth_token.is_none();

    // Buffered input: one read syscall can carry many pipelined frames (the native driver keeps up to 1,024 requests in
    // flight per connection), instead of two reads (header, body) per message.
    let mut rbuf = BytesMut::with_capacity(READ_CHUNK);
    // Produce frames parsed from the buffer but not yet appended, and the acks waiting to be written.
    let mut pending: Vec<ProduceFrame> = Vec::new();
    let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);

    // Fast-path partition cache for repeated writes to the same topic-partition
    let mut cached_partition: CachedPartition = None;

    'conn: loop {
        // Serve every complete frame that is already buffered.
        while let Some((cmd, body)) = take_frame(&mut rbuf)? {
            // Command 0: AUTH handshake. Body is the raw bearer token.
            if cmd == 0 {
                answer_produce(&mut stream, &mut pending, &log_manager, &mut cached_partition, &mut out).await?;
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
                    break 'conn;
                }
                continue;
            }

            // All other commands require authentication.
            if !authenticated {
                warn!("[AeroMQ Broker] Rejected unauthenticated command {}", cmd);
                stream.write_all(&[0xAE, 0x01, 3]).await?; // status 3 = auth failed
                break 'conn;
            }

            if cmd == 1 {
                // Command 1: Produce/Write. Collected and appended together once the buffered frames are parsed.
                match parse_produce_frame(&body) {
                    Ok(frame) => pending.push(frame),
                    Err(e) => {
                        // Answer the frames that preceded the malformed one, then close, as sequential handling did.
                        answer_produce(&mut stream, &mut pending, &log_manager, &mut cached_partition, &mut out).await?;
                        return Err(e.into());
                    }
                }
                continue;
            }

            // Every other command is answered in request order, so the produce acks that precede it go out first.
            answer_produce(&mut stream, &mut pending, &log_manager, &mut cached_partition, &mut out).await?;

            match cmd {
            2 => {
                // Command 2: Fetch/Read (Consumer fetch, respects High-Watermark)
                let req = parse_fetch_body(&body)?;

                // Read from partition log with High-Watermark blocking
                let part_log = match &cached_partition {
                    Some(((top, part), log)) if top.as_str() == req.topic && *part == req.partition => {
                        log.clone()
                    }
                    _ => {
                        let log = log_manager.get_partition(req.topic, req.partition).await?;
                        cached_partition = Some(((req.topic.to_string(), req.partition), log.clone()));
                        log
                    }
                };

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
                        // Header indicating success with data, then the data
                        stream.send_with_file(&encode_data_header(bytes_to_read), file, position, bytes_to_read).await?;
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
                let part_log = match &cached_partition {
                    Some(((top, part), log)) if top.as_str() == req.topic && *part == req.partition => {
                        log.clone()
                    }
                    _ => {
                        let log = log_manager.get_partition(req.topic, req.partition).await?;
                        cached_partition = Some(((req.topic.to_string(), req.partition), log.clone()));
                        log
                    }
                };
                let mut log_guard = part_log.lock().await;

                // Record replica progress
                log_guard.update_follower_offset(req.replica_id, req.start_offset);

                let data_opt = log_guard.read_from_offset(req.start_offset, req.max_bytes)?;
                drop(log_guard);

                match data_opt {
                    Some((file, position, bytes_to_read)) => {
                        stream.send_with_file(&encode_data_header(bytes_to_read), file, position, bytes_to_read).await?;
                    }
                    None => {
                        stream.write_all(&encode_empty_response()).await?;
                    }
                }
            }
            4 => {
                // Command 4: Multi-entry Fetch with long polling (consumer, respects High-Watermark).
                // Returns every whole entry from start_offset up to the high watermark within max_bytes (at least
                // one), waiting up to max_wait_ms for data instead of re-polling.
                let req = parse_fetch_multi_body(&body)?;
                let part_log = match &cached_partition {
                    Some(((top, part), log)) if top.as_str() == req.topic && *part == req.partition => {
                        log.clone()
                    }
                    _ => {
                        let log = log_manager.get_partition(req.topic, req.partition).await?;
                        cached_partition = Some(((req.topic.to_string(), req.partition), log.clone()));
                        log
                    }
                };

                let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_millis(req.max_wait_ms as u64);
                // This partition's own notify: a fetch on one partition must not be woken by appends elsewhere.
                let notify = part_log.lock().await.append_notify.clone().expect("partition always has a notify");
                let data_opt = loop {
                    // Register for the wakeup before checking, so an append between the check and the wait is seen.
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let res = {
                        let mut log_guard = part_log.lock().await;
                        let hw = log_guard.high_watermark;
                        log_guard.read_range_entries(req.start_offset, hw, req.max_bytes.max(1))?
                    };
                    if res.is_some() || tokio::time::Instant::now() >= deadline {
                        break res;
                    }
                    let _ = tokio::time::timeout_at(deadline, notified).await;
                };

                match data_opt {
                    Some((file, position, bytes_to_read, entries)) => {
                        stream.send_with_file(&encode_multi_header(&entries), file, position, bytes_to_read).await?;
                    }
                    None => {
                        stream.write_all(&encode_empty_response()).await?;
                    }
                }
            }
            _ => return Err(format!("Unknown protocol command: {}", cmd).into()),
            }
        }

        // Nothing complete is left in the buffer: append the collected produce frames and write all their acks at once.
        answer_produce(&mut stream, &mut pending, &log_manager, &mut cached_partition, &mut out).await?;

        // Wait for more bytes.
        if rbuf.capacity() - rbuf.len() < 4096 {
            rbuf.reserve(READ_CHUNK);
        }
        let n = stream.read_buf(&mut rbuf).await?;
        if n == 0 {
            if rbuf.is_empty() {
                debug!("[AeroMQ Broker] Client closed connection");
                break;
            }
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed in the middle of a frame").into());
        }
    }

    Ok(())
}

/// Input buffer growth step for a connection.
const READ_CHUNK: usize = 256 * 1024;
/// Largest request body accepted (a corrupt or hostile length field must not make the broker allocate gigabytes).
const MAX_FRAME_BYTES: usize = 512 * 1024 * 1024;

type CachedPartition = Option<((String, u32), Arc<tokio::sync::Mutex<crate::log::manager::PartitionLog>>)>;

/// One parsed Produce frame. `topic` and `payload` are refcounted slices of the received bytes (no copies).
struct ProduceFrame {
    topic: Bytes,
    partition: u32,
    payload: Bytes,
}

/// Removes one complete request frame `[magic(2)][cmd(1)][body_len(4)][body]` from the front of `rbuf`, if a whole frame is
/// buffered, and returns `(cmd, body)`.
fn take_frame(rbuf: &mut BytesMut) -> io::Result<Option<(u8, Bytes)>> {
    if rbuf.len() < 7 {
        return Ok(None);
    }
    if !is_valid_magic(rbuf[0], rbuf[1]) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid protocol magic bytes"));
    }
    let cmd = rbuf[2];
    let body_len = u32::from_be_bytes(rbuf[3..7].try_into().unwrap()) as usize;
    if body_len > MAX_FRAME_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("request body of {body_len} bytes exceeds the {MAX_FRAME_BYTES} byte limit")));
    }
    let total = 7 + body_len;
    if rbuf.len() < total {
        rbuf.reserve(total - rbuf.len());
        return Ok(None);
    }
    let mut frame = rbuf.split_to(total);
    frame.advance(7);
    Ok(Some((cmd, frame.freeze())))
}

/// Same validation as `parse_produce_body`, but returns refcounted slices so the frame can be queued without copying.
fn parse_produce_frame(body: &Bytes) -> io::Result<ProduceFrame> {
    let invalid = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
    if body.len() < 10 {
        return Err(invalid("Produce body too short"));
    }
    let topic_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    if body.len() < 10 + topic_len {
        return Err(invalid("Produce topic length exceeds body size"));
    }
    std::str::from_utf8(&body[2..2 + topic_len]).map_err(|e| invalid(&e.to_string()))?;
    let partition = u32::from_be_bytes(body[2 + topic_len..6 + topic_len].try_into().unwrap());
    let payload_len = u32::from_be_bytes(body[6 + topic_len..10 + topic_len].try_into().unwrap()) as usize;
    if body.len() < 10 + topic_len + payload_len {
        return Err(invalid("Produce payload length mismatch"));
    }
    Ok(ProduceFrame {
        topic: body.slice(2..2 + topic_len),
        partition,
        payload: body.slice(10 + topic_len..10 + topic_len + payload_len),
    })
}

/// Appends the queued produce frames, then writes their acks (in request order) with one write. Acks for frames that were
/// completed before an append error are still written before the error is returned.
async fn answer_produce(
    stream: &mut Conn,
    pending: &mut Vec<ProduceFrame>,
    log_manager: &Arc<LogManager>,
    cached: &mut CachedPartition,
    out: &mut Vec<u8>,
) -> io::Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let res = process_produce(pending, log_manager, cached, out).await;
    pending.clear();
    let written = if out.is_empty() { Ok(()) } else { stream.write_all(out).await };
    out.clear();
    res?;
    written
}

async fn process_produce(
    pending: &[ProduceFrame],
    log_manager: &Arc<LogManager>,
    cached: &mut CachedPartition,
    out: &mut Vec<u8>,
) -> io::Result<()> {
    let mut i = 0;
    while i < pending.len() {
        // A run of consecutive frames for the same topic-partition shares one lock acquisition.
        let (topic, partition) = (&pending[i].topic, pending[i].partition);
        let mut j = i + 1;
        while j < pending.len() && pending[j].partition == partition && pending[j].topic == *topic {
            j += 1;
        }
        let topic_str = std::str::from_utf8(topic).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let part_log = match cached {
            Some(((t, p), log)) if t.as_str() == topic_str && *p == partition => log.clone(),
            _ => {
                let log = log_manager.get_partition(topic_str, partition).await?;
                *cached = Some(((topic_str.to_string(), partition), log.clone()));
                log
            }
        };
        let mut log_guard = part_log.lock().await;

        // Frames without producer-sequence state are appended together; a frame that carries a producer id and sequence
        // is checked against the producer state first, after everything before it has been appended (so its check sees
        // the right next offset), exactly as handling the frames one by one did.
        let mut plain: Vec<&[u8]> = Vec::new();
        for frame in &pending[i..j] {
            let tracked = match crate::kafka::handlers::extract_batch_producer_info(&frame.payload) {
                Some((producer_id, epoch, base_sequence, record_count)) if producer_id >= 0 && base_sequence >= 0 => {
                    Some((producer_id, epoch, base_sequence, record_count))
                }
                _ => None,
            };
            match tracked {
                None => plain.push(&frame.payload),
                Some((producer_id, epoch, base_sequence, record_count)) => {
                    append_plain(&mut log_guard, &mut plain, out)?;
                    let next_off = log_guard.next_offset;
                    match log_guard.producer_tracker.check_and_update_sequence(producer_id, epoch, base_sequence, record_count, next_off) {
                        crate::log::producer_state::SequenceCheckResult::Duplicate { last_offset } => {
                            // Duplicate: skip the append and ack the previous offset.
                            out.extend_from_slice(&encode_produce_ack(last_offset));
                        }
                        crate::log::producer_state::SequenceCheckResult::OutOfOrder { .. } => {
                            // Status 45 = OutOfOrderSequenceNumber
                            out.extend_from_slice(&[0xAE, 0x01, 45]);
                        }
                        crate::log::producer_state::SequenceCheckResult::ValidNext => {
                            let offset = log_guard.append(&frame.payload)?;
                            out.extend_from_slice(&encode_produce_ack(offset));
                        }
                    }
                }
            }
        }
        append_plain(&mut log_guard, &mut plain, out)?;
        // The lock is released here, before the acks are written to the network.
        drop(log_guard);
        i = j;
    }
    Ok(())
}

/// Appends `plain` as consecutive log entries with one batched write and queues one ack per entry.
fn append_plain(
    log: &mut crate::log::manager::PartitionLog,
    plain: &mut Vec<&[u8]>,
    out: &mut Vec<u8>,
) -> io::Result<()> {
    if plain.is_empty() {
        return Ok(());
    }
    let first = log.append_entries(plain)?;
    for k in 0..plain.len() as u64 {
        out.extend_from_slice(&encode_produce_ack(first + k));
    }
    plain.clear();
    Ok(())
}

/// Parsed fields of a Produce (cmd=1) request body:
/// `[topic_len(2)][topic][partition(4)][payload_len(4)][payload]`
#[derive(Debug)]
struct ProduceRequest<'a> {
    topic: &'a str,
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
    let topic = std::str::from_utf8(&body[2..2 + topic_len])?;
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
struct FetchRequest<'a> {
    topic: &'a str,
    partition: u32,
    start_offset: u64,
    max_bytes: u32,
}

fn parse_fetch_body(body: &[u8]) -> Result<FetchRequest<'_>, Box<dyn std::error::Error>> {
    if body.len() < 18 {
        return Err("Fetch body too short".into());
    }
    let topic_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    if body.len() < 18 + topic_len {
        return Err("Fetch topic length exceeds body size".into());
    }
    let topic = std::str::from_utf8(&body[2..2 + topic_len])?;
    let partition = u32::from_be_bytes(body[2 + topic_len..6 + topic_len].try_into().unwrap());
    let start_offset = u64::from_be_bytes(body[6 + topic_len..14 + topic_len].try_into().unwrap());
    let max_bytes = u32::from_be_bytes(body[14 + topic_len..18 + topic_len].try_into().unwrap());

    Ok(FetchRequest { topic, partition, start_offset, max_bytes })
}

/// Parsed fields of a Multi Fetch (cmd=4) request body:
/// `[topic_len(2)][topic][partition(4)][start_offset(8)][max_bytes(4)][max_wait_ms(4)]`
#[derive(Debug)]
struct FetchMultiRequest<'a> {
    topic: &'a str,
    partition: u32,
    start_offset: u64,
    max_bytes: u32,
    max_wait_ms: u32,
}

fn parse_fetch_multi_body(body: &[u8]) -> Result<FetchMultiRequest<'_>, Box<dyn std::error::Error>> {
    if body.len() < 22 {
        return Err("Multi Fetch body too short".into());
    }
    let topic_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
    if body.len() < 22 + topic_len {
        return Err("Multi Fetch topic length exceeds body size".into());
    }
    let topic = std::str::from_utf8(&body[2..2 + topic_len])?;
    let partition = u32::from_be_bytes(body[2 + topic_len..6 + topic_len].try_into().unwrap());
    let start_offset = u64::from_be_bytes(body[6 + topic_len..14 + topic_len].try_into().unwrap());
    let max_bytes = u32::from_be_bytes(body[14 + topic_len..18 + topic_len].try_into().unwrap());
    let max_wait_ms = u32::from_be_bytes(body[18 + topic_len..22 + topic_len].try_into().unwrap());

    Ok(FetchMultiRequest { topic, partition, start_offset, max_bytes, max_wait_ms })
}

/// Parsed fields of a Replica Fetch (cmd=3) request body:
/// `[replica_id(4)][topic_len(2)][topic][partition(4)][start_offset(8)][max_bytes(4)]`
#[derive(Debug)]
struct ReplicaFetchRequest<'a> {
    replica_id: u32,
    topic: &'a str,
    partition: u32,
    start_offset: u64,
    max_bytes: u32,
}

fn parse_replica_fetch_body(body: &[u8]) -> Result<ReplicaFetchRequest<'_>, Box<dyn std::error::Error>> {
    if body.len() < 22 {
        return Err("Replica Fetch body too short".into());
    }
    let replica_id = u32::from_be_bytes(body[0..4].try_into().unwrap());
    let topic_len = u16::from_be_bytes(body[4..6].try_into().unwrap()) as usize;
    if body.len() < 22 + topic_len {
        return Err("Replica Fetch topic length exceeds body size".into());
    }
    let topic = std::str::from_utf8(&body[6..6 + topic_len])?;
    let partition = u32::from_be_bytes(body[6 + topic_len..10 + topic_len].try_into().unwrap());
    let start_offset = u64::from_be_bytes(body[10 + topic_len..18 + topic_len].try_into().unwrap());
    let max_bytes = u32::from_be_bytes(body[18 + topic_len..22 + topic_len].try_into().unwrap());

    Ok(ReplicaFetchRequest { replica_id, topic, partition, start_offset, max_bytes })
}

/// Encode a successful Produce response: `[magic(2)][status=0][offset(8)]`.
fn encode_produce_ack(offset: u64) -> [u8; 11] {
    let mut resp = [0u8; 11];
    resp[0] = 0xAE;
    resp[1] = 0x01;
    resp[2] = 0x00; // success
    resp[3..11].copy_from_slice(&offset.to_be_bytes());
    resp
}

/// Encode a "data follows" Fetch/ReplicaFetch response header:
/// `[magic(2)][status=2][bytes_to_read(4)]`.
fn encode_data_header(bytes_to_read: u32) -> [u8; 7] {
    let mut resp = [0u8; 7];
    resp[0] = 0xAE;
    resp[1] = 0x01;
    resp[2] = 0x02; // Success with Data
    resp[3..7].copy_from_slice(&bytes_to_read.to_be_bytes());
    resp
}

/// Encode a Multi Fetch (cmd=4) response header; the entry data follows it:
/// `[magic(2)][status=2][count(4)]` then `count` x `[offset(8)][len(4)]`.
fn encode_multi_header(entries: &[(u64, u32)]) -> Vec<u8> {
    let mut resp = Vec::with_capacity(7 + entries.len() * 12);
    resp.extend_from_slice(&[0xAE, 0x01, 0x02]);
    resp.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (offset, len) in entries {
        resp.extend_from_slice(&offset.to_be_bytes());
        resp.extend_from_slice(&len.to_be_bytes());
    }
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
    fn test_parse_fetch_multi_body_round_trip() {
        let mut body = encode_fetch_body("orders", 3, 42, 65536);
        body.extend_from_slice(&500u32.to_be_bytes());
        let req = parse_fetch_multi_body(&body).unwrap();
        assert_eq!((req.topic, req.partition, req.start_offset, req.max_bytes, req.max_wait_ms), ("orders", 3, 42, 65536, 500));
        // a plain Fetch body (no max_wait_ms) is too short
        assert!(parse_fetch_multi_body(&encode_fetch_body("orders", 3, 42, 65536)).is_err());
    }

    #[test]
    fn test_encode_multi_header_format() {
        let h = encode_multi_header(&[(7, 100), (8, 1)]);
        assert_eq!(&h[..7], &[0xAE, 0x01, 0x02, 0, 0, 0, 2]);
        assert_eq!(&h[7..15], &7u64.to_be_bytes());
        assert_eq!(&h[15..19], &100u32.to_be_bytes());
        assert_eq!(&h[19..27], &8u64.to_be_bytes());
        assert_eq!(&h[27..31], &1u32.to_be_bytes());
        assert_eq!(h.len(), 31);
    }

    #[test]
    fn test_encode_empty_response_format() {
        let resp = encode_empty_response();
        assert_eq!(resp, [0xAE, 0x01, 1]);
    }

    /// Regression test ensuring concurrent Produce requests to the same partition
    /// do not serialize socket writes under the partition lock.
    #[tokio::test]
    async fn produce_to_same_partition_on_two_connections_does_not_serialize_on_network_io() {
        let dir = tempfile::tempdir().unwrap();
        let log_manager = Arc::new(LogManager::new(dir.path(), 1).with_limits(1 << 20, None, None));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept loop: hand every connection to the real handler under test, exactly as DataServer::run does.
        let lm = log_manager.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                let lm = lm.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(Conn::Plain(stream), lm, None).await;
                });
            }
        });

        async fn produce_once(addr: SocketAddr, topic: &str) -> u64 {
            let mut conn = TcpStream::connect(addr).await.unwrap();
            let body = encode_produce_body(topic, 0, b"x");
            let mut frame = vec![0xAE, 0x01, 1];
            frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
            frame.extend_from_slice(&body);
            conn.write_all(&frame).await.unwrap();
            let mut resp = [0u8; 11];
            conn.read_exact(&mut resp).await.unwrap();
            assert_eq!(resp[2], 0, "produce ack status");
            u64::from_be_bytes(resp[3..11].try_into().unwrap())
        }

        // 40 interleaved produces per connection to the same partition, both connections racing.
        let a = tokio::spawn(async move {
            let mut offsets = Vec::new();
            for _ in 0..40 {
                offsets.push(produce_once(addr, "shared").await);
            }
            offsets
        });
        let b = tokio::spawn(async move {
            let mut offsets = Vec::new();
            for _ in 0..40 {
                offsets.push(produce_once(addr, "shared").await);
            }
            offsets
        });

        let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(5), async { (a.await.unwrap(), b.await.unwrap()) })
            .await
            .expect("80 interleaved produces to one partition across 2 connections must not deadlock/serialize");

        // Every offset 0..80 was handed out exactly once between the two connections.
        let mut all: Vec<u64> = a.into_iter().chain(b).collect();
        all.sort_unstable();
        assert_eq!(all, (0..80).collect::<Vec<u64>>());
    }


    // ---- end-to-end tests of the buffered / pipelined connection handler -------------------------------------------

    /// Serves connections with the real `handle_connection`, exactly as `DataServer::run` does.
    async fn spawn_test_server(token: Option<String>) -> (SocketAddr, tempfile::TempDir, Arc<LogManager>) {
        let dir = tempfile::tempdir().unwrap();
        let log_manager = Arc::new(LogManager::new(dir.path(), 1).with_limits(1 << 20, None, None));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let lm = log_manager.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let (lm, token) = (lm.clone(), token.clone());
                tokio::spawn(async move {
                    let _ = handle_connection(Conn::Plain(stream), lm, token).await;
                });
            }
        });
        (addr, dir, log_manager)
    }

    fn frame(cmd: u8, body: &[u8]) -> Vec<u8> {
        let mut f = vec![0xAE, 0x01, cmd];
        f.extend_from_slice(&(body.len() as u32).to_be_bytes());
        f.extend_from_slice(body);
        f
    }

    async fn read_ack(conn: &mut TcpStream) -> u64 {
        let mut resp = [0u8; 11];
        conn.read_exact(&mut resp).await.unwrap();
        assert_eq!(&resp[..3], &[0xAE, 0x01, 0], "produce ack header/status");
        u64::from_be_bytes(resp[3..11].try_into().unwrap())
    }

    async fn read_fetch_entry(conn: &mut TcpStream) -> Vec<u8> {
        let mut hdr = [0u8; 7];
        conn.read_exact(&mut hdr).await.unwrap();
        assert_eq!(&hdr[..3], &[0xAE, 0x01, 2], "fetch header with data");
        let mut data = vec![0u8; u32::from_be_bytes(hdr[3..7].try_into().unwrap()) as usize];
        conn.read_exact(&mut data).await.unwrap();
        data
    }

    /// 500 pipelined frames sent in a single write, for two partitions in runs: every ack arrives, in order, with
    /// consecutive offsets per partition, and the stored entries are the ones that were sent.
    #[tokio::test]
    async fn pipelined_produce_acks_in_order_with_consecutive_offsets() {
        let (addr, _dir, _lm) = spawn_test_server(None).await;
        let mut conn = TcpStream::connect(addr).await.unwrap();
        // 200 to partition 0, 100 to partition 1, 200 to partition 0 again
        let plan: Vec<u32> = std::iter::repeat(0).take(200).chain(std::iter::repeat(1).take(100)).chain(std::iter::repeat(0).take(200)).collect();
        let mut wire = Vec::new();
        for (i, p) in plan.iter().enumerate() {
            wire.extend_from_slice(&frame(1, &encode_produce_body("pipe", *p, format!("msg-{i:04}").as_bytes())));
        }
        conn.write_all(&wire).await.unwrap();

        let mut next = [0u64; 2];
        for p in &plan {
            let off = read_ack(&mut conn).await;
            assert_eq!(off, next[*p as usize], "partition {p} offsets must be consecutive");
            next[*p as usize] += 1;
        }
        assert_eq!(next, [400, 100]);

        // entry 250 of partition 0 is the 250th frame sent to partition 0: global index 250 + 100 = 350
        conn.write_all(&frame(2, &encode_fetch_body("pipe", 0, 250, 1 << 20))).await.unwrap();
        let entry = read_fetch_entry(&mut conn).await;
        assert_eq!(entry, b"msg-0350");
    }

    /// A produce and a fetch in the same write are answered in request order: the ack first, then the data.
    #[tokio::test]
    async fn produce_then_fetch_in_one_write_keeps_request_order() {
        let (addr, _dir, _lm) = spawn_test_server(None).await;
        let mut conn = TcpStream::connect(addr).await.unwrap();
        let mut wire = frame(1, &encode_produce_body("order", 0, b"first"));
        wire.extend_from_slice(&frame(1, &encode_produce_body("order", 0, b"second")));
        wire.extend_from_slice(&frame(2, &encode_fetch_body("order", 0, 0, 1 << 20)));
        conn.write_all(&wire).await.unwrap();
        assert_eq!(read_ack(&mut conn).await, 0);
        assert_eq!(read_ack(&mut conn).await, 1);
        assert_eq!(read_fetch_entry(&mut conn).await, b"first");
    }

    /// Frames that arrive in tiny fragments (even split inside the 7-byte header) are reassembled correctly.
    #[tokio::test]
    async fn fragmented_frames_are_reassembled() {
        let (addr, _dir, _lm) = spawn_test_server(None).await;
        let mut conn = TcpStream::connect(addr).await.unwrap();
        conn.set_nodelay(true).unwrap();
        let mut wire = frame(1, &encode_produce_body("frag", 0, b"alpha"));
        wire.extend_from_slice(&frame(1, &encode_produce_body("frag", 0, b"beta")));
        for chunk in wire.chunks(3) {
            conn.write_all(chunk).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert_eq!(read_ack(&mut conn).await, 0);
        assert_eq!(read_ack(&mut conn).await, 1);
    }

    /// Producer-sequence frames mixed with plain frames: the duplicate is acked with the original offset without being
    /// appended, the out-of-order one gets status 45, and plain frames after them continue at the right offset.
    #[tokio::test]
    async fn idempotent_frames_mixed_with_plain_frames_in_one_pipeline() {
        use crate::kafka::handlers::{encode_idempotent_records_batch, KafkaRecord};
        let (addr, _dir, _lm) = spawn_test_server(None).await;
        let mut conn = TcpStream::connect(addr).await.unwrap();
        let rec = KafkaRecord::new(None, Some(b"v".to_vec()));
        let batch = |seq: i32| encode_idempotent_records_batch(0, 7, 0, seq, std::slice::from_ref(&rec));
        let mut wire = frame(1, &encode_produce_body("idem", 0, b"plain-0"));
        wire.extend_from_slice(&frame(1, &encode_produce_body("idem", 0, &batch(0))));   // valid next: appended at offset 1
        wire.extend_from_slice(&frame(1, &encode_produce_body("idem", 0, &batch(0))));   // duplicate: acked with offset 1
        wire.extend_from_slice(&frame(1, &encode_produce_body("idem", 0, &batch(5))));   // sequence gap: status 45
        wire.extend_from_slice(&frame(1, &encode_produce_body("idem", 0, b"plain-2")));  // next plain: offset 2
        conn.write_all(&wire).await.unwrap();
        assert_eq!(read_ack(&mut conn).await, 0);
        assert_eq!(read_ack(&mut conn).await, 1);
        assert_eq!(read_ack(&mut conn).await, 1);
        let mut err = [0u8; 3];
        conn.read_exact(&mut err).await.unwrap();
        assert_eq!(err, [0xAE, 0x01, 45]);
        assert_eq!(read_ack(&mut conn).await, 2);
    }

    /// With an auth token configured, produce is rejected until the AUTH frame is accepted.
    #[tokio::test]
    async fn auth_is_enforced_before_any_buffered_produce() {
        let (addr, _dir, _lm) = spawn_test_server(Some("secret".into())).await;
        let mut bad = TcpStream::connect(addr).await.unwrap();
        bad.write_all(&frame(1, &encode_produce_body("auth", 0, b"x"))).await.unwrap();
        let mut resp = [0u8; 3];
        bad.read_exact(&mut resp).await.unwrap();
        assert_eq!(resp, [0xAE, 0x01, 3]);
        let mut rest = Vec::new();
        assert_eq!(bad.read_to_end(&mut rest).await.unwrap_or(0), 0, "connection is closed after the rejection");

        let mut good = TcpStream::connect(addr).await.unwrap();
        let mut wire = frame(0, b"secret");
        wire.extend_from_slice(&frame(1, &encode_produce_body("auth", 0, b"ok")));
        good.write_all(&wire).await.unwrap();
        let mut a = [0u8; 3];
        good.read_exact(&mut a).await.unwrap();
        assert_eq!(a, [0xAE, 0x01, 0]);
        assert_eq!(read_ack(&mut good).await, 0);
    }

    /// Valid frames that precede a malformed one are answered before the connection is closed; an absurd length field is
    /// rejected without allocating it.
    #[tokio::test]
    async fn malformed_frames_close_the_connection_after_answering_earlier_ones() {
        let (addr, _dir, _lm) = spawn_test_server(None).await;
        let mut conn = TcpStream::connect(addr).await.unwrap();
        let mut wire = frame(1, &encode_produce_body("bad", 0, b"one"));
        wire.extend_from_slice(&frame(1, &encode_produce_body("bad", 0, b"two")));
        wire.extend_from_slice(&frame(1, b"short"));          // malformed produce body
        conn.write_all(&wire).await.unwrap();
        assert_eq!(read_ack(&mut conn).await, 0);
        assert_eq!(read_ack(&mut conn).await, 1);
        let mut rest = Vec::new();
        assert_eq!(conn.read_to_end(&mut rest).await.unwrap_or(0), 0, "closed after the malformed frame");

        let mut huge = TcpStream::connect(addr).await.unwrap();
        let mut hdr = vec![0xAE, 0x01, 1];
        hdr.extend_from_slice(&u32::MAX.to_be_bytes());
        huge.write_all(&hdr).await.unwrap();
        let mut rest = Vec::new();
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), huge.read_to_end(&mut rest)).await
            .expect("the server must close the connection instead of waiting for 4 GiB").unwrap_or(0);
        assert_eq!(n, 0);
    }
}
