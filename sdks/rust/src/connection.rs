use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, trace};

use crate::error::AeroError;
use crate::protocol::{
    decode_multi_entries, decode_produce_response, decode_status_prefix, encode_auth_request,
    encode_fetch_multi_request, encode_fetch_request, encode_produce_request, Record, STATUS_DATA,
    STATUS_EMPTY, STATUS_OK,
};

/// Transport stream wrapping either plain TCP or TLS.
pub enum TransportStream {
    /// Plain unencrypted TCP connection.
    Plain(TcpStream),
    /// TLS encrypted TCP connection.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for TransportStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TransportStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            TransportStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            TransportStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            TransportStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TransportStream::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            TransportStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TransportStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            TransportStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Guard to mark connection closed if an operation is cancelled mid-flight.
struct CancellationGuard<'a> {
    closed: &'a mut bool,
    defused: bool,
}

impl<'a> CancellationGuard<'a> {
    fn new(closed: &'a mut bool) -> Self {
        Self {
            closed,
            defused: false,
        }
    }

    fn defuse(&mut self) {
        self.defused = true;
    }
}

impl<'a> Drop for CancellationGuard<'a> {
    fn drop(&mut self) {
        if !self.defused {
            *self.closed = true;
        }
    }
}

/// Internal state for an established AeroStream connection.
struct ConnectionState {
    stream: TransportStream,
    is_closed: bool,
}

/// Thread-safe, cancellation-safe connection manager for AeroStream brokers.
#[derive(Clone)]
pub struct AeroConnection {
    state: Arc<Mutex<ConnectionState>>,
    broken: Arc<AtomicBool>,
}

impl AeroConnection {
    /// Connect to an AeroStream broker and perform optional authentication.
    pub async fn connect(
        addr: &str,
        token: Option<&str>,
        connect_timeout: Duration,
    ) -> Result<Self, AeroError> {
        debug!(broker = %addr, "Connecting to AeroStream broker");
        let tcp = tokio::time::timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| AeroError::Timeout(format!("Connection to {addr} timed out")))?
            .map_err(AeroError::Io)?;

        tcp.set_nodelay(true)?;

        let mut transport = TransportStream::Plain(tcp);

        // Perform authentication handshake if token is configured
        if let Some(t) = token {
            Self::perform_auth(&mut transport, t).await?;
        }

        let broken = Arc::new(AtomicBool::new(false));
        let state = Arc::new(Mutex::new(ConnectionState {
            stream: transport,
            is_closed: false,
        }));

        Ok(Self { state, broken })
    }

    /// Check if this connection is currently active and healthy.
    pub fn is_alive(&self) -> bool {
        !self.broken.load(Ordering::Acquire)
    }

    /// Mark the connection as broken.
    pub fn mark_broken(&self) {
        self.broken.store(true, Ordering::Release);
    }

    /// Perform Command 0 authentication handshake on a raw stream.
    async fn perform_auth(stream: &mut TransportStream, token: &str) -> Result<(), AeroError> {
        let auth_frame = encode_auth_request(token);
        stream.write_all(&auth_frame).await?;
        stream.flush().await?;

        let mut resp_header = [0u8; 3];
        stream.read_exact(&mut resp_header).await?;

        let status = decode_status_prefix(&resp_header)?;
        if status != STATUS_OK {
            return Err(AeroError::AuthFailed(format!(
                "Handshake rejected with status code: {status}"
            )));
        }
        trace!("Authentication handshake succeeded");
        Ok(())
    }

    /// Send a produce request and receive the assigned offset.
    pub async fn produce(
        &self,
        topic: &str,
        partition: u32,
        payload: &[u8],
    ) -> Result<u64, AeroError> {
        if !self.is_alive() {
            return Err(AeroError::Disconnected);
        }

        let frame = encode_produce_request(topic, partition, payload)?;
        let mut guard = self.state.lock().await;
        if guard.is_closed {
            self.mark_broken();
            return Err(AeroError::Disconnected);
        }

        let ConnectionState { ref mut stream, ref mut is_closed } = *guard;
        let mut cancel_guard = CancellationGuard::new(is_closed);

        stream.write_all(&frame).await?;
        stream.flush().await?;

        let mut status_buf = [0u8; 3];
        stream.read_exact(&mut status_buf).await?;
        let status = decode_status_prefix(&status_buf)?;

        let offset = if status == STATUS_OK {
            let mut offset_buf = [0u8; 8];
            stream.read_exact(&mut offset_buf).await?;
            decode_produce_response(status, Some(&offset_buf))?
        } else {
            decode_produce_response(status, None)?
        };

        cancel_guard.defuse();
        Ok(offset)
    }

    /// Execute Command 2: Consumer Fetch (single/range).
    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        max_bytes: u32,
    ) -> Result<Option<Bytes>, AeroError> {
        if !self.is_alive() {
            return Err(AeroError::Disconnected);
        }

        let frame = encode_fetch_request(topic, partition, start_offset, max_bytes)?;
        let mut guard = self.state.lock().await;
        if guard.is_closed {
            self.mark_broken();
            return Err(AeroError::Disconnected);
        }

        let ConnectionState { ref mut stream, ref mut is_closed } = *guard;
        let mut cancel_guard = CancellationGuard::new(is_closed);

        stream.write_all(&frame).await?;
        stream.flush().await?;

        let mut status_buf = [0u8; 3];
        stream.read_exact(&mut status_buf).await?;
        let status = decode_status_prefix(&status_buf)?;

        let result = match status {
            STATUS_EMPTY => Ok(None),
            STATUS_DATA => {
                let mut len_buf = [0u8; 4];
                stream.read_exact(&mut len_buf).await?;
                let bytes_to_read = u32::from_be_bytes(len_buf) as usize;

                let mut data_buf = BytesMut::with_capacity(bytes_to_read);
                data_buf.resize(bytes_to_read, 0);
                stream.read_exact(&mut data_buf).await?;
                Ok(Some(data_buf.freeze()))
            }
            STATUS_OK => Ok(None),
            other => Err(AeroError::ProtocolError(format!(
                "Fetch failed with unexpected status: {other}"
            ))),
        };

        if result.is_ok() {
            cancel_guard.defuse();
        } else {
            self.mark_broken();
        }
        result
    }

    /// Execute Command 4: Multi-Entry Long-Polling Fetch.
    pub async fn fetch_multi(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    ) -> Result<Vec<Record>, AeroError> {
        if !self.is_alive() {
            return Err(AeroError::Disconnected);
        }

        let frame =
            encode_fetch_multi_request(topic, partition, start_offset, max_bytes, max_wait_ms)?;
        let mut guard = self.state.lock().await;
        if guard.is_closed {
            self.mark_broken();
            return Err(AeroError::Disconnected);
        }

        let ConnectionState { ref mut stream, ref mut is_closed } = *guard;
        let mut cancel_guard = CancellationGuard::new(is_closed);

        stream.write_all(&frame).await?;
        stream.flush().await?;

        let mut status_buf = [0u8; 3];
        stream.read_exact(&mut status_buf).await?;
        let status = decode_status_prefix(&status_buf)?;

        let result = match status {
            STATUS_EMPTY => Ok(Vec::new()),
            STATUS_DATA => {
                let mut count_buf = [0u8; 4];
                stream.read_exact(&mut count_buf).await?;
                let entry_count = u32::from_be_bytes(count_buf);

                if entry_count == 0 {
                    Ok(Vec::new())
                } else {
                    let index_bytes_len = (entry_count as usize) * 12;
                    let mut index_buf = vec![0u8; index_bytes_len];
                    stream.read_exact(&mut index_buf).await?;

                    // Calculate total payload length across all entries
                    let mut total_payload_len: usize = 0;
                    #[allow(clippy::chunks_exact_to_as_chunks)]
                    for chunk in index_buf.chunks_exact(12) {
                        let len = u32::from_be_bytes(chunk[8..12].try_into().unwrap()) as usize;
                        total_payload_len = total_payload_len.saturating_add(len);
                    }

                    let mut payload_buf = BytesMut::with_capacity(total_payload_len);
                    payload_buf.resize(total_payload_len, 0);
                    stream.read_exact(&mut payload_buf).await?;

                    decode_multi_entries(entry_count, &index_buf, payload_buf.freeze())
                }
            }
            other => Err(AeroError::ProtocolError(format!(
                "Multi-fetch failed with unexpected status: {other}"
            ))),
        };

        if result.is_ok() {
            cancel_guard.defuse();
        } else {
            self.mark_broken();
        }
        result
    }
}
