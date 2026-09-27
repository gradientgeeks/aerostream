use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::config::BrokerConfig;
use crate::kafka::quota::{self, RequestCtx};
use crate::log::LogManager;

pub struct KafkaServer {
    addr: SocketAddr,
    log_manager: Arc<LogManager>,
    cfg: Arc<BrokerConfig>,
}

impl KafkaServer {
    pub fn new(addr: SocketAddr, log_manager: Arc<LogManager>, cfg: Arc<BrokerConfig>) -> Self {
        Self {
            addr,
            log_manager,
            cfg,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let std_listener = std::net::TcpListener::bind(self.addr)?;
        std_listener.set_nonblocking(true)?;
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = std_listener.as_raw_fd();
            unsafe {
                let buf_size: libc::c_int = 8 * 1024 * 1024; // 8MB buffer for TCP window scale factor negotiation
                libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &buf_size as *const _ as *const libc::c_void, std::mem::size_of_val(&buf_size) as libc::socklen_t);
                libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &buf_size as *const _ as *const libc::c_void, std::mem::size_of_val(&buf_size) as libc::socklen_t);
            }
        }
        let listener = TcpListener::from_std(std_listener)?;
        info!(
            "[AeroMQ Kafka] Kafka Wire Protocol TCP listener active on {}",
            self.addr
        );

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            // Kafka clients pipeline small requests; without TCP_NODELAY, Nagle's algorithm plus the client's delayed
            // ACK stalls every response for milliseconds.
            let _ = stream.set_nodelay(true);
            let log_manager = self.log_manager.clone();
            let cfg = self.cfg.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_kafka_connection(stream, log_manager, cfg).await {
                    debug!(
                        "[AeroMQ Kafka] Connection ended for {}: {:?}",
                        peer_addr, e
                    );
                }
            });
        }
    }
}

async fn handle_kafka_connection(
    mut stream: TcpStream,
    log_manager: Arc<LogManager>,
    cfg: Arc<BrokerConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = stream.as_raw_fd();
        unsafe {
            let buf_size: libc::c_int = 8 * 1024 * 1024; // 8MB buffer for 1MB-50MB Kafka batches
            libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_RCVBUF, &buf_size as *const _ as *const libc::c_void, std::mem::size_of_val(&buf_size) as libc::socklen_t);
            libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_SNDBUF, &buf_size as *const _ as *const libc::c_void, std::mem::size_of_val(&buf_size) as libc::socklen_t);
            let quickack: libc::c_int = 1;
            libc::setsockopt(fd, libc::IPPROTO_TCP, libc::TCP_QUICKACK, &quickack as *const _ as *const libc::c_void, std::mem::size_of_val(&quickack) as libc::socklen_t);
        }
    }

    let mut len_buf = [0u8; 4];
    // Reusable connection-level frame buffer to eliminate memory reallocation and page-faults:
    let mut frame_buf: Vec<u8> = Vec::with_capacity(256 * 1024);
    let mut conn_sasl = crate::kafka::sasl::SaslState::default();

    loop {
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => return Err(e.into()),
        }

        let frame_len = i32::from_be_bytes(len_buf);
        if frame_len <= 0 || frame_len > 64 * 1024 * 1024 {
            warn!("[AeroMQ Kafka] Invalid frame length: {}", frame_len);
            break;
        }

        // Reuse connection buffer capacity; physical pages stay resident across frames:
        frame_buf.clear();
        frame_buf.resize(frame_len as usize, 0);
        stream.read_exact(&mut frame_buf).await?;

        let mut ctx = RequestCtx::new(None, conn_sasl.authenticated_user());
        ctx.sasl_state = conn_sasl.clone();
        let resp = handle_kafka_frame_ctx(&frame_buf, &log_manager, &cfg, &mut ctx).await?;
        conn_sasl = ctx.sasl_state;
        let (api_key, api_version) = if frame_buf.len() >= 4 {
            (i16::from_be_bytes([frame_buf[0], frame_buf[1]]), i16::from_be_bytes([frame_buf[2], frame_buf[3]]))
        } else {
            (-1, 0)
        };
        let throttle = std::time::Duration::from_millis(ctx.throttle_ms as u64);
        // KIP-219: old API versions get their response delayed; newer ones get it right away
        // (carrying throttle_time_ms) and the channel is muted before the next request is read.
        let delay_before = ctx.throttle_ms > 0 && !quota::response_not_delayed(api_key, api_version);
        if delay_before {
            tokio::time::sleep(throttle).await;
        }
        if let Some(resp_bytes) = resp {
            let resp_len = (resp_bytes.len() as i32).to_be_bytes();
            if resp_bytes.len() <= 64 * 1024 {
                // One write per small response (a separate 4-byte length segment would wait on Nagle / delayed ACK).
                let mut out = Vec::with_capacity(4 + resp_bytes.len());
                out.extend_from_slice(&resp_len);
                out.extend_from_slice(&resp_bytes);
                stream.write_all(&out).await?;
            } else {
                stream.write_all(&resp_len).await?;
                stream.write_all(&resp_bytes).await?;
            }
            stream.flush().await?;
        }
        if ctx.throttle_ms > 0 && !delay_before {
            tokio::time::sleep(throttle).await;
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub async fn handle_kafka_frame(
    frame: &[u8],
    log_manager: &Arc<LogManager>,
    cfg: &Arc<BrokerConfig>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    let mut ctx = RequestCtx::default();
    handle_kafka_frame_ctx(frame, log_manager, cfg, &mut ctx).await
}

/// Like `handle_kafka_frame`, but exposes quota state: `ctx.throttle_ms` is filled with the
/// throttle the connection must apply (KIP-13/124/219).
pub async fn handle_kafka_frame_ctx(
    frame: &[u8],
    log_manager: &Arc<LogManager>,
    cfg: &Arc<BrokerConfig>,
    ctx: &mut RequestCtx,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    let started = std::time::Instant::now();
    let qm = quota::manager();
    let quotas_active = !qm.is_empty();
    let result = handle_kafka_frame_inner(frame, log_manager, cfg, ctx).await;
    if quotas_active {
        let t = qm.record_request_time(&ctx.user, &ctx.client_id, started.elapsed().as_nanos() as u64);
        ctx.throttle_ms = ctx.throttle_ms.max(t);
    }
    result
}

async fn handle_kafka_frame_inner(
    frame: &[u8],
    log_manager: &Arc<LogManager>,
    cfg: &Arc<BrokerConfig>,
    ctx: &mut RequestCtx,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    let mut cursor = io::Cursor::new(frame);
    if cursor.remaining() < 8 {
        return Err("Kafka frame too short for header".into());
    }

    let api_key = cursor.get_i16();
    let api_version = cursor.get_i16();
    let correlation_id = cursor.get_i32();
    let client_id = read_kafka_string(&mut cursor)?;
    let prev_user = ctx.user.clone();
    let prev_sasl = ctx.sasl_state.clone();
    *ctx = RequestCtx::new(client_id.as_deref(), Some(&prev_user));
    ctx.sasl_state = prev_sasl;
    if !quota::manager().is_empty() {
        // Throttle owed from earlier request-time usage is reported on this response too.
        ctx.base_throttle_ms = quota::manager().peek(quota::Metric::Request, &ctx.user, &ctx.client_id);
        ctx.throttle_ms = ctx.base_throttle_ms;
    }

    match api_key {
        18 => {
            // ApiVersions
            let resp = handle_api_versions(correlation_id, api_version)?;
            Ok(Some(resp))
        }
        17 => {
            // SaslHandshake
            let rest = &frame[cursor.position() as usize..];
            let resp = crate::kafka::sasl::handle_sasl_handshake(api_version, correlation_id, rest, &mut ctx.sasl_state)?;
            Ok(Some(resp))
        }
        36 => {
            // SaslAuthenticate
            let rest = &frame[cursor.position() as usize..];
            let resp = crate::kafka::sasl::handle_sasl_authenticate(api_version, correlation_id, rest, &mut ctx.sasl_state, cfg)?;
            if let Some(user) = ctx.sasl_state.authenticated_user() {
                ctx.user = user.to_string();
            }
            Ok(Some(resp))
        }
        3 => {
            // Metadata
            let resp = handle_metadata(correlation_id, api_version, &mut cursor, log_manager, cfg).await?;
            Ok(Some(resp))
        }
        0 => {
            // Produce
            let resp_opt = handle_produce(correlation_id, api_version, &mut cursor, log_manager, ctx, cfg).await?;
            Ok(resp_opt)
        }
        1 => {
            // Fetch
            let resp = handle_fetch(correlation_id, api_version, &mut cursor, log_manager, ctx, cfg).await?;
            Ok(Some(resp))
        }
        22 | 24 | 25 | 26 | 28 => {
            // Transactions: InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit
            let rest = &frame[cursor.position() as usize..];
            Ok(Some(crate::txn::api::handle_frame(api_key, api_version, correlation_id, rest, log_manager, cfg).await))
        }
        76..=79 => {
            // Share groups (KIP-932): ShareGroupHeartbeat/Describe, ShareFetch, ShareAcknowledge
            let rest = &frame[cursor.position() as usize..];
            Ok(Some(crate::share::api::handle_frame(api_key, api_version, correlation_id, rest, log_manager, cfg).await))
        }
        _ => {
            // Admin / group-coordinator APIs (stream C): ListOffsets, CreateTopics, JoinGroup, ...
            if crate::kafka::admin::supported_range(api_key).is_some() {
                let st = crate::kafka::admin::init(cfg, log_manager);
                let client_id = ctx.client_id.clone();
                let body = &frame[cursor.position() as usize..];
                if let Some(res) = crate::kafka::admin::dispatch(&st, api_key, api_version, correlation_id, &client_id, body).await {
                    return res.map(Some).map_err(|e| e.into());
                }
            }
            warn!("[AeroMQ Kafka] Unsupported API key: {}", api_key);
            let mut resp = BytesMut::new();
            resp.put_i32(correlation_id);
            resp.put_i16(35); // UNSUPPORTED_VERSION error code
            Ok(Some(resp.to_vec()))
        }
    }
}

// ============================================================================
// API Handlers
// ============================================================================

/// Handler for ApiVersions (API Key 18). Advertises every API the broker implements.
/// The response header is always v0; v3+ bodies use the flexible (compact) encoding.
fn handle_api_versions(
    correlation_id: i32,
    api_version: i16,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    use crate::kafka::codec::Wr;

    let mut apis: Vec<(i16, i16, i16)> = vec![
        (0, 0, 7),  // Produce: v0 - v7
        (1, 0, 11), // Fetch: v0 - v11 (v11 = KIP-392 rack_id / preferred_read_replica)
        (3, 0, 5),  // Metadata: v0 - v5
        (18, 0, 3), // ApiVersions: v0 - v3
    ];
    // Transactions (22/24/25/26/28), share groups (76-79), and SASL (17/36).
    apis.extend_from_slice(&crate::txn::api::TXN_API_VERSIONS);
    apis.extend_from_slice(&crate::share::api::SHARE_API_VERSIONS);
    apis.extend_from_slice(&crate::kafka::sasl::SASL_API_VERSIONS);
    apis.extend_from_slice(crate::kafka::admin::ADMIN_APIS);
    apis.sort();

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);

    // Unsupported ApiVersions version: reply with UNSUPPORTED_VERSION in v0 format (KIP-511).
    if api_version < 0 || api_version > 3 {
        buf.put_i16(35);
        buf.put_i32(1);
        buf.put_i16(18);
        buf.put_i16(0);
        buf.put_i16(3);
        return Ok(buf.to_vec());
    }

    let flex = api_version >= 3;
    let mut w = Wr::new(flex);
    w.i16(0); // ErrorCode: NONE
    w.arr(apis.len());
    for (key, min_v, max_v) in apis {
        w.i16(key).i16(min_v).i16(max_v).tagged();
    }
    if api_version >= 1 {
        w.i32(0); // ThrottleTimeMs
    }
    w.tagged();
    buf.put_slice(&w.finish());
    Ok(buf.to_vec())
}

/// Handler for Metadata (API Key 3)
async fn handle_metadata(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
    log_manager: &Arc<LogManager>,
    cfg: &Arc<BrokerConfig>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    // Cluster view (brokers with racks, real partition layout) from the controller-fed topology cache
    // (kept fresh by the refresh loop started in `admin::init` at broker startup).
    let snap = crate::topology::TopologyCache::global().snapshot();
    handle_metadata_with_snapshot(correlation_id, api_version, cursor, log_manager, cfg, snap).await
}

pub(crate) async fn handle_metadata_with_snapshot(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
    log_manager: &Arc<LogManager>,
    cfg: &Arc<BrokerConfig>,
    snap: Arc<crate::topology::Snapshot>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut requested_topics = Vec::new();
    if cursor.remaining() >= 4 {
        let topics_count = cursor.get_i32();
        if topics_count > 0 {
            for _ in 0..topics_count {
                if let Some(topic) = read_kafka_string(cursor)? {
                    requested_topics.push(topic);
                }
            }
        }
    }

    let my_id = cfg.id as i32;
    let cluster = !snap.brokers.is_empty();

    // If no specific topics requested (empty or null array), list known topics from the cluster + local logs
    let topics_to_report: Vec<String> = if requested_topics.is_empty() {
        let mut set: std::collections::BTreeSet<String> = snap.topics.keys().cloned().collect();
        // With a populated cluster view the controller is authoritative (so deleted topics disappear even
        // while stale local logs linger); otherwise fall back to locally known topics.
        if snap.topics.is_empty() {
            for (topic, _, _) in log_manager.get_all_offsets().await {
                set.insert(topic);
            }
        }
        if set.is_empty() {
            vec!["default".to_string()]
        } else {
            set.into_iter().collect()
        }
    } else {
        requested_topics
    };

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);

    if api_version >= 1 {
        buf.put_i32(0); // ThrottleTimeMs
    }

    // Brokers array (all live brokers when the controller view is available)
    let self_rack = cfg.rack.clone();
    let brokers: Vec<(i32, String, i32, Option<String>)> = if cluster {
        snap.brokers.values().map(|b| {
            let port = if b.id == my_id {
                cfg.kafka_port
            } else if b.kafka_port > 0 {
                b.kafka_port
            } else {
                cfg.kafka_port
            };
            (b.id, b.host.clone(), port, b.rack.clone())
        }).collect()
    } else {
        vec![(my_id, cfg.host.clone(), cfg.kafka_port, self_rack)]
    };
    buf.put_i32(brokers.len() as i32);
    for (id, host, port, rack) in &brokers {
        buf.put_i32(*id); // NodeId
        put_kafka_string(&mut buf, Some(host));
        buf.put_i32(*port);
        if api_version >= 1 {
            put_kafka_string(&mut buf, rack.as_deref()); // Rack
        }
    }

    if api_version >= 2 {
        put_kafka_string(&mut buf, Some("aerostream-cluster")); // ClusterId (v2+)
    }

    if api_version >= 1 {
        let controller = brokers.iter().map(|b| b.0).min().unwrap_or(my_id);
        buf.put_i32(controller); // ControllerId
    }

    // TopicMetadata array
    buf.put_i32(topics_to_report.len() as i32);
    for topic_name in topics_to_report {
        buf.put_i16(0); // ErrorCode: 0 (NONE)
        put_kafka_string(&mut buf, Some(&topic_name));

        if api_version >= 1 {
            buf.put_u8(0); // IsInternal: false
        }

        // Partition layout: controller-provided when known, else single local partition 0
        let layout: Vec<(i32, i32, Vec<i32>, Vec<i32>)> = match snap.topics.get(&topic_name) {
            Some(t) if !t.partitions.is_empty() => t
                .partitions
                .iter()
                .map(|(pid, p)| (*pid, if p.leader == 0 { -1 } else { p.leader }, p.replicas.clone(), p.isr.clone()))
                .collect(),
            _ => vec![(0, my_id, vec![my_id], vec![my_id])],
        };
        buf.put_i32(layout.len() as i32);
        for (pid, leader, replicas, isr) in layout {
            buf.put_i16(if leader < 0 { 5 } else { 0 }); // LEADER_NOT_AVAILABLE when unassigned
            buf.put_i32(pid); // PartitionIndex
            buf.put_i32(leader); // LeaderId

            if api_version >= 7 {
                buf.put_i32(0); // LeaderEpoch: 0
            }

            buf.put_i32(replicas.len() as i32);
            for r in &replicas {
                buf.put_i32(*r);
            }
            buf.put_i32(isr.len() as i32);
            for r in &isr {
                buf.put_i32(*r);
            }

            if api_version >= 5 {
                // OfflineReplicas: replicas whose broker is not currently registered/active
                let offline: Vec<i32> = if cluster {
                    replicas.iter().copied().filter(|r| !snap.brokers.contains_key(r)).collect()
                } else {
                    vec![]
                };
                buf.put_i32(offline.len() as i32);
                for r in offline {
                    buf.put_i32(r);
                }
            }
        }
    }

    Ok(buf.to_vec())
}

/// Handler for Produce (API Key 0)
async fn handle_produce(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
    log_manager: &Arc<LogManager>,
    ctx: &mut RequestCtx,
    cfg: &Arc<BrokerConfig>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    let mut produced_bytes: u64 = 0;
    let transactional_id = if api_version >= 3 {
        read_kafka_string(cursor)?
    } else {
        None
    };

    if cursor.remaining() < 6 {
        return Err("Produce request truncated".into());
    }

    let acks = cursor.get_i16();
    let _timeout_ms = cursor.get_i32();

    let topics_count = cursor.get_i32();
    let mut topic_results = Vec::new();

    for _ in 0..topics_count {
        let topic_name = read_kafka_string(cursor)?.unwrap_or_default();
        let partitions_count = cursor.get_i32();
        let mut part_results = Vec::new();

        for _ in 0..partitions_count {
            let partition_index = cursor.get_i32();
            let records_size = cursor.get_i32();

            let mut appended_offset = 0i64;
            let mut error_code = 0i16;

            if records_size > 0 && cursor.remaining() >= records_size as usize {
                let pos = cursor.position() as usize;
                let raw_records_slice = &cursor.get_ref()[pos..pos + records_size as usize];
                cursor.set_position((pos + records_size as usize) as u64);
                produced_bytes += raw_records_slice.len() as u64;

                // Compression: validate codec/body, and re-encode when the topic forces a codec.
                let ctype = crate::kafka::compression::registry().for_topic(&topic_name);
                let records_data = match crate::kafka::compression::normalize_produce_payload(raw_records_slice, ctype) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("[AeroMQ Kafka] Rejecting produce for {}-{}: {}", topic_name, partition_index, e);
                        part_results.push((partition_index, e.error_code(), -1));
                        continue;
                    }
                };

                // Modern RecordBatch payloads (idempotent / transactional aware, one record per offset).
                let mut handled = false;
                if crate::txn::batch::is_magic2(&records_data) {
                    match log_manager.get_partition(&topic_name, partition_index as u32).await {
                        Ok(part_log) => {
                            let coord = crate::txn::coordinator_for(log_manager, cfg);
                            let mut guard = part_log.lock().await;
                            if let Some(outcome) = crate::txn::produce::append_payload(
                                &mut guard,
                                &records_data,
                                &coord,
                                transactional_id.as_deref(),
                                &topic_name,
                                partition_index,
                            ) {
                                error_code = outcome.error;
                                appended_offset = outcome.base_offset;
                                handled = true;
                            }
                        }
                        Err(e) => {
                            error!("[AeroMQ Kafka] Failed to get partition {}-{}: {:?}", topic_name, partition_index, e);
                            error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                            handled = true;
                        }
                    }
                }

                // Legacy MessageSet / raw payloads keep the original path.
                let (producer_id, base_sequence, records_count) = (-1i64, -1i32, 1i32);

                if !handled {
                match log_manager.get_partition(&topic_name, partition_index as u32).await {
                    Ok(part_log) => {
                        let mut guard = part_log.lock().await;
                        match guard.validate_idempotent_produce(producer_id, base_sequence) {
                            Ok(Some(cached_offset)) => {
                                appended_offset = cached_offset;
                                error_code = 0;
                            }
                            Ok(None) => {
                                match guard.append(&records_data) {
                                    Ok(off) => {
                                        appended_offset = off as i64;
                                        guard.update_producer_state(producer_id, base_sequence, records_count, appended_offset);
                                    }
                                    Err(e) => {
                                        error!("[AeroMQ Kafka] Append error for {}-{}: {:?}", topic_name, partition_index, e);
                                        error_code = 1; // OFFSET_OUT_OF_RANGE or general error
                                    }
                                }
                            }
                            Err(err) => {
                                warn!("[AeroMQ Kafka] Idempotent produce sequence error for PID {} seq {}: err {}", producer_id, base_sequence, err);
                                error_code = err; // 45: OutOfOrderSequenceNumber
                            }
                        }
                    }
                    Err(e) => {
                        error!("[AeroMQ Kafka] Failed to get partition {}-{}: {:?}", topic_name, partition_index, e);
                        error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                    }
                }
                }
            }

            part_results.push((partition_index, error_code, appended_offset));
        }

        topic_results.push((topic_name, part_results));
    }

    // Producer byte-rate quota (KIP-13): record bytes and compute throttle.
    let mut throttle_ms = ctx.base_throttle_ms;
    if !quota::manager().is_empty() {
        let t = quota::manager().record(quota::Metric::Produce, &ctx.user, &ctx.client_id, produced_bytes as f64);
        throttle_ms = throttle_ms.max(t);
        ctx.throttle_ms = ctx.throttle_ms.max(throttle_ms);
    }

    // If acks == 0, Kafka specification dictates NO response frame is returned to the client
    if acks == 0 {
        return Ok(None);
    }

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);
    buf.put_i32(topic_results.len() as i32);

    for (topic, parts) in topic_results {
        put_kafka_string(&mut buf, Some(&topic));
        buf.put_i32(parts.len() as i32);
        for (part_idx, err_code, base_off) in parts {
            buf.put_i32(part_idx);
            buf.put_i16(err_code);
            buf.put_i64(base_off);
            if api_version >= 2 {
                buf.put_i64(-1); // LogAppendTimeMs
            }
            if api_version >= 5 {
                buf.put_i64(0); // LogStartOffset
            }
        }
    }

    if api_version >= 1 {
        buf.put_i32(throttle_ms as i32); // ThrottleTimeMs
    }

    Ok(Some(buf.to_vec()))
}

/// Handler for Fetch (API Key 1), using the process-wide topology cache for KIP-392 routing.
async fn handle_fetch(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
    log_manager: &Arc<LogManager>,
    ctx: &mut RequestCtx,
    cfg: &Arc<BrokerConfig>,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let topo = crate::topology::TopologyCache::global();
    handle_fetch_with_topo(correlation_id, api_version, cursor, log_manager, ctx, cfg, &topo).await
}

/// Fetch implementation. Supports v0-v11; v11 carries `rack_id` and returns `preferred_read_replica` (KIP-392).
pub(crate) async fn handle_fetch_with_topo(
    correlation_id: i32,
    api_version: i16,
    cursor: &mut io::Cursor<&[u8]>,
    log_manager: &Arc<LogManager>,
    ctx: &mut RequestCtx,
    cfg: &Arc<BrokerConfig>,
    topo: &crate::topology::TopologyCache,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    if cursor.remaining() < 12 {
        return Err("Fetch request truncated".into());
    }

    let replica_id = cursor.get_i32();
    let max_wait_ms = cursor.get_i32();
    let min_bytes = cursor.get_i32();

    let mut max_bytes = i32::MAX;
    if api_version >= 3 && cursor.remaining() >= 4 {
        max_bytes = cursor.get_i32();
    }
    let mut isolation_level = 0i8;
    if api_version >= 4 && cursor.remaining() >= 1 {
        isolation_level = cursor.get_i8();
    }
    if api_version >= 7 && cursor.remaining() >= 8 {
        let _session_id = cursor.get_i32();
        let _session_epoch = cursor.get_i32();
    }

    let topics_count = cursor.get_i32();
    // Requested partitions are parsed first; rack_id (v11) trails the topics/forgotten-topics arrays.
    let mut requested: Vec<(String, Vec<(i32, i64, i32)>)> = Vec::new();

    for _ in 0..topics_count {
        let topic_name = read_kafka_string(cursor)?.unwrap_or_default();
        let partitions_count = cursor.get_i32();
        let mut parts = Vec::new();

        for _ in 0..partitions_count {
            let partition_index = cursor.get_i32();
            if api_version >= 9 && cursor.remaining() >= 4 {
                let _current_leader_epoch = cursor.get_i32();
            }
            let fetch_offset = cursor.get_i64();
            if api_version >= 5 && cursor.remaining() >= 8 {
                let _log_start_offset = cursor.get_i64();
            }
            let partition_max_bytes = cursor.get_i32();
            parts.push((partition_index, fetch_offset, partition_max_bytes));
        }
        requested.push((topic_name, parts));
    }

    // forgotten_topics_data (v7+): incremental fetch sessions are not used, so just skip it.
    if api_version >= 7 && cursor.remaining() >= 4 {
        let forgotten = cursor.get_i32();
        for _ in 0..forgotten.max(0) {
            let _ = read_kafka_string(cursor)?;
            let n = cursor.get_i32();
            for _ in 0..n.max(0) {
                if cursor.remaining() >= 4 {
                    let _ = cursor.get_i32();
                }
            }
        }
    }
    // rack_id (v11, KIP-392): the client's rack, used to pick the closest replica.
    let client_rack = if api_version >= 11 { read_kafka_string(cursor)? } else { None };

    let my_id = cfg.id as i32;
    let selector = crate::topology::ReplicaSelector::parse(&cfg.replica_selector);

    // Long polling (like Kafka's purgatory): if fewer than `min_bytes` are available, wait up to `max_wait_ms` for an
    // append on any partition, then read again. Answering empty fetches immediately made caught-up consumers spin.
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(max_wait_ms.max(0) as u64);
    let notify = log_manager.append_notify.clone();
    let topic_results = loop {
        // Registered before reading, so an append that lands while we read is not missed.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let mut topic_results = Vec::new();
        let mut total_bytes: usize = 0;
        let mut must_answer = false; // errors / replica redirects are returned without waiting
        let mut remaining = max_bytes.max(0) as u64;

        for (topic_name, parts) in &requested {
            let mut part_results = Vec::new();

            for &(partition_index, fetch_offset, partition_max_bytes) in parts {
                let mut high_watermark = 0i64;
                let mut last_stable_offset = 0i64;
                let mut aborted: Option<Vec<(i64, i64)>> = None;
                let mut record_set = Vec::new();
                let mut error_code = 0i16;
                let mut preferred_replica = -1i32;
                let mut hw_cap: Option<i64> = None;

                // KIP-392 routing (consumers only: replica_id == -1) using the controller-fed topology.
                if replica_id < 0 {
                    if let Some(info) = topo.partition(topic_name, partition_index) {
                        if info.leader == my_id {
                            let snap = topo.snapshot();
                            let choice = crate::topology::select_replica(selector, client_rack.as_deref(), &info, &snap.brokers, fetch_offset);
                            if choice != my_id && choice > 0 {
                                // Tell the client to read from the closer replica; no records from the leader.
                                preferred_replica = choice;
                            }
                        } else if info.replicas.contains(&my_id) {
                            // Follower read: only expose data known to be committed (<= partition high watermark).
                            hw_cap = Some(info.high_watermark);
                        } else if info.leader != 0 {
                            error_code = 6; // NOT_LEADER_OR_FOLLOWER
                        }
                    }
                }

                match log_manager.get_partition(topic_name, partition_index as u32).await {
                    Ok(_) if error_code != 0 => {}
                    Ok(part_log) => {
                        // The partition lock covers only in-memory state and the index lookup; file reads happen after.
                        let (hw, lso, aborted_txns, read_spec) = {
                            let mut guard = part_log.lock().await;
                            let view = crate::txn::produce::fetch_view(&guard, fetch_offset, isolation_level);
                            let mut upper_bound = view.upper_bound;
                            let mut hw = view.high_watermark;
                            let mut lso = view.last_stable_offset;
                            if let Some(cap) = hw_cap {
                                hw = hw.min(cap);
                                lso = lso.min(cap);
                                upper_bound = upper_bound.min(cap);
                            }

                            // The first partition with data always returns at least one entry (KIP-74).
                            let budget = if total_bytes == 0 { partition_max_bytes.max(1) as u64 } else { remaining.min(partition_max_bytes.max(0) as u64) };
                            let spec = if preferred_replica < 0 && fetch_offset >= 0 && fetch_offset < upper_bound && budget > 0 {
                                let max_read = budget.min(32 * 1024 * 1024) as u32;
                                guard.read_range(fetch_offset as u64, upper_bound as u64, max_read).ok().flatten()
                            } else {
                                None
                            };

                            (hw, lso, view.aborted, spec)
                        };

                        high_watermark = hw;
                        last_stable_offset = lso;
                        aborted = aborted_txns;

                        if let Some((file, position, len, first_len)) = read_spec {
                            use std::os::unix::fs::FileExt;
                            let mut raw = vec![0u8; len as usize];
                            if file.read_exact_at(&mut raw, position).is_ok() {
                                // A run of record batches is returned as is (their base offsets were patched on append);
                                // anything else (legacy message sets, raw native-protocol payloads) is wrapped entry by entry,
                                // so only the first entry is returned.
                                record_set = if crate::txn::batch::split_batches(&raw).is_some() {
                                    raw
                                } else {
                                    ensure_kafka_record_set(fetch_offset, &raw[..first_len.min(len) as usize])
                                };
                            }
                        }
                    }
                    Err(_) => {
                        error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                    }
                }

                total_bytes += record_set.len();
                remaining = remaining.saturating_sub(record_set.len() as u64);
                must_answer |= error_code != 0 || preferred_replica >= 0;
                part_results.push((partition_index, error_code, high_watermark, last_stable_offset, aborted, record_set, preferred_replica));
            }

            topic_results.push((topic_name.clone(), part_results));
        }

        let now = std::time::Instant::now();
        if must_answer || min_bytes <= 0 || total_bytes >= min_bytes as usize || now >= deadline {
            break topic_results;
        }
        let _ = tokio::time::timeout(deadline - now, notified).await;
    };

    // Consumer byte-rate quota (KIP-13): record served bytes and compute throttle.
    let mut throttle_ms = ctx.base_throttle_ms;
    if !quota::manager().is_empty() {
        let served: usize = topic_results
            .iter()
            .flat_map(|(_, parts)| parts.iter())
            .map(|(_, _, _, _, _, r, _)| r.len())
            .sum();
        let t = quota::manager().record(quota::Metric::Fetch, &ctx.user, &ctx.client_id, served as f64);
        throttle_ms = throttle_ms.max(t);
        ctx.throttle_ms = ctx.throttle_ms.max(throttle_ms);
    }

    let mut buf = BytesMut::new();
    buf.put_i32(correlation_id);

    if api_version >= 1 {
        buf.put_i32(throttle_ms as i32); // ThrottleTimeMs
    }

    if api_version >= 7 {
        buf.put_i16(0); // ErrorCode
        buf.put_i32(0); // SessionId
    }

    buf.put_i32(topic_results.len() as i32);
    for (topic, parts) in topic_results {
        put_kafka_string(&mut buf, Some(&topic));
        buf.put_i32(parts.len() as i32);
        for (part_idx, err_code, hw, lso, aborted, records, preferred) in parts {
            buf.put_i32(part_idx);
            buf.put_i16(err_code);
            buf.put_i64(hw);
            if api_version >= 4 {
                buf.put_i64(lso); // LastStableOffset
            }
            if api_version >= 5 {
                buf.put_i64(0); // LogStartOffset
            }
            if api_version >= 4 {
                match aborted {
                    // read_uncommitted: null array
                    None => buf.put_i32(-1),
                    Some(list) => {
                        buf.put_i32(list.len() as i32);
                        for (pid, first_offset) in list {
                            buf.put_i64(pid);
                            buf.put_i64(first_offset);
                        }
                    }
                }
            }
            if api_version >= 11 {
                buf.put_i32(preferred); // PreferredReadReplica (-1 = none)
            }
            buf.put_i32(records.len() as i32);
            buf.put_slice(&records);
        }
    }

    Ok(buf.to_vec())
}

// ============================================================================
// Encoding / Decoding Utilities
// ============================================================================

pub fn read_kafka_string(
    cursor: &mut io::Cursor<&[u8]>,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    if cursor.remaining() < 2 {
        return Ok(None);
    }
    let len = cursor.get_i16();
    if len < 0 {
        return Ok(None);
    }
    let ulen = len as usize;
    if cursor.remaining() < ulen {
        return Err("Kafka string length exceeds available bytes".into());
    }
    let mut buf = vec![0u8; ulen];
    cursor.copy_to_slice(&mut buf);
    Ok(Some(String::from_utf8(buf)?))
}

pub fn put_kafka_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(val) => {
            buf.put_i16(val.len() as i16);
            buf.put_slice(val.as_bytes());
        }
        None => {
            buf.put_i16(-1);
        }
    }
}

#[allow(dead_code)]
pub fn put_kafka_bytes(buf: &mut BytesMut, b: Option<&[u8]>) {
    match b {
        Some(val) => {
            buf.put_i32(val.len() as i32);
            buf.put_slice(val);
        }
        None => {
            buf.put_i32(-1);
        }
    }
}

/// Ensures the data is in Kafka record format. If it is already formatted as a
/// RecordBatch (magic=2) or MessageSet (magic=0 or 1), returns it directly;
/// otherwise wraps the raw payload into a standard Kafka MessageSet v0 frame.
pub fn ensure_kafka_record_set(offset: i64, raw_buf: &[u8]) -> Vec<u8> {
    if raw_buf.len() >= 17 {
        // RecordBatch check: magic byte 2 at offset 16
        if raw_buf[16] == 2 {
            let batch_len = i32::from_be_bytes(raw_buf[8..12].try_into().unwrap());
            if batch_len > 0 && (batch_len as usize + 12) <= raw_buf.len() {
                return raw_buf.to_vec();
            }
        }
        // MessageSet check: magic byte 0 or 1 at offset 16
        if raw_buf[16] == 0 || raw_buf[16] == 1 {
            let msg_size = i32::from_be_bytes(raw_buf[8..12].try_into().unwrap());
            if msg_size > 0 && (msg_size as usize + 12) <= raw_buf.len() {
                return raw_buf.to_vec();
            }
        }
    }

    wrap_in_kafka_message_set(offset, raw_buf)
}

/// Wrap a raw payload into a Kafka v0 MessageSet entry.
pub fn wrap_in_kafka_message_set(offset: i64, payload: &[u8]) -> Vec<u8> {
    // Inner message: [magic(1)][attributes(1)][key_len(4)][val_len(4)][value]
    let inner_len = 1 + 1 + 4 + 4 + payload.len();
    let mut inner = Vec::with_capacity(inner_len);
    inner.push(0u8); // magic: 0
    inner.push(0u8); // attributes: 0
    inner.extend_from_slice(&(-1i32).to_be_bytes()); // null key
    inner.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    inner.extend_from_slice(payload);

    let crc = crc32_ieee(&inner);

    // Frame: [offset(8)][message_size(4)][crc(4)][inner...]
    let mut record = Vec::with_capacity(8 + 4 + 4 + inner.len());
    record.extend_from_slice(&offset.to_be_bytes());
    record.extend_from_slice(&((4 + inner.len()) as i32).to_be_bytes());
    record.extend_from_slice(&crc.to_be_bytes());
    record.extend_from_slice(&inner);
    record
}

/// Standard IEEE 802.3 CRC32 implementation.
pub fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = -((crc & 1) as i32) as u32;
            crc = (crc >> 1) ^ (0xEDB88320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    struct TestDir(std::path::PathBuf);
    impl TestDir {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!("aeromq_test_{}_{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> std::io::Result<TestDir> {
        Ok(TestDir::new())
    }

    #[test]
    fn test_crc32_ieee() {
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF43926);
    }

    #[test]
    fn test_wrap_in_kafka_message_set() {
        let payload = b"hello kafka";
        let wrapped = wrap_in_kafka_message_set(42, payload);

        // Header: offset(8) + message_size(4) + crc(4) + magic(1) + attr(1) + key(-1: 4) + val_len(4) + val(11)
        assert_eq!(wrapped.len(), 8 + 4 + 4 + 1 + 1 + 4 + 4 + payload.len());
        let offset = i64::from_be_bytes(wrapped[0..8].try_into().unwrap());
        assert_eq!(offset, 42);
    }

    #[tokio::test]
    async fn test_handle_api_versions() {
        let resp = handle_api_versions(1234, 0).unwrap();
        let mut cursor = io::Cursor::new(resp.as_slice());
        assert_eq!(cursor.get_i32(), 1234); // correlation_id
        assert_eq!(cursor.get_i16(), 0);    // error_code: NONE
        let num_keys = cursor.get_i32();
        assert_eq!(
            num_keys as usize,
            4 + crate::kafka::admin::ADMIN_APIS.len()
                + crate::txn::api::TXN_API_VERSIONS.len()
                + crate::share::api::SHARE_API_VERSIONS.len()
                + crate::kafka::sasl::SASL_API_VERSIONS.len()
        );
        let mut keys = Vec::new();
        for _ in 0..num_keys {
            keys.push(cursor.get_i16());
            cursor.advance(4); // min/max version
        }
        // Transactions (22/24/25/26/28), share groups (76-79), and SASL (17/36) are advertised.
        for k in [17i16, 22, 24, 25, 26, 28, 36, 76, 77, 78, 79] {
            assert!(keys.contains(&k), "missing api key {}", k);
        }
    }

    #[tokio::test]
    async fn test_handle_metadata() {
        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = Arc::new(BrokerConfig {
            id: 1,
            host: "127.0.0.1".into(),
            data_port: 9091,
            kafka_port: 9093,
            ..Default::default()
        });

        // Request metadata for "test-topic"
        let mut req = BytesMut::new();
        req.put_i32(1); // 1 topic
        put_kafka_string(&mut req, Some("test-topic"));

        let mut cursor = io::Cursor::new(req.as_ref());
        let resp = handle_metadata(5678, 0, &mut cursor, &log_mgr, &cfg).await.unwrap();

        let mut resp_cursor = io::Cursor::new(resp.as_slice());
        assert_eq!(resp_cursor.get_i32(), 5678); // correlation_id
        let brokers_count = resp_cursor.get_i32();
        assert_eq!(brokers_count, 1);
        assert_eq!(resp_cursor.get_i32(), 1); // broker id = 1
        let host = read_kafka_string(&mut resp_cursor).unwrap().unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(resp_cursor.get_i32(), 9093); // kafka port
    }

    #[tokio::test]
    async fn test_produce_and_fetch_round_trip() {
        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = Arc::new(BrokerConfig {
            id: 1,
            host: "127.0.0.1".into(),
            data_port: 9091,
            kafka_port: 9093,
            ..Default::default()
        });

        // 1. Build Produce request
        let payload = b"aero-kafka-record";
        let message_record = wrap_in_kafka_message_set(0, payload);

        let mut prod_frame = BytesMut::new();
        prod_frame.put_i16(0); // api_key = 0 (Produce)
        prod_frame.put_i16(0); // api_version = 0
        prod_frame.put_i32(999); // correlation_id = 999
        put_kafka_string(&mut prod_frame, Some("test-client"));
        prod_frame.put_i16(1); // acks = 1
        prod_frame.put_i32(1000); // timeout_ms
        prod_frame.put_i32(1); // 1 topic
        put_kafka_string(&mut prod_frame, Some("my-topic"));
        prod_frame.put_i32(1); // 1 partition
        prod_frame.put_i32(0); // partition 0
        prod_frame.put_i32(message_record.len() as i32);
        prod_frame.put_slice(&message_record);

        let prod_resp = handle_kafka_frame(&prod_frame, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut cur = io::Cursor::new(prod_resp.as_slice());
        assert_eq!(cur.get_i32(), 999); // correlation_id
        assert_eq!(cur.get_i32(), 1);   // 1 topic
        let resp_topic = read_kafka_string(&mut cur).unwrap().unwrap();
        assert_eq!(resp_topic, "my-topic");
        assert_eq!(cur.get_i32(), 1);   // 1 partition
        assert_eq!(cur.get_i32(), 0);   // partition 0
        assert_eq!(cur.get_i16(), 0);   // error_code 0
        let base_offset = cur.get_i64();
        assert_eq!(base_offset, 0);

        // 2. Build Fetch request
        let mut fetch_frame = BytesMut::new();
        fetch_frame.put_i16(1); // api_key = 1 (Fetch)
        fetch_frame.put_i16(0); // api_version = 0
        fetch_frame.put_i32(1000); // correlation_id
        put_kafka_string(&mut fetch_frame, Some("test-consumer"));
        fetch_frame.put_i32(-1); // replica_id
        fetch_frame.put_i32(500); // max_wait_ms
        fetch_frame.put_i32(1); // min_bytes
        fetch_frame.put_i32(1); // 1 topic
        put_kafka_string(&mut fetch_frame, Some("my-topic"));
        fetch_frame.put_i32(1); // 1 partition
        fetch_frame.put_i32(0); // partition 0
        fetch_frame.put_i64(0); // fetch_offset = 0
        fetch_frame.put_i32(65536); // partition_max_bytes

        let fetch_resp = handle_kafka_frame(&fetch_frame, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut fcur = io::Cursor::new(fetch_resp.as_slice());
        assert_eq!(fcur.get_i32(), 1000); // correlation_id
        assert_eq!(fcur.get_i32(), 1);    // 1 topic
        let ftopic = read_kafka_string(&mut fcur).unwrap().unwrap();
        assert_eq!(ftopic, "my-topic");
        assert_eq!(fcur.get_i32(), 1);    // 1 partition
        assert_eq!(fcur.get_i32(), 0);    // partition 0
        assert_eq!(fcur.get_i16(), 0);    // error_code 0
        let hw = fcur.get_i64();
        assert!(hw >= 1);
        let records_len = fcur.get_i32();
        assert!(records_len > 0);
    }

    fn produce_frame_v3(client: &str, topic: &str, records: &[u8], version: i16) -> BytesMut {
        let mut f = BytesMut::new();
        f.put_i16(0);
        f.put_i16(version);
        f.put_i32(7);
        put_kafka_string(&mut f, Some(client));
        put_kafka_string(&mut f, None); // transactional_id
        f.put_i16(1); // acks
        f.put_i32(1000);
        f.put_i32(1);
        put_kafka_string(&mut f, Some(topic));
        f.put_i32(1);
        f.put_i32(0);
        f.put_i32(records.len() as i32);
        f.put_slice(records);
        f
    }

    /// Returns the produce partition error code (v3 layout).
    fn produce_error_code(resp: &[u8]) -> i16 {
        let mut c = io::Cursor::new(resp);
        c.get_i32();
        c.get_i32();
        read_kafka_string(&mut c).unwrap();
        c.get_i32();
        c.get_i32();
        c.get_i16()
    }

    fn fetch_records_v4(log_mgr_resp: &[u8]) -> Vec<u8> {
        let mut c = io::Cursor::new(log_mgr_resp);
        c.get_i32(); // corr
        c.get_i32(); // throttle
        c.get_i32(); // topics
        read_kafka_string(&mut c).unwrap();
        c.get_i32(); // parts
        c.get_i32();
        c.get_i16();
        c.get_i64(); // hw
        c.get_i64(); // lso
        c.get_i32(); // aborted
        let n = c.get_i32() as usize;
        let mut v = vec![0u8; n];
        c.copy_to_slice(&mut v);
        v
    }

    /// Fetch v4 frame with explicit max_wait_ms / min_bytes / partition max bytes.
    fn fetch_frame_v4_opts(topic: &str, offset: i64, max_wait_ms: i32, min_bytes: i32, part_max: i32) -> BytesMut {
        let mut f = BytesMut::new();
        f.put_i16(1);
        f.put_i16(4);
        f.put_i32(9);
        put_kafka_string(&mut f, Some("c"));
        f.put_i32(-1);
        f.put_i32(max_wait_ms);
        f.put_i32(min_bytes);
        f.put_i32(64 << 20);
        f.put_i8(0);
        f.put_i32(1);
        put_kafka_string(&mut f, Some(topic));
        f.put_i32(1);
        f.put_i32(0);
        f.put_i64(offset);
        f.put_i32(part_max);
        f
    }

    #[tokio::test]
    async fn fetch_returns_many_records_per_partition_not_one() {
        use crate::kafka::handlers::{encode_records_batch, parse_records, KafkaRecord};
        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1).with_limits(128 << 20, None, None));
        let cfg = Arc::new(BrokerConfig::default());
        for i in 0..50u8 {
            let b = encode_records_batch(0, &[KafkaRecord::new(None, Some(vec![i; 1000]))]);
            let resp = handle_kafka_frame(&produce_frame_v3("c", "many", &b, 3), &log_mgr, &cfg).await.unwrap().unwrap();
            assert_eq!(produce_error_code(&resp), 0);
        }
        let fr = handle_kafka_frame(&fetch_frame_v4_opts("many", 0, 0, 1, 1 << 20), &log_mgr, &cfg).await.unwrap().unwrap();
        let recs = parse_records(&fetch_records_v4(&fr)).unwrap();
        assert_eq!(recs.len(), 50, "one Fetch must return every record up to the byte limit");
        assert_eq!(recs[49].value.as_deref(), Some(&[49u8; 1000][..]));

        // Byte limit: ~5 KB returns a handful of records, and at least one when the limit is smaller than one record.
        let fr = handle_kafka_frame(&fetch_frame_v4_opts("many", 10, 0, 1, 5000), &log_mgr, &cfg).await.unwrap().unwrap();
        let n = parse_records(&fetch_records_v4(&fr)).unwrap().len();
        assert!((1..=5).contains(&n), "got {}", n);
        let fr = handle_kafka_frame(&fetch_frame_v4_opts("many", 10, 0, 1, 10), &log_mgr, &cfg).await.unwrap().unwrap();
        assert_eq!(parse_records(&fetch_records_v4(&fr)).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn fetch_long_polls_until_data_or_max_wait() {
        use crate::kafka::handlers::{encode_records_batch, parse_records, KafkaRecord};
        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1).with_limits(128 << 20, None, None));
        let cfg = Arc::new(BrokerConfig::default());
        let b = encode_records_batch(0, &[KafkaRecord::new(None, Some(b"x".to_vec()))]);
        handle_kafka_frame(&produce_frame_v3("c", "lp", &b, 3), &log_mgr, &cfg).await.unwrap().unwrap();

        // Caught up (offset 1 == high watermark): waits max_wait, then answers empty.
        let t = std::time::Instant::now();
        let fr = handle_kafka_frame(&fetch_frame_v4_opts("lp", 1, 300, 1, 1 << 20), &log_mgr, &cfg).await.unwrap().unwrap();
        assert!(fetch_records_v4(&fr).is_empty());
        assert!(t.elapsed() >= std::time::Duration::from_millis(250), "returned after {:?}", t.elapsed());

        // An append while waiting wakes the fetch early.
        let (lm, c2) = (log_mgr.clone(), cfg.clone());
        let producer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let b = encode_records_batch(0, &[KafkaRecord::new(None, Some(b"late".to_vec()))]);
            handle_kafka_frame(&produce_frame_v3("c", "lp", &b, 3), &lm, &c2).await.unwrap();
        });
        let t = std::time::Instant::now();
        let fr = handle_kafka_frame(&fetch_frame_v4_opts("lp", 1, 5000, 1, 1 << 20), &log_mgr, &cfg).await.unwrap().unwrap();
        producer.await.unwrap();
        assert!(t.elapsed() < std::time::Duration::from_millis(2000), "not woken by append: {:?}", t.elapsed());
        assert_eq!(parse_records(&fetch_records_v4(&fr)).unwrap()[0].value.as_deref(), Some(&b"late"[..]));
    }

    fn fetch_frame_v4(client: &str, topic: &str, offset: i64) -> BytesMut {
        let mut f = BytesMut::new();
        f.put_i16(1);
        f.put_i16(4);
        f.put_i32(8);
        put_kafka_string(&mut f, Some(client));
        f.put_i32(-1);
        f.put_i32(100);
        f.put_i32(1);
        f.put_i32(1 << 20); // max_bytes
        f.put_i8(0); // isolation
        f.put_i32(1);
        put_kafka_string(&mut f, Some(topic));
        f.put_i32(1);
        f.put_i32(0);
        f.put_i64(offset);
        f.put_i32(1 << 20);
        f
    }

    #[tokio::test]
    async fn test_compressed_batches_produce_fetch_all_codecs() {
        use crate::kafka::compression::{recompress_batch, Codec};
        use crate::kafka::handlers::{encode_records_batch, parse_records, KafkaRecord};
        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = Arc::new(BrokerConfig::default());
        let recs: Vec<KafkaRecord> = (0..10)
            .map(|i| KafkaRecord::new(Some(format!("k{}", i).into_bytes()), Some(vec![b'a' + i as u8; 300])))
            .collect();
        let plain = encode_records_batch(0, &recs);
        for (n, codec) in [Codec::Gzip, Codec::Snappy, Codec::Lz4, Codec::Zstd].into_iter().enumerate() {
            let topic = format!("comp-{}", n);
            let z = recompress_batch(&plain, codec).unwrap();
            let resp = handle_kafka_frame(&produce_frame_v3("c", &topic, &z, 3), &log_mgr, &cfg).await.unwrap().unwrap();
            assert_eq!(produce_error_code(&resp), 0);
            // The broker decompresses multi-record batches on produce so every record keeps its own
            // offset (see txn::batch::to_entries); each one must be fetchable and decodable.
            let part = log_mgr.get_partition(&topic, 0).await.unwrap();
            assert_eq!(part.lock().await.next_offset, 10, "{:?}: one offset per record", codec);
            for i in [0usize, 3, 9] {
                let fr = handle_kafka_frame(&fetch_frame_v4("c", &topic, i as i64), &log_mgr, &cfg).await.unwrap().unwrap();
                let got = fetch_records_v4(&fr);
                let parsed = parse_records(&got).unwrap();
                assert_eq!(parsed[0].value, recs[i].value, "{:?} offset {}", codec, i);
            }
        }
    }

    #[tokio::test]
    async fn test_topic_compression_type_recompresses_and_unsupported_rejected() {
        use crate::kafka::compression::{registry, Codec, CompressionType};
        use crate::kafka::handlers::{encode_records_batch, parse_records, KafkaRecord};
        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = Arc::new(BrokerConfig::default());
        let recs: Vec<KafkaRecord> = (0..10)
            .map(|i| KafkaRecord::new(None, Some(vec![b'z'; 100 + i])))
            .collect();
        let plain = encode_records_batch(0, &recs);
        registry().set_topic("force-zstd-topic", CompressionType::Codec(Codec::Zstd));
        let resp = handle_kafka_frame(&produce_frame_v3("c", "force-zstd-topic", &plain, 3), &log_mgr, &cfg).await.unwrap().unwrap();
        assert_eq!(produce_error_code(&resp), 0);
        // Recompressed to zstd on the wire path, then split into one entry (offset) per record.
        let part = log_mgr.get_partition("force-zstd-topic", 0).await.unwrap();
        assert_eq!(part.lock().await.next_offset, 10);
        for i in [0usize, 9] {
            let fr = handle_kafka_frame(&fetch_frame_v4("c", "force-zstd-topic", i as i64), &log_mgr, &cfg).await.unwrap().unwrap();
            let got = fetch_records_v4(&fr);
            assert_eq!(parse_records(&got).unwrap()[0].value, recs[i].value);
        }

        // unsupported codec id 7 -> UNSUPPORTED_COMPRESSION_TYPE (76)
        let mut bad = plain.clone();
        bad[22] = 7;
        let resp = handle_kafka_frame(&produce_frame_v3("c", "plain-topic", &bad, 3), &log_mgr, &cfg).await.unwrap().unwrap();
        assert_eq!(produce_error_code(&resp), 76);
    }

    #[test]
    fn test_compaction_key_extraction_from_compressed_batch() {
        use crate::kafka::compression::{recompress_batch, Codec};
        use crate::kafka::handlers::{encode_records_batch, KafkaRecord};
        let mut rec = KafkaRecord::new(Some(b"user-1".to_vec()), Some(vec![b'q'; 500]));
        rec.timestamp = 1_700_000_000_000;
        let plain = encode_records_batch(0, &[rec]);
        for c in [Codec::Gzip, Codec::Snappy, Codec::Lz4, Codec::Zstd] {
            let z = recompress_batch(&plain, c).unwrap();
            let ek = crate::log::compactor::extract_key(&z);
            assert_eq!(ek.key.as_deref(), Some(&b"user-1"[..]), "{:?}", c);
            assert!(!ek.is_tombstone);
        }
    }

    #[tokio::test]
    async fn test_producer_quota_returns_throttle_time() {
        use crate::kafka::handlers::{encode_records_batch, KafkaRecord};
        use crate::kafka::quota::{manager, QuotaEntry};
        let _g = crate::kafka::quota::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = Arc::new(BrokerConfig::default());
        manager().set_entries(vec![QuotaEntry {
            client_id: Some("quota-test-client".into()),
            producer_byte_rate: Some(1000.0),
            ..Default::default()
        }]);
        let big = encode_records_batch(0, &[KafkaRecord::new(None, Some(vec![7u8; 50_000]))]);
        let frame = produce_frame_v3("quota-test-client", "quota-topic", &big, 3);
        let mut ctx = RequestCtx::default();
        let resp = handle_kafka_frame_ctx(&frame, &log_mgr, &cfg, &mut ctx).await.unwrap().unwrap();
        let throttle = i32::from_be_bytes(resp[resp.len() - 4..].try_into().unwrap());
        assert!(throttle > 0, "expected throttle_time_ms > 0");
        assert_eq!(ctx.throttle_ms as i32, throttle);
        // unrelated client is not throttled
        let frame2 = produce_frame_v3("someone-else", "quota-topic", &big, 3);
        let mut ctx2 = RequestCtx::default();
        let resp2 = handle_kafka_frame_ctx(&frame2, &log_mgr, &cfg, &mut ctx2).await.unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(resp2[resp2.len() - 4..].try_into().unwrap()), 0);
        manager().set_entries(vec![]);
    }

    #[tokio::test]
    async fn test_init_producer_id_and_idempotent_produce() {
        use crate::kafka::handlers::{encode_idempotent_records_batch, KafkaRecord};

        let dir = tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = Arc::new(BrokerConfig {
            id: 1,
            host: "127.0.0.1".into(),
            data_port: 9091,
            kafka_port: 9093,
            ..Default::default()
        });

        // 1. Send InitProducerId (ApiKey 22)
        let mut init_frame = BytesMut::new();
        init_frame.put_i16(22); // ApiKey = 22 (InitProducerId)
        init_frame.put_i16(0);  // ApiVersion = 0
        init_frame.put_i32(701); // CorrelationId = 701
        put_kafka_string(&mut init_frame, Some("idempotent-client"));
        put_kafka_string(&mut init_frame, None); // transactional_id: null
        init_frame.put_i32(60000); // transaction_timeout_ms

        let init_resp = handle_kafka_frame(&init_frame, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut icur = io::Cursor::new(init_resp.as_slice());
        assert_eq!(icur.get_i32(), 701); // correlation_id
        assert_eq!(icur.get_i32(), 0);   // throttle_time_ms
        assert_eq!(icur.get_i16(), 0);   // error_code: 0
        let producer_id = icur.get_i64();
        let producer_epoch = icur.get_i16();
        assert!(producer_id >= 1000);
        assert_eq!(producer_epoch, 0);

        // 2. First Idempotent Produce: sequence 0
        let record = KafkaRecord::new(Some(b"key-0".to_vec()), Some(b"val-seq-0".to_vec()));
        let batch_seq_0 = encode_idempotent_records_batch(0, producer_id, producer_epoch, 0, &[record]);

        let mut prod_frame_0 = BytesMut::new();
        prod_frame_0.put_i16(0); // Produce
        prod_frame_0.put_i16(0); // v0
        prod_frame_0.put_i32(702); // CorrelationId
        put_kafka_string(&mut prod_frame_0, Some("idempotent-client"));
        prod_frame_0.put_i16(1); // acks = 1
        prod_frame_0.put_i32(1000); // timeout
        prod_frame_0.put_i32(1); // 1 topic
        put_kafka_string(&mut prod_frame_0, Some("idemp-topic"));
        prod_frame_0.put_i32(1); // 1 partition
        prod_frame_0.put_i32(0); // partition 0
        prod_frame_0.put_i32(batch_seq_0.len() as i32);
        prod_frame_0.put_slice(&batch_seq_0);

        let resp_0 = handle_kafka_frame(&prod_frame_0, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut cur0 = io::Cursor::new(resp_0.as_slice());
        assert_eq!(cur0.get_i32(), 702);
        assert_eq!(cur0.get_i32(), 1);
        let _ = read_kafka_string(&mut cur0);
        assert_eq!(cur0.get_i32(), 1); // 1 part
        assert_eq!(cur0.get_i32(), 0); // part 0
        assert_eq!(cur0.get_i16(), 0); // error_code: 0
        let offset_0 = cur0.get_i64();
        assert_eq!(offset_0, 0);

        // Verify partition next_offset is 1
        {
            let part_log = log_mgr.get_partition("idemp-topic", 0).await.unwrap();
            let guard = part_log.lock().await;
            assert_eq!(guard.next_offset, 1);
        }

        // 3. Duplicate Produce: duplicate sequence 0
        let mut prod_frame_dup = BytesMut::new();
        prod_frame_dup.put_i16(0);
        prod_frame_dup.put_i16(0);
        prod_frame_dup.put_i32(703);
        put_kafka_string(&mut prod_frame_dup, Some("idempotent-client"));
        prod_frame_dup.put_i16(1);
        prod_frame_dup.put_i32(1000);
        prod_frame_dup.put_i32(1);
        put_kafka_string(&mut prod_frame_dup, Some("idemp-topic"));
        prod_frame_dup.put_i32(1);
        prod_frame_dup.put_i32(0);
        prod_frame_dup.put_i32(batch_seq_0.len() as i32);
        prod_frame_dup.put_slice(&batch_seq_0);

        let resp_dup = handle_kafka_frame(&prod_frame_dup, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut cur_dup = io::Cursor::new(resp_dup.as_slice());
        assert_eq!(cur_dup.get_i32(), 703);
        assert_eq!(cur_dup.get_i32(), 1);
        let _ = read_kafka_string(&mut cur_dup);
        assert_eq!(cur_dup.get_i32(), 1);
        assert_eq!(cur_dup.get_i32(), 0);
        assert_eq!(cur_dup.get_i16(), 0); // duplicate ACK returned without error
        let dup_offset = cur_dup.get_i64();
        assert_eq!(dup_offset, 0); // returned cached offset 0

        // Assert log length only increased by 1 and broker did not write a second record!
        {
            let part_log = log_mgr.get_partition("idemp-topic", 0).await.unwrap();
            let guard = part_log.lock().await;
            assert_eq!(guard.next_offset, 1);
        }

        // 4. Sequence gap: send sequence 5 when last sequence was 0 -> assert error code 45 (OutOfOrderSequenceNumber)
        let record_gap = KafkaRecord::new(Some(b"key-gap".to_vec()), Some(b"val-gap".to_vec()));
        let batch_gap = encode_idempotent_records_batch(1, producer_id, producer_epoch, 5, &[record_gap]);

        let mut prod_frame_gap = BytesMut::new();
        prod_frame_gap.put_i16(0);
        prod_frame_gap.put_i16(0);
        prod_frame_gap.put_i32(704);
        put_kafka_string(&mut prod_frame_gap, Some("idempotent-client"));
        prod_frame_gap.put_i16(1);
        prod_frame_gap.put_i32(1000);
        prod_frame_gap.put_i32(1);
        put_kafka_string(&mut prod_frame_gap, Some("idemp-topic"));
        prod_frame_gap.put_i32(1);
        prod_frame_gap.put_i32(0);
        prod_frame_gap.put_i32(batch_gap.len() as i32);
        prod_frame_gap.put_slice(&batch_gap);

        let resp_gap = handle_kafka_frame(&prod_frame_gap, &log_mgr, &cfg).await.unwrap().unwrap();
        let mut cur_gap = io::Cursor::new(resp_gap.as_slice());
        assert_eq!(cur_gap.get_i32(), 704);
        assert_eq!(cur_gap.get_i32(), 1);
        let _ = read_kafka_string(&mut cur_gap);
        assert_eq!(cur_gap.get_i32(), 1);
        assert_eq!(cur_gap.get_i32(), 0);
        let gap_err_code = cur_gap.get_i16();
        assert_eq!(gap_err_code, 45); // OutOfOrderSequenceNumber!

        // Assert log still did not write the out-of-order record!
        {
            let part_log = log_mgr.get_partition("idemp-topic", 0).await.unwrap();
            let guard = part_log.lock().await;
            assert_eq!(guard.next_offset, 1);
        }
    }
}

#[cfg(test)]
mod topology_tests {
    use super::*;
    use crate::kafka::admin::ADMIN_APIS;
    use crate::kafka::codec::Rd;
    use crate::topology::{BrokerNode, PartitionInfo, Snapshot, TopicInfo, TopologyCache};
    use std::collections::{BTreeMap, HashMap};

    fn cluster_snapshot() -> Snapshot {
        let mut s = Snapshot::default();
        for (id, rack) in [(1, "rack-a"), (2, "rack-b"), (3, "rack-c")] {
            s.brokers.insert(
                id,
                BrokerNode { id, host: format!("broker-{id}"), kafka_port: 9092, rack: Some(rack.to_string()) },
            );
        }
        let mut ti = TopicInfo::default();
        ti.partitions.insert(
            0,
            PartitionInfo {
                leader: 1,
                replicas: vec![1, 2],
                isr: vec![1, 2],
                high_watermark: 1,
                replica_offsets: HashMap::from([(1, 1), (2, 1)]),
            },
        );
        ti.partitions.insert(
            1,
            PartitionInfo { leader: 0, replicas: vec![3, 1], isr: vec![], high_watermark: 0, replica_offsets: HashMap::new() },
        );
        s.topics.insert("rt".into(), ti);
        s
    }

    fn cfg_for(id: u32) -> Arc<BrokerConfig> {
        Arc::new(BrokerConfig { id, host: "127.0.0.1".into(), kafka_port: 9092, rack: Some("rack-x".into()), ..Default::default() })
    }

    #[tokio::test]
    async fn api_versions_v0_lists_every_admin_api() {
        let resp = handle_api_versions(7, 0).unwrap();
        let mut c = io::Cursor::new(resp.as_slice());
        assert_eq!(c.get_i32(), 7);
        assert_eq!(c.get_i16(), 0);
        let n = c.get_i32();
        let mut got = BTreeMap::new();
        for _ in 0..n {
            got.insert(c.get_i16(), (c.get_i16(), c.get_i16()));
        }
        for (k, lo, hi) in ADMIN_APIS {
            assert_eq!(got.get(k), Some(&(*lo, *hi)), "api {k}");
        }
        assert_eq!(got[&1], (0, 11), "Fetch v11 advertised for KIP-392");
        assert_eq!(c.remaining(), 0, "v0 has no throttle/tagged trailer");
    }

    #[tokio::test]
    async fn api_versions_v3_uses_flexible_encoding() {
        let resp = handle_api_versions(8, 3).unwrap();
        let mut r = Rd::new(&resp[4..], true);
        assert_eq!(i32::from_be_bytes(resp[..4].try_into().unwrap()), 8);
        assert_eq!(r.i16().unwrap(), 0);
        let n = r.arr().unwrap();
        assert_eq!(
            n,
            4 + ADMIN_APIS.len()
                + crate::txn::api::TXN_API_VERSIONS.len()
                + crate::share::api::SHARE_API_VERSIONS.len()
                + crate::kafka::sasl::SASL_API_VERSIONS.len()
        );
        let mut keys = vec![];
        for _ in 0..n {
            keys.push(r.i16().unwrap());
            r.i16().unwrap();
            r.i16().unwrap();
            r.tagged().unwrap();
        }
        assert!(keys.windows(2).all(|w| w[0] < w[1]), "sorted and unique");
        assert_eq!(r.i32().unwrap(), 0);
        r.tagged().unwrap();
        assert_eq!(r.remaining(), 0);
    }

    #[tokio::test]
    async fn api_versions_unsupported_version_falls_back_to_v0_error() {
        let resp = handle_api_versions(9, 42).unwrap();
        let mut c = io::Cursor::new(resp.as_slice());
        assert_eq!(c.get_i32(), 9);
        assert_eq!(c.get_i16(), 35);
        assert_eq!(c.get_i32(), 1);
        assert_eq!((c.get_i16(), c.get_i16(), c.get_i16()), (18, 0, 3));
    }

    #[tokio::test]
    async fn metadata_reports_racks_and_real_partition_layout() {
        let dir = tempfile::tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = cfg_for(1);
        let mut req = BytesMut::new();
        req.put_i32(1);
        put_kafka_string(&mut req, Some("rt"));
        let mut cur = io::Cursor::new(req.as_ref());
        let resp = handle_metadata_with_snapshot(1, 5, &mut cur, &log_mgr, &cfg, Arc::new(cluster_snapshot())).await.unwrap();

        let mut c = io::Cursor::new(resp.as_slice());
        assert_eq!(c.get_i32(), 1);
        assert_eq!(c.get_i32(), 0); // throttle
        assert_eq!(c.get_i32(), 3); // brokers
        let mut racks = vec![];
        for _ in 0..3 {
            let id = c.get_i32();
            let host = read_kafka_string(&mut c).unwrap().unwrap();
            assert_eq!(host, format!("broker-{id}"));
            assert_eq!(c.get_i32(), 9092);
            racks.push(read_kafka_string(&mut c).unwrap().unwrap());
        }
        assert_eq!(racks, vec!["rack-a", "rack-b", "rack-c"]);
        assert_eq!(read_kafka_string(&mut c).unwrap().as_deref(), Some("aerostream-cluster")); // cluster_id (v2+)
        assert_eq!(c.get_i32(), 1); // controller = lowest broker id
        assert_eq!(c.get_i32(), 1); // topics
        assert_eq!(c.get_i16(), 0);
        assert_eq!(read_kafka_string(&mut c).unwrap().unwrap(), "rt");
        assert_eq!(c.get_u8(), 0);
        assert_eq!(c.get_i32(), 2); // partitions
        // p0
        assert_eq!(c.get_i16(), 0);
        assert_eq!(c.get_i32(), 0);
        assert_eq!(c.get_i32(), 1); // leader
        assert_eq!(c.get_i32(), 2);
        assert_eq!((c.get_i32(), c.get_i32()), (1, 2)); // replicas
        assert_eq!(c.get_i32(), 2);
        assert_eq!((c.get_i32(), c.get_i32()), (1, 2)); // isr
        assert_eq!(c.get_i32(), 0); // offline
        // p1 has no leader yet -> LEADER_NOT_AVAILABLE (5), leader -1
        assert_eq!(c.get_i16(), 5);
        assert_eq!(c.get_i32(), 1);
        assert_eq!(c.get_i32(), -1);
    }

    #[tokio::test]
    async fn metadata_without_controller_view_keeps_local_single_broker_behaviour() {
        let dir = tempfile::tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        let cfg = cfg_for(1);
        let mut req = BytesMut::new();
        req.put_i32(1);
        put_kafka_string(&mut req, Some("adhoc"));
        let mut cur = io::Cursor::new(req.as_ref());
        let resp = handle_metadata_with_snapshot(1, 1, &mut cur, &log_mgr, &cfg, Arc::new(Snapshot::default())).await.unwrap();
        let mut c = io::Cursor::new(resp.as_slice());
        c.get_i32();
        c.get_i32();
        assert_eq!(c.get_i32(), 1);
        assert_eq!(c.get_i32(), 1);
        read_kafka_string(&mut c).unwrap();
        assert_eq!(c.get_i32(), 9092);
        assert_eq!(read_kafka_string(&mut c).unwrap().as_deref(), Some("rack-x"), "own broker.rack advertised");
    }

    fn fetch_v11(rack: &str, replica_id: i32, topic: &str, partition: i32, offset: i64) -> Vec<u8> {
        // body only (the frame header is consumed by the dispatcher)
        let mut b = BytesMut::new();
        b.put_i32(replica_id);
        b.put_i32(100); // max_wait
        b.put_i32(1); // min_bytes
        b.put_i32(1 << 20); // max_bytes
        b.put_i8(0); // isolation
        b.put_i32(0); // session_id
        b.put_i32(-1); // session_epoch
        b.put_i32(1);
        put_kafka_string(&mut b, Some(topic));
        b.put_i32(1);
        b.put_i32(partition);
        b.put_i32(-1); // current_leader_epoch (v9+)
        b.put_i64(offset);
        b.put_i64(0); // log_start_offset
        b.put_i32(65536);
        b.put_i32(0); // forgotten topics
        put_kafka_string(&mut b, Some(rack));
        b.to_vec()
    }

    /// (error, high watermark, preferred replica, records length)
    fn parse_fetch_v11(resp: &[u8]) -> (i16, i64, i32, i32) {
        let mut c = io::Cursor::new(resp);
        assert_eq!(c.get_i32(), 55);
        c.get_i32(); // throttle
        assert_eq!(c.get_i16(), 0);
        c.get_i32(); // session id
        assert_eq!(c.get_i32(), 1);
        read_kafka_string(&mut c).unwrap();
        assert_eq!(c.get_i32(), 1);
        c.get_i32(); // partition
        let err = c.get_i16();
        let hw = c.get_i64();
        c.get_i64(); // lso
        c.get_i64(); // log start
        assert_eq!(c.get_i32(), -1); // aborted txns: null under read_uncommitted
        let preferred = c.get_i32();
        let len = c.get_i32();
        (err, hw, preferred, len)
    }

    async fn do_fetch(id: u32, topo: &TopologyCache, log_mgr: &Arc<LogManager>, body: &[u8]) -> Vec<u8> {
        let cfg = cfg_for(id);
        let mut cur = io::Cursor::new(body);
        handle_fetch_with_topo(55, 11, &mut cur, log_mgr, &mut RequestCtx::default(), &cfg, topo).await.unwrap()
    }

    #[tokio::test]
    async fn fetch_v11_leader_redirects_rack_local_consumer_to_follower() {
        let dir = tempfile::tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        {
            let p = log_mgr.get_partition("rt", 0).await.unwrap();
            p.lock().await.append(&[9u8; 32]).unwrap();
        }
        let topo = TopologyCache::default();
        topo.update(cluster_snapshot());

        // consumer in rack-b (follower 2's rack): leader answers with preferred_read_replica=2 and no records
        let r = do_fetch(1, &topo, &log_mgr, &fetch_v11("rack-b", -1, "rt", 0, 0)).await;
        assert_eq!(parse_fetch_v11(&r), (0, 1, 2, 0));

        // consumer in the leader's rack, unknown rack, or empty rack: served by the leader (-1)
        for rack in ["rack-a", "rack-zzz", ""] {
            let r = do_fetch(1, &topo, &log_mgr, &fetch_v11(rack, -1, "rt", 0, 0)).await;
            let (err, hw, pref, len) = parse_fetch_v11(&r);
            assert_eq!((err, hw, pref), (0, 1, -1), "rack {rack:?}");
            assert!(len > 0, "leader must return records for rack {rack:?}");
        }

        // replica fetchers (replica_id >= 0) are never redirected
        let r = do_fetch(1, &topo, &log_mgr, &fetch_v11("rack-b", 2, "rt", 0, 0)).await;
        assert_eq!(parse_fetch_v11(&r).2, -1);
    }

    #[tokio::test]
    async fn fetch_v11_follower_serves_only_committed_data_and_rejects_non_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 2));
        {
            let p = log_mgr.get_partition("rt", 0).await.unwrap();
            p.lock().await.append(&[9u8; 32]).unwrap();
        }
        let topo = TopologyCache::default();
        let mut snap = cluster_snapshot();
        snap.topics.get_mut("rt").unwrap().partitions.get_mut(&0).unwrap().high_watermark = 0;
        topo.update(snap);

        // follower (broker 2) has 1 local entry but the partition HW is 0 -> nothing exposed yet
        let r = do_fetch(2, &topo, &log_mgr, &fetch_v11("rack-b", -1, "rt", 0, 0)).await;
        assert_eq!(parse_fetch_v11(&r), (0, 0, -1, 0));

        // once the leader advances the HW the follower serves the record itself
        topo.update(cluster_snapshot());
        let r = do_fetch(2, &topo, &log_mgr, &fetch_v11("rack-b", -1, "rt", 0, 0)).await;
        let (err, hw, pref, len) = parse_fetch_v11(&r);
        assert_eq!((err, hw, pref), (0, 1, -1));
        assert!(len > 0);

        // broker 3 is not a replica of partition 0 -> NOT_LEADER_OR_FOLLOWER
        let dir3 = tempfile::tempdir().unwrap();
        let lm3 = Arc::new(LogManager::new(dir3.path(), 3));
        let r = do_fetch(3, &topo, &lm3, &fetch_v11("rack-c", -1, "rt", 0, 0)).await;
        let mut c = io::Cursor::new(r.as_slice());
        c.advance(4 + 4 + 2 + 4 + 4); // corr, throttle, error, session, topic count
        read_kafka_string(&mut c).unwrap();
        c.advance(4 + 4); // partition count, partition index
        assert_eq!(c.get_i16(), 6);
    }

    #[tokio::test]
    async fn fetch_v7_partition_layout_has_no_leader_epoch() {
        // v5-v8 partitions carry log_start_offset but NO current_leader_epoch (that arrives in v9).
        let dir = tempfile::tempdir().unwrap();
        let log_mgr = Arc::new(LogManager::new(dir.path(), 1));
        {
            let p = log_mgr.get_partition("v7t", 0).await.unwrap();
            p.lock().await.append(&[1u8; 16]).unwrap();
        }
        let cfg = cfg_for(1);
        let mut b = BytesMut::new();
        b.put_i32(-1);
        b.put_i32(100);
        b.put_i32(1);
        b.put_i32(1 << 20);
        b.put_i8(0);
        b.put_i32(0);
        b.put_i32(-1);
        b.put_i32(1);
        put_kafka_string(&mut b, Some("v7t"));
        b.put_i32(1);
        b.put_i32(0); // partition
        b.put_i64(0); // fetch_offset
        b.put_i64(0); // log_start_offset
        b.put_i32(65536);
        b.put_i32(0); // forgotten
        let topo = TopologyCache::default();
        let mut cur = io::Cursor::new(b.as_ref());
        let resp = handle_fetch_with_topo(3, 7, &mut cur, &log_mgr, &mut RequestCtx::default(), &cfg, &topo).await.unwrap();
        let mut c = io::Cursor::new(resp.as_slice());
        c.advance(4 + 4 + 2 + 4 + 4);
        read_kafka_string(&mut c).unwrap();
        c.advance(4 + 4);
        assert_eq!(c.get_i16(), 0);
        assert_eq!(c.get_i64(), 1);
    }
}
