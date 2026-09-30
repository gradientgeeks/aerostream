use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::StreamExt;

use aerostream_client::protocol::*;
use aerostream_client::{AeroClient, AeroConsumer, AeroProducer, ClientConfig, ConsumerConfig, ProducerConfig};

type MessageStore = Arc<Mutex<HashMap<(String, u32), Vec<(u64, Vec<u8>)>>>>;

struct MockBroker {
    addr: SocketAddr,
    _messages: MessageStore,
    shutdown_tx: broadcast::Sender<()>,
}

impl MockBroker {
    async fn start(auth_token: Option<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let messages: MessageStore = Arc::new(Mutex::new(HashMap::new()));
        let (shutdown_tx, _) = broadcast::channel(1);

        let msgs_clone = messages.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let expected_token = auth_token.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_res = listener.accept() => {
                        let (mut socket, _) = match accept_res {
                            Ok(res) => res,
                            Err(_) => break,
                        };
                        let msgs = msgs_clone.clone();
                        let token_req = expected_token.clone();

                        tokio::spawn(async move {
                            let mut authenticated = token_req.is_none();
                            let mut header = [0u8; 7];

                            while socket.read_exact(&mut header).await.is_ok() {
                                if !is_valid_magic(header[0], header[1]) {
                                    break;
                                }
                                let cmd = header[2];
                                let body_len = u32::from_be_bytes(header[3..7].try_into().unwrap()) as usize;
                                let mut body = vec![0u8; body_len];
                                if socket.read_exact(&mut body).await.is_err() {
                                    break;
                                }

                                if cmd == 0 {
                                    // Auth
                                    let provided = String::from_utf8_lossy(&body);
                                    let ok = match &token_req {
                                        Some(expected) => provided == *expected,
                                        None => true,
                                    };
                                    authenticated = ok;
                                    let status = if ok { 0u8 } else { 3u8 };
                                    let _ = socket.write_all(&[0xAE, 0x01, status]).await;
                                    if !ok {
                                        break;
                                    }
                                    continue;
                                }

                                if !authenticated {
                                    let _ = socket.write_all(&[0xAE, 0x01, 3]).await;
                                    break;
                                }

                                match cmd {
                                    1 => {
                                        // Produce: [topic_len(2)][topic][part(4)][payload_len(4)][payload]
                                        let t_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
                                        let topic = std::str::from_utf8(&body[2..2 + t_len]).unwrap().to_string();
                                        let partition = u32::from_be_bytes(body[2 + t_len..6 + t_len].try_into().unwrap());
                                        let p_len = u32::from_be_bytes(body[6 + t_len..10 + t_len].try_into().unwrap()) as usize;
                                        let payload = body[10 + t_len..10 + t_len + p_len].to_vec();

                                        let mut store = msgs.lock().await;
                                        let partition_log = store.entry((topic, partition)).or_default();
                                        let offset = partition_log.len() as u64;
                                        partition_log.push((offset, payload));

                                        let mut resp = [0u8; 11];
                                        resp[0] = 0xAE;
                                        resp[1] = 0x01;
                                        resp[2] = 0x00; // Success
                                        resp[3..11].copy_from_slice(&offset.to_be_bytes());
                                        let _ = socket.write_all(&resp).await;
                                    }
                                    2 => {
                                        // Fetch (single): [t_len(2)][topic][part(4)][start_off(8)][max_bytes(4)]
                                        let t_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
                                        let topic = std::str::from_utf8(&body[2..2 + t_len]).unwrap().to_string();
                                        let partition = u32::from_be_bytes(body[2 + t_len..6 + t_len].try_into().unwrap());
                                        let start_off = u64::from_be_bytes(body[6 + t_len..14 + t_len].try_into().unwrap());

                                        let store = msgs.lock().await;
                                        let mut found_data = None;
                                        if let Some(log) = store.get(&(topic, partition)) {
                                            for (off, data) in log {
                                                if *off == start_off {
                                                    found_data = Some(data.clone());
                                                    break;
                                                }
                                            }
                                        }

                                        match found_data {
                                            Some(data) => {
                                                let mut header = [0u8; 7];
                                                header[0] = 0xAE;
                                                header[1] = 0x01;
                                                header[2] = 0x02; // DATA
                                                header[3..7].copy_from_slice(&(data.len() as u32).to_be_bytes());
                                                let _ = socket.write_all(&header).await;
                                                let _ = socket.write_all(&data).await;
                                            }
                                            None => {
                                                let _ = socket.write_all(&[0xAE, 0x01, 0x01]).await; // EMPTY
                                            }
                                        }
                                    }
                                    4 => {
                                        // FetchMulti: [t_len(2)][topic][part(4)][start_off(8)][max_bytes(4)][max_wait(4)]
                                        let t_len = u16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
                                        let topic = std::str::from_utf8(&body[2..2 + t_len]).unwrap().to_string();
                                        let partition = u32::from_be_bytes(body[2 + t_len..6 + t_len].try_into().unwrap());
                                        let start_off = u64::from_be_bytes(body[6 + t_len..14 + t_len].try_into().unwrap());

                                        let store = msgs.lock().await;
                                        let mut matching = Vec::new();
                                        if let Some(log) = store.get(&(topic, partition)) {
                                            for (off, data) in log {
                                                if *off >= start_off {
                                                    matching.push((*off, data.clone()));
                                                }
                                            }
                                        }

                                        if matching.is_empty() {
                                            let _ = socket.write_all(&[0xAE, 0x01, 0x01]).await; // EMPTY
                                        } else {
                                            let mut resp = Vec::new();
                                            resp.extend_from_slice(&[0xAE, 0x01, 0x02]);
                                            resp.extend_from_slice(&(matching.len() as u32).to_be_bytes());
                                            for (off, data) in &matching {
                                                resp.extend_from_slice(&off.to_be_bytes());
                                                resp.extend_from_slice(&(data.len() as u32).to_be_bytes());
                                            }
                                            for (_, data) in &matching {
                                                resp.extend_from_slice(data);
                                            }
                                            let _ = socket.write_all(&resp).await;
                                        }
                                    }
                                    _ => break,
                                }
                            }
                        });
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        Self {
            addr,
            _messages: messages,
            shutdown_tx,
        }
    }
}

impl Drop for MockBroker {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

#[tokio::test]
async fn test_auth_handshake() {
    let broker = MockBroker::start(Some("secret-key-123".into())).await;

    // Fail authentication with bad token
    let bad_config = ClientConfig::builder()
        .bootstrap_server(broker.addr.to_string())
        .token("wrong-token")
        .connect_timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let client_bad = AeroClient::new(bad_config);
    let res = client_bad.get_connection(&broker.addr.to_string()).await;
    assert!(res.is_err());

    // Succeed authentication with correct token
    let ok_config = ClientConfig::builder()
        .bootstrap_server(broker.addr.to_string())
        .token("secret-key-123")
        .connect_timeout(Duration::from_secs(2))
        .build()
        .unwrap();

    let client_ok = AeroClient::new(ok_config);
    let conn = client_ok.get_connection(&broker.addr.to_string()).await.unwrap();
    assert!(conn.is_alive());
}

#[tokio::test]
async fn test_end_to_end_produce_and_fetch_multi() {
    let broker = MockBroker::start(None).await;

    let producer_config = ProducerConfig::builder()
        .bootstrap_server(broker.addr.to_string())
        .build()
        .unwrap();

    let producer = AeroProducer::new(producer_config);

    // Produce multiple messages
    let topic = "orders";
    let partition = 0;
    for i in 0..5 {
        let payload = format!("order-payload-{i}");
        let offset = producer.send(topic, partition, payload.as_bytes()).await.unwrap();
        assert_eq!(offset, i as u64);
    }

    // Consumer fetch_multi
    let consumer_config = ConsumerConfig::builder()
        .bootstrap_server(broker.addr.to_string())
        .topic(topic)
        .partition(partition)
        .initial_offset(0)
        .build()
        .unwrap();

    let consumer = AeroConsumer::new(consumer_config);
    let records = consumer
        .fetch_multi(topic, partition, 0, 1024 * 1024, 100)
        .await
        .unwrap();

    assert_eq!(records.len(), 5);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.offset, i as u64);
        let expected = format!("order-payload-{i}");
        assert_eq!(record.payload, Bytes::from(expected));
    }
}

#[tokio::test]
async fn test_end_to_end_continuous_stream() {
    let broker = MockBroker::start(None).await;
    let topic = "telemetry";
    let partition = 0;

    let client = AeroClient::connect(broker.addr.to_string()).unwrap();
    let producer = client.producer();
    let consumer = client.consumer(topic, partition);

    let mut stream = consumer.stream();

    // Produce in background
    let p = producer.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        for i in 0..4 {
            p.send(topic, partition, format!("telemetry-{i}").as_bytes())
                .await
                .unwrap();
        }
    });

    // Read from stream
    let mut collected = Vec::new();
    for _ in 0..4 {
        let item = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .expect("stream item timed out")
            .expect("stream ended");
        let record = item.unwrap();
        collected.push(record);
    }

    assert_eq!(collected.len(), 4);
    for (i, r) in collected.iter().enumerate() {
        assert_eq!(r.offset, i as u64);
        assert_eq!(r.payload, Bytes::from(format!("telemetry-{i}")));
    }
}
