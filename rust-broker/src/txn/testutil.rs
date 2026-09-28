//! Test helpers: build Kafka request frames and parse responses through the
//! real `handle_kafka_frame` entry point.

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};

use crate::config::BrokerConfig;
use crate::log::LogManager;
use crate::net::kafka_server::handle_kafka_frame;

pub struct TestDir(pub std::path::PathBuf);

impl TestDir {
    pub fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "aero_{}_{}_{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
    pub fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub struct Env {
    pub dir: TestDir,
    pub lm: Arc<LogManager>,
    pub cfg: Arc<BrokerConfig>,
}

pub fn env(tag: &str) -> Env {
    let dir = TestDir::new(tag);
    let lm = Arc::new(LogManager::new(dir.path(), 1).with_limits(1024 * 1024, None, None));
    let cfg = Arc::new(BrokerConfig::default());
    Env { dir, lm, cfg }
}

pub fn frame(api_key: i16, ver: i16, corr: i32, flex: bool, body: &[u8]) -> Vec<u8> {
    let mut b = BytesMut::new();
    b.put_i16(api_key);
    b.put_i16(ver);
    b.put_i32(corr);
    b.put_i16(4);
    b.put_slice(b"test");
    if flex {
        b.put_u8(0); // header tagged fields
    }
    b.put_slice(body);
    b.to_vec()
}

/// Sends a request and returns the response body (after correlation id and, if flexible, header tags).
pub async fn call(e: &Env, api_key: i16, ver: i16, flex: bool, body: &[u8]) -> Vec<u8> {
    let corr = 42;
    let f = frame(api_key, ver, corr, flex, body);
    let resp = handle_kafka_frame(&f, &e.lm, &e.cfg).await.unwrap().unwrap();
    assert_eq!(i32::from_be_bytes(resp[0..4].try_into().unwrap()), corr);
    let skip = if flex { 5 } else { 4 };
    resp[skip..].to_vec()
}

pub fn produce_body_v3(tid: Option<&str>, topic: &str, partition: i32, records: &[u8]) -> Vec<u8> {
    let mut b = BytesMut::new();
    match tid {
        Some(t) => {
            b.put_i16(t.len() as i16);
            b.put_slice(t.as_bytes());
        }
        None => b.put_i16(-1),
    }
    b.put_i16(1); // acks
    b.put_i32(1000);
    b.put_i32(1);
    b.put_i16(topic.len() as i16);
    b.put_slice(topic.as_bytes());
    b.put_i32(1);
    b.put_i32(partition);
    b.put_i32(records.len() as i32);
    b.put_slice(records);
    b.to_vec()
}

/// Returns (error_code, base_offset) of the only partition in a Produce v3 response body.
pub fn parse_produce_resp_v3(body: &[u8]) -> (i16, i64) {
    let mut c = std::io::Cursor::new(body);
    assert_eq!(c.get_i32(), 1);
    let n = c.get_i16() as usize;
    c.advance(n);
    assert_eq!(c.get_i32(), 1);
    let _idx = c.get_i32();
    let err = c.get_i16();
    let off = c.get_i64();
    (err, off)
}

pub fn fetch_body_v7(topic: &str, partition: i32, offset: i64, isolation: i8) -> Vec<u8> {
    let mut b = BytesMut::new();
    b.put_i32(-1); // replica
    b.put_i32(0); // max wait
    b.put_i32(1); // min bytes
    b.put_i32(1 << 20); // max bytes
    b.put_i8(isolation);
    b.put_i32(0); // session id
    b.put_i32(-1); // session epoch
    b.put_i32(1);
    b.put_i16(topic.len() as i16);
    b.put_slice(topic.as_bytes());
    b.put_i32(1);
    b.put_i32(partition);
    b.put_i64(offset);
    b.put_i64(0); // log start offset
    b.put_i32(1 << 20);
    b.put_i32(0); // forgotten topics
    b.to_vec()
}

#[derive(Debug)]
pub struct FetchPart {
    pub error: i16,
    pub hw: i64,
    pub lso: i64,
    pub aborted: Option<Vec<(i64, i64)>>,
    pub records: Vec<u8>,
}

pub fn parse_fetch_resp_v7(body: &[u8]) -> FetchPart {
    let mut c = std::io::Cursor::new(body);
    let _throttle = c.get_i32();
    let _err = c.get_i16();
    let _session = c.get_i32();
    assert_eq!(c.get_i32(), 1);
    let n = c.get_i16() as usize;
    c.advance(n);
    assert_eq!(c.get_i32(), 1);
    let _idx = c.get_i32();
    let error = c.get_i16();
    let hw = c.get_i64();
    let lso = c.get_i64();
    let _log_start = c.get_i64();
    let na = c.get_i32();
    let aborted = if na < 0 {
        None
    } else {
        let mut v = Vec::new();
        for _ in 0..na {
            v.push((c.get_i64(), c.get_i64()));
        }
        Some(v)
    };
    let rl = c.get_i32() as usize;
    let mut records = vec![0u8; rl];
    c.copy_to_slice(&mut records);
    FetchPart { error, hw, lso, aborted, records }
}
