//! Client quotas and throttling (KIP-13, KIP-124, KIP-219).
//!
//! Three quota types, keyed by user and/or client-id (each optionally `<default>`):
//!   * `producer_byte_rate`  - bytes/sec accepted via Produce
//!   * `consumer_byte_rate`  - bytes/sec served via Fetch
//!   * `request_percentage`  - % of one request-handler thread (100 = one full thread)
//!
//! Rates are measured over a sliding window of `NUM_SAMPLES` one-second samples, the
//! same shape as Kafka's `SampledStat`. When the observed rate O exceeds the quota T the
//! client is throttled for `(O - T) / T * W` where W is the observed window length; the
//! value is returned to the client in `throttle_time_ms` and the broker mutes the
//! connection for that long (KIP-219: for old API versions the response itself is delayed).
//!
//! Lookup precedence for a request from (user U, client C), most specific first:
//!   /users/U/clients/C, /users/U/clients/<default>, /users/U,
//!   /users/<default>/clients/C, /users/<default>/clients/<default>, /users/<default>,
//!   /clients/C, /clients/<default>
//! Each metric is resolved independently (first entry that defines it wins).

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock, RwLock};

use serde::{Deserialize, Serialize};

pub const DEFAULT_ENTITY: &str = "<default>";
pub const NUM_SAMPLES: usize = 11;
const SAMPLE_MS: u64 = 1000;
/// Upper bound on a single throttle so misconfigured quotas cannot wedge clients forever.
pub const MAX_THROTTLE_MS: u32 = 30_000;

/// One quota entry, as configured via broker TOML, controller REST, or heartbeat push.
/// `user` / `client_id` of `None` mean "not part of the entity"; `"<default>"` is the default entity.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct QuotaEntry {
    pub user: Option<String>,
    pub client_id: Option<String>,
    pub producer_byte_rate: Option<f64>,
    pub consumer_byte_rate: Option<f64>,
    pub request_percentage: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Metric {
    Produce,
    Fetch,
    Request,
}

impl QuotaEntry {
    fn limit(&self, m: Metric) -> Option<f64> {
        match m {
            Metric::Produce => self.producer_byte_rate,
            Metric::Fetch => self.consumer_byte_rate,
            Metric::Request => self.request_percentage,
        }
    }
}

type EntityKey = (Option<String>, Option<String>);

#[derive(Default)]
struct Window {
    /// (sample index = now_ms / SAMPLE_MS, accumulated value)
    samples: VecDeque<(u64, f64)>,
}

impl Window {
    fn prune(&mut self, now_ms: u64) {
        let cur = now_ms / SAMPLE_MS;
        while let Some(&(idx, _)) = self.samples.front() {
            if cur >= idx + NUM_SAMPLES as u64 {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn record(&mut self, now_ms: u64, v: f64) {
        self.prune(now_ms);
        let cur = now_ms / SAMPLE_MS;
        match self.samples.back_mut() {
            Some((idx, val)) if *idx == cur => *val += v,
            _ => self.samples.push_back((cur, v)),
        }
    }

    /// Observed rate per second over the window (at least NUM_SAMPLES-1 s, at most NUM_SAMPLES s).
    fn rate_and_window(&mut self, now_ms: u64) -> (f64, f64) {
        self.prune(now_ms);
        let total: f64 = self.samples.iter().map(|s| s.1).sum();
        let first = match self.samples.front() {
            Some(&(idx, _)) => idx * SAMPLE_MS,
            None => return (0.0, 1.0),
        };
        // Like Kafka's Rate: the window is never shorter than (NUM_SAMPLES - 1) full samples,
        // so short bursts are averaged over ~10s before they trigger throttling.
        let span_ms = now_ms
            .saturating_sub(first)
            .max((NUM_SAMPLES as u64 - 1) * SAMPLE_MS)
            .min(NUM_SAMPLES as u64 * SAMPLE_MS);
        let w = span_ms as f64 / 1000.0;
        (total / w, w)
    }
}

pub struct QuotaManager {
    entries: RwLock<Vec<QuotaEntry>>,
    windows: Mutex<HashMap<(Metric, EntityKey, EntityKey), Window>>,
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl QuotaManager {
    pub fn new() -> Self {
        Self { entries: RwLock::new(Vec::new()), windows: Mutex::new(HashMap::new()) }
    }

    /// Replaces the full quota set (entries with no limits are dropped).
    pub fn set_entries(&self, entries: Vec<QuotaEntry>) {
        let entries: Vec<QuotaEntry> = entries
            .into_iter()
            .filter(|e| e.producer_byte_rate.is_some() || e.consumer_byte_rate.is_some() || e.request_percentage.is_some())
            .collect();
        *self.entries.write().unwrap() = entries;
    }

    pub fn entries(&self) -> Vec<QuotaEntry> {
        self.entries.read().unwrap().clone()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }

    fn find(&self, m: Metric, user: &str, client: &str) -> Option<(EntityKey, f64)> {
        let entries = self.entries.read().unwrap();
        let d = DEFAULT_ENTITY;
        // (user selector, client selector); None = dimension absent.
        let order: [(Option<&str>, Option<&str>); 8] = [
            (Some(user), Some(client)),
            (Some(user), Some(d)),
            (Some(user), None),
            (Some(d), Some(client)),
            (Some(d), Some(d)),
            (Some(d), None),
            (None, Some(client)),
            (None, Some(d)),
        ];
        for (u, c) in order {
            for e in entries.iter() {
                if e.user.as_deref() == u && e.client_id.as_deref() == c {
                    if let Some(l) = e.limit(m) {
                        // Usage is tracked per concrete principal/client of the matched dimensions.
                        let key = (u.map(|_| user.to_string()), c.map(|_| client.to_string()));
                        return Some((key, l));
                    }
                }
            }
        }
        None
    }

    /// Records `amount` for the metric and returns the throttle time in ms (0 = none).
    pub fn record(&self, m: Metric, user: &str, client: &str, amount: f64) -> u32 {
        self.record_at(m, user, client, amount, now_ms())
    }

    pub fn record_at(&self, m: Metric, user: &str, client: &str, amount: f64, now: u64) -> u32 {
        let (key, quota) = match self.find(m, user, client) {
            Some(x) => x,
            None => return 0,
        };
        let mut wins = self.windows.lock().unwrap();
        let w = wins.entry((m, key.clone(), (None, None))).or_default();
        w.record(now, amount);
        Self::throttle_for(w, quota, now)
    }

    /// Current throttle for the metric without recording anything new.
    pub fn peek_at(&self, m: Metric, user: &str, client: &str, now: u64) -> u32 {
        let (key, quota) = match self.find(m, user, client) {
            Some(x) => x,
            None => return 0,
        };
        let mut wins = self.windows.lock().unwrap();
        match wins.get_mut(&(m, key, (None, None))) {
            Some(w) => Self::throttle_for(w, quota, now),
            None => 0,
        }
    }

    pub fn peek(&self, m: Metric, user: &str, client: &str) -> u32 {
        self.peek_at(m, user, client, now_ms())
    }

    fn throttle_for(w: &mut Window, quota: f64, now: u64) -> u32 {
        if quota <= 0.0 {
            return MAX_THROTTLE_MS;
        }
        let (rate, window) = w.rate_and_window(now);
        if rate <= quota {
            return 0;
        }
        let secs = (rate - quota) / quota * window;
        ((secs * 1000.0).ceil() as u64).min(MAX_THROTTLE_MS as u64) as u32
    }

    /// Records request-handler time (ns) as percent-of-thread-seconds; returns throttle ms.
    pub fn record_request_time(&self, user: &str, client: &str, elapsed_ns: u64) -> u32 {
        self.record(Metric::Request, user, client, elapsed_ns as f64 / 1e9 * 100.0)
    }

    pub fn record_request_time_at(&self, user: &str, client: &str, elapsed_ns: u64, now: u64) -> u32 {
        self.record_at(Metric::Request, user, client, elapsed_ns as f64 / 1e9 * 100.0, now)
    }
}

/// Serializes tests that mutate the process-global manager.
#[cfg(test)]
pub static TEST_LOCK: Mutex<()> = Mutex::new(());

pub fn manager() -> &'static QuotaManager {
    static M: OnceLock<QuotaManager> = OnceLock::new();
    M.get_or_init(QuotaManager::new)
}

/// KIP-219: from these versions the broker sends the response immediately and mutes the
/// channel instead of delaying the response.
pub fn response_not_delayed(api_key: i16, api_version: i16) -> bool {
    match api_key {
        0 => api_version >= 6,
        1 => api_version >= 8,
        _ => true,
    }
}

/// Per-request state carried from the frame dispatcher into produce/fetch handlers.
#[derive(Debug, Clone, Default)]
pub struct RequestCtx {
    pub client_id: String,
    pub user: String,
    /// Throttle already owed from earlier request-time usage.
    pub base_throttle_ms: u32,
    /// Final throttle decided while handling this request.
    pub throttle_ms: u32,
}

impl RequestCtx {
    pub fn new(client_id: Option<&str>, user: Option<&str>) -> Self {
        Self {
            client_id: client_id.unwrap_or("").to_string(),
            user: user.unwrap_or("ANONYMOUS").to_string(),
            base_throttle_ms: 0,
            throttle_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(u: Option<&str>, c: Option<&str>, p: Option<f64>, f: Option<f64>, r: Option<f64>) -> QuotaEntry {
        QuotaEntry {
            user: u.map(String::from),
            client_id: c.map(String::from),
            producer_byte_rate: p,
            consumer_byte_rate: f,
            request_percentage: r,
        }
    }

    #[test]
    fn no_quota_no_throttle() {
        let m = QuotaManager::new();
        assert_eq!(m.record_at(Metric::Produce, "u", "c", 1e9, 1000), 0);
    }

    #[test]
    fn under_quota_not_throttled_over_quota_is() {
        let m = QuotaManager::new();
        m.set_entries(vec![entry(None, Some("c1"), Some(1000.0), None, None)]);
        // 500 bytes in first second: rate 500/s <= 1000
        assert_eq!(m.record_at(Metric::Produce, "u", "c1", 500.0, 10_000), 0);
        // burst of 10_000 more: 10_500 bytes over a 10s window = 1050/s; (1050-1000)/1000*10s = 0.5s
        let t = m.record_at(Metric::Produce, "u", "c1", 10_000.0, 10_100);
        assert!((499..=501).contains(&t), "t={}", t);
        let t = m.record_at(Metric::Produce, "u", "c1", 100_000.0, 10_200);
        assert!(t > 5000 && t <= MAX_THROTTLE_MS, "t={}", t);
        // other client unaffected
        assert_eq!(m.record_at(Metric::Produce, "u", "other", 1e9, 10_100), 0);
        // fetch not covered by a producer-only quota
        assert_eq!(m.record_at(Metric::Fetch, "u", "c1", 1e9, 10_100), 0);
    }

    #[test]
    fn window_decays_over_time() {
        let m = QuotaManager::new();
        m.set_entries(vec![entry(None, Some("c"), Some(1000.0), None, None)]);
        assert!(m.record_at(Metric::Produce, "u", "c", 20_000.0, 0) > 0);
        // 12 seconds later all samples expired
        assert_eq!(m.peek_at(Metric::Produce, "u", "c", 12_000), 0);
        assert_eq!(m.record_at(Metric::Produce, "u", "c", 100.0, 12_000), 0);
    }

    #[test]
    fn steady_rate_within_quota_never_throttles() {
        let m = QuotaManager::new();
        m.set_entries(vec![entry(None, Some("c"), None, Some(1000.0), None)]);
        for s in 0..30u64 {
            assert_eq!(m.record_at(Metric::Fetch, "u", "c", 900.0, s * 1000 + 5), 0, "sec {}", s);
        }
    }

    #[test]
    fn precedence_most_specific_wins() {
        let m = QuotaManager::new();
        m.set_entries(vec![
            entry(None, Some(DEFAULT_ENTITY), Some(100.0), None, None),
            entry(None, Some("c"), Some(200.0), None, None),
            entry(Some("alice"), None, Some(300.0), None, None),
            entry(Some("alice"), Some("c"), Some(400.0), None, None),
            entry(Some(DEFAULT_ENTITY), None, Some(500.0), None, None),
        ]);
        assert_eq!(m.find(Metric::Produce, "alice", "c").unwrap().1, 400.0);
        assert_eq!(m.find(Metric::Produce, "alice", "x").unwrap().1, 300.0);
        assert_eq!(m.find(Metric::Produce, "bob", "c").unwrap().1, 500.0);
        let m2 = QuotaManager::new();
        m2.set_entries(vec![
            entry(None, Some(DEFAULT_ENTITY), Some(100.0), None, None),
            entry(None, Some("c"), Some(200.0), None, None),
        ]);
        assert_eq!(m2.find(Metric::Produce, "bob", "c").unwrap().1, 200.0);
        assert_eq!(m2.find(Metric::Produce, "bob", "zzz").unwrap().1, 100.0);
    }

    #[test]
    fn default_quota_tracked_per_client_separately() {
        let m = QuotaManager::new();
        m.set_entries(vec![entry(None, Some(DEFAULT_ENTITY), Some(1000.0), None, None)]);
        assert!(m.record_at(Metric::Produce, "u", "a", 50_000.0, 0) > 0);
        assert_eq!(m.record_at(Metric::Produce, "u", "b", 100.0, 0), 0);
    }

    #[test]
    fn request_percentage_quota() {
        let m = QuotaManager::new();
        m.set_entries(vec![entry(None, Some("c"), None, None, Some(5.0))]);
        // 0.05s of handler time = 5 percent-seconds over a 10s window = 0.5% <= 5%
        assert_eq!(m.record_request_time_at("u", "c", 50_000_000, 100), 0);
        // +0.5s => 55 pct-s / 10s = 5.5%: (5.5-5)/5*10s = 1s
        let t = m.record_request_time_at("u", "c", 500_000_000, 200);
        assert!((999..=1001).contains(&t), "t={}", t);
    }

    #[test]
    fn throttle_is_capped() {
        let m = QuotaManager::new();
        m.set_entries(vec![entry(None, Some("c"), Some(1.0), None, None)]);
        assert_eq!(m.record_at(Metric::Produce, "u", "c", 1e12, 0), MAX_THROTTLE_MS);
    }

    #[test]
    fn kip219_versions() {
        assert!(!response_not_delayed(0, 5));
        assert!(response_not_delayed(0, 6));
        assert!(!response_not_delayed(1, 7));
        assert!(response_not_delayed(1, 8));
    }
}
