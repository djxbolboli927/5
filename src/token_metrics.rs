use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct TokenStat {
    pub mint: String,
    pub q_sent: AtomicU64,
    pub route_ok: AtomicU64,
    pub route_fail: AtomicU64,
    pub profitable: AtomicU64,
    pub not_profitable: AtomicU64,
}

pub struct TokenMetrics {
    pub stats: Vec<TokenStat>,
    index: HashMap<String, usize>,
}

#[derive(Clone)]
struct TokenSnapshot {
    mint: String,
    q_sent: u64,
    route_ok: u64,
    route_fail: u64,
    profitable: u64,
    not_profitable: u64,
}

impl TokenMetrics {
    pub fn new(token_mints: &[String]) -> Arc<Self> {
        let stats: Vec<TokenStat> = token_mints
            .iter()
            .map(|m| TokenStat {
                mint: m.clone(),
                q_sent: AtomicU64::new(0),
                route_ok: AtomicU64::new(0),
                route_fail: AtomicU64::new(0),
                profitable: AtomicU64::new(0),
                not_profitable: AtomicU64::new(0),
            })
            .collect();
        let index: HashMap<String, usize> = stats
            .iter()
            .enumerate()
            .map(|(i, s)| (s.mint.clone(), i))
            .collect();
        Arc::new(Self { stats, index })
    }

    pub fn get(&self, mint: &str) -> Option<&TokenStat> {
        self.index.get(mint).and_then(|&i| self.stats.get(i))
    }

    /// Every 5 min: snapshot + reset counters → overwrite /root/c/gozaresh5.json.
    /// Every 30 min (6 windows): accumulate → overwrite /root/c/gozaresh30.json.
    /// Nothing is printed to the terminal.
    pub fn spawn_reporter(self: &Arc<Self>) {
        let m = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            interval.tick().await; // discard the immediate first tick

            // Running 30-min accumulator (same order as stats vec).
            let mut acc: Vec<TokenSnapshot> = m
                .stats
                .iter()
                .map(|s| TokenSnapshot {
                    mint: s.mint.clone(),
                    q_sent: 0,
                    route_ok: 0,
                    route_fail: 0,
                    profitable: 0,
                    not_profitable: 0,
                })
                .collect();

            let mut windows: u32 = 0;

            loop {
                interval.tick().await;
                windows += 1;

                let ts = now_iso();

                // Snapshot and atomically reset every token's counters.
                let snap: Vec<TokenSnapshot> = m
                    .stats
                    .iter()
                    .map(|s| TokenSnapshot {
                        mint: s.mint.clone(),
                        q_sent: s.q_sent.swap(0, Ordering::Relaxed),
                        route_ok: s.route_ok.swap(0, Ordering::Relaxed),
                        route_fail: s.route_fail.swap(0, Ordering::Relaxed),
                        profitable: s.profitable.swap(0, Ordering::Relaxed),
                        not_profitable: s.not_profitable.swap(0, Ordering::Relaxed),
                    })
                    .collect();

                // Add this window into the 30-min accumulator.
                for (a, s) in acc.iter_mut().zip(snap.iter()) {
                    a.q_sent += s.q_sent;
                    a.route_ok += s.route_ok;
                    a.route_fail += s.route_fail;
                    a.profitable += s.profitable;
                    a.not_profitable += s.not_profitable;
                }

                // Overwrite the 5-min file with this window's data.
                write_json("/root/c/gozaresh5.json", &snap, &ts, 5);

                // Every 30 min (6 × 5-min windows): flush the summary file.
                if windows >= 6 {
                    write_json("/root/c/gozaresh30.json", &acc, &ts, 30);
                    for a in acc.iter_mut() {
                        a.q_sent = 0;
                        a.route_ok = 0;
                        a.route_fail = 0;
                        a.profitable = 0;
                        a.not_profitable = 0;
                    }
                    windows = 0;
                }
            }
        });
    }
}

fn write_json(path: &str, snapshots: &[TokenSnapshot], timestamp: &str, window_min: u32) {
    // Sort: profitable desc, then route_ok desc.
    let mut sorted: Vec<&TokenSnapshot> = snapshots.iter().collect();
    sorted.sort_by(|a, b| b.profitable.cmp(&a.profitable).then(b.route_ok.cmp(&a.route_ok)));

    let t_q: u64 = sorted.iter().map(|r| r.q_sent).sum();
    let t_ok: u64 = sorted.iter().map(|r| r.route_ok).sum();
    let t_fail: u64 = sorted.iter().map(|r| r.route_fail).sum();
    let t_prof: u64 = sorted.iter().map(|r| r.profitable).sum();
    let t_noprof: u64 = sorted.iter().map(|r| r.not_profitable).sum();

    let mut tokens_arr = String::new();
    for (i, r) in sorted.iter().enumerate() {
        if i > 0 {
            tokens_arr.push(',');
        }
        tokens_arr.push_str(&format!(
            "\n    {{\
                \"mint\":\"{}\",\
                \"q_sent\":{},\
                \"route_ok\":{},\
                \"route_fail\":{},\
                \"profitable\":{},\
                \"not_profitable\":{}\
            }}",
            r.mint, r.q_sent, r.route_ok, r.route_fail, r.profitable, r.not_profitable
        ));
    }

    let json = format!(
        "{{\n\
          \"updated_at\": \"{timestamp}\",\n\
          \"window_minutes\": {window_min},\n\
          \"total\": {{\
            \"q_sent\": {t_q},\
            \"route_ok\": {t_ok},\
            \"route_fail\": {t_fail},\
            \"profitable\": {t_prof},\
            \"not_profitable\": {t_noprof}\
          }},\n\
          \"tokens\": [{tokens_arr}\n  ]\n}}\n"
    );

    if let Err(e) = std::fs::write(path, &json) {
        eprintln!("token_metrics: failed to write {path}: {e}");
    }
}

fn now_iso() -> String {
    let total_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let s = total_secs % 60;
    let m = (total_secs / 60) % 60;
    let h = (total_secs / 3600) % 24;
    let (year, month, day) = epoch_days_to_ymd(total_secs / 86400);
    format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}Z")
}

fn epoch_days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    let mut year = 1970u64;
    loop {
        let dy = if is_leap(year) { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let month_days: [u64; 12] = if is_leap(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut month = 1u64;
    for &dm in &month_days {
        if days < dm {
            break;
        }
        days -= dm;
        month += 1;
    }
    (year, month, days + 1)
}

fn is_leap(y: u64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}
