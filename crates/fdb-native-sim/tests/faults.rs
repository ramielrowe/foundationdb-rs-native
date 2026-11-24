//! Directed runs of the whole simulation with one fault turned up, under both settings of
//! the stream's write promises: what the sweep exercises now and then, these exercise a lot.

use std::time::Duration;

use fdb_native_sim::NetConfig;
use fdb_native_sim::harness::{SeedConfig, SeedReport, run};

fn sweep(seeds: std::ops::Range<u64>, config: impl Fn(u64) -> SeedConfig) -> Vec<SeedReport> {
    let reports: Vec<SeedReport> = std::thread::scope(|scope| {
        let handles: Vec<_> = seeds
            .map(|seed| {
                let config = config(seed);
                scope.spawn(move || run(&config))
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let failed: Vec<String> = reports
        .iter()
        .filter(|r| !r.passed())
        .map(ToString::to_string)
        .collect();
    assert!(failed.is_empty(), "{}", failed.join("\n"));
    reports
}

/// A network whose client writes often reset the connection mid-write.
fn resetting() -> NetConfig {
    let mut net = NetConfig::default();
    net.write_reset_chance = 0.03;
    net.untrusted_take_chance = 1.0;
    net.partial_write_chance = 0.3;
    net.pending_write_chance = 0.1;
    net
}

/// Streams that declare nothing: failed and pending writes put bytes on the wire (often
/// whole frames), and the client must count them as possibly delivered.
#[test]
fn untrusted_streams_at_most_once() {
    let reports = sweep(0..24, |seed| {
        SeedConfig::new(seed)
            .with_trust(false, false)
            .with_chaos(Duration::from_secs(15))
            .with_net(resetting())
            .with_convergence(false)
    });
    let takes: u64 = reports.iter().map(|r| r.net.untrusted_takes).sum();
    assert!(takes > 50, "only {takes} untrusted takes");
}

/// Streams that keep both promises: a failed or pending write took nothing, and the client
/// may report the requests it never wrote as not sent.
#[test]
fn trusted_streams_at_most_once() {
    let reports = sweep(0..24, |seed| {
        SeedConfig::new(seed)
            .with_trust(true, true)
            .with_chaos(Duration::from_secs(15))
            .with_net(resetting())
            .with_convergence(false)
    });
    assert!(reports.iter().all(|r| r.net.untrusted_takes == 0));
    let resets: u64 = reports.iter().map(|r| r.net.resets).sum();
    assert!(resets > 50, "only {resets} resets");
}
