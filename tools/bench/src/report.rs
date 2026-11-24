//! `results.jsonl` -> Markdown tables of medians over the repetitions.

use std::collections::BTreeMap;
use std::fmt::Write;

use serde_json::Value;

use crate::workloads::WORKLOADS;

fn median(v: &mut [f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn fmt_us(us: f64) -> String {
    if us >= 10_000.0 {
        format!("{:.1} ms", us / 1000.0)
    } else if us >= 1000.0 {
        format!("{:.2} ms", us / 1000.0)
    } else {
        format!("{us:.0} us")
    }
}

fn fmt_rate(r: f64) -> String {
    if r >= 100.0 {
        format!("{r:.0}")
    } else if r >= 10.0 {
        format!("{r:.1}")
    } else {
        format!("{r:.2}")
    }
}

#[derive(Default)]
struct Cell {
    reps: Vec<Value>,
}

impl Cell {
    fn field(&self, name: &str) -> Vec<f64> {
        self.reps
            .iter()
            .map(|r| r[name].as_f64().unwrap_or(0.0))
            .collect()
    }

    fn med(&self, name: &str) -> f64 {
        median(&mut self.field(name))
    }

    fn range(&self, name: &str) -> (f64, f64) {
        let f = self.field(name);
        (
            f.iter().cloned().fold(f64::INFINITY, f64::min),
            f.iter().cloned().fold(0.0, f64::max),
        )
    }
}

type Key = (String, String, u64, String, u64); // workload, variant, conc, backend, workers

pub fn markdown(lines: &[Value]) -> String {
    let mut cells: BTreeMap<Key, Cell> = BTreeMap::new();
    let mut order: Vec<(String, String, u64)> = Vec::new();
    for r in lines {
        let k = (
            r["workload"].as_str().unwrap_or("").to_string(),
            r["variant"].as_str().unwrap_or("").to_string(),
            r["conc"].as_u64().unwrap_or(0),
            r["backend"].as_str().unwrap_or("").to_string(),
            r["workers"].as_u64().unwrap_or(0),
        );
        let ord = (k.0.clone(), k.1.clone(), k.2);
        if !order.contains(&ord) {
            order.push(ord);
        }
        cells.entry(k).or_default().reps.push(r.clone());
    }
    let mut workers: Vec<u64> = cells.keys().map(|k| k.4).collect();
    workers.sort();
    workers.dedup();
    let mut backends: Vec<String> = cells.keys().map(|k| k.3.clone()).collect();
    backends.sort();
    backends.dedup();
    // The native client first.
    backends.sort_by_key(|b| if b == "native" { 0 } else { 1 });

    let mut out = String::new();
    let mut wl: Vec<&str> = WORKLOADS.to_vec();
    wl.retain(|w| order.iter().any(|o| o.0 == *w));
    for w in wl {
        let _ = writeln!(out, "#### `{w}`\n");
        let _ = writeln!(
            out,
            "| variant | conc | client | threads | ops/s (median) | ops/s min-max | p50 | p90 | p99 | p99.9 | CPU us/op | cores busy | peak RSS | errors | reps |"
        );
        let _ = writeln!(
            out,
            "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
        );
        for (_, variant, conc) in order.iter().filter(|o| o.0 == w) {
            for &wk in &workers {
                for b in &backends {
                    let k = (w.to_string(), variant.clone(), *conc, b.clone(), wk);
                    let Some(c) = cells.get(&k) else { continue };
                    let (lo, hi) = c.range("ops_per_s");
                    let _ = writeln!(
                        out,
                        "| {variant} | {conc} | {b} | {wk} | {} | {}-{} | {} | {} | {} | {} | {:.1} | {:.2} | {:.0} MB | {} | {} |",
                        fmt_rate(c.med("ops_per_s")),
                        fmt_rate(lo),
                        fmt_rate(hi),
                        fmt_us(c.med("p50_us")),
                        fmt_us(c.med("p90_us")),
                        fmt_us(c.med("p99_us")),
                        fmt_us(c.med("p999_us")),
                        c.med("cpu_us_per_op"),
                        c.med("cpu_cores"),
                        c.med("maxrss_mb"),
                        c.med("errors"),
                        c.reps.len(),
                    );
                }
            }
        }
        out.push('\n');
    }

    // Native relative to libfdb_c, at equal application threads.
    if backends.iter().any(|b| b == "native") && backends.iter().any(|b| b == "c") {
        let _ = writeln!(out, "#### Native relative to libfdb_c\n");
        let _ = writeln!(
            out,
            "Ratios of medians, native / libfdb_c, at the same application thread count. Throughput above 1.00 is native faster; latency and CPU below 1.00 is native better.\n"
        );
        let _ = writeln!(
            out,
            "| workload | variant | conc | threads | ops/s | p50 | p99 | CPU/op |"
        );
        let _ = writeln!(out, "|---|---|---:|---:|---:|---:|---:|---:|");
        for (w, variant, conc) in &order {
            for &wk in &workers {
                let n = cells.get(&(w.clone(), variant.clone(), *conc, "native".into(), wk));
                let c = cells.get(&(w.clone(), variant.clone(), *conc, "c".into(), wk));
                let (Some(n), Some(c)) = (n, c) else { continue };
                let r = |f: &str| {
                    let d = c.med(f);
                    if d > 0.0 {
                        format!("{:.2}", n.med(f) / d)
                    } else {
                        "-".into()
                    }
                };
                let _ = writeln!(
                    out,
                    "| {w} | {variant} | {conc} | {wk} | {} | {} | {} | {} |",
                    r("ops_per_s"),
                    r("p50_us"),
                    r("p99_us"),
                    r("cpu_us_per_op"),
                );
            }
        }
        out.push('\n');
    }
    out
}
