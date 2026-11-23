//! At-most-once through the full stack: a chaos server that records every frame it decodes
//! (the wire tap), random drops, closes, refusals, version flips and endpoint failures. Every
//! request that failed as certainly-not-delivered must never have reached the server, none
//! may reach it twice, and every reply must be the request's own. (From the as8.5 re-review;
//! the seed ranges below are reduced for the regular run; the original soak was seeds
//! 41..=140 paused and 200..=207 multi-threaded.)

#![cfg(feature = "tokio")]

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use fdb_native_transport::connection::{PING_REPLY_OK, PingRequest, well_known};
use fdb_native_transport::peer::{PeerHandle, PeerManager, PeerManagerConfig, event_channel};
use fdb_native_transport::rpc::RequestError;
use fdb_native_transport::{Checksum, ConnectPacket, FrameConfig, FrameDecoder};
use fdb_native_wire::{ProtocolVersion, Uid};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

const V74: ProtocolVersion = ProtocolVersion::V7_4;
const V80: ProtocolVersion = ProtocolVersion::V8_0;

#[derive(Clone)]
struct Rng(Arc<Mutex<u64>>);
impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(Arc::new(Mutex::new(seed)))
    }
    fn next(&self) -> u64 {
        let mut s = self.0.lock().unwrap();
        *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&self, n: u64) -> u64 {
        self.next() % n
    }
}

const ENDPOINTS: u64 = 8;
fn endpoint(i: u64) -> Uid {
    Uid::new(0xE000_0000_0000_0000 | 1, i)
}
fn endpoint_index(t: Uid) -> Option<u64> {
    (t.first == (0xE000_0000_0000_0000 | 1)).then_some(t.second)
}

#[derive(Default)]
struct Tap {
    /// value -> number of times a frame carrying it was decoded by the server.
    seen: HashMap<u64, u32>,
    /// value -> number of times the endpoint processed it (not refused).
    delivered: HashMap<u64, u32>,
    /// value -> endpoint it was sent to.
    endpoint_of: HashMap<u64, u64>,
}

/// Server-side endpoint state, shared by all connections (a server process).
#[derive(Default)]
struct Endpoints {
    /// gone: endpoint-not-found for every later request.
    gone: [bool; ENDPOINTS as usize],
    /// unauthorized: refused with an unauthorized notice.
    unauthorized: [bool; ENDPOINTS as usize],
}

struct Chaos {
    tap: Mutex<Tap>,
    eps: Mutex<Endpoints>,
    version: AtomicU64,
    rng: Rng,
    live: Mutex<Vec<mpsc::UnboundedSender<()>>>,
    endpoint_failures: bool,
}

fn frame(cfg: FrameConfig, token: Uid, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    cfg.encode_frame(token, payload, &mut out).unwrap();
    out
}

async fn chaos_conn(
    chaos: Arc<Chaos>,
    stream: tokio::io::DuplexStream,
    listen: std::net::SocketAddr,
) {
    let cfg = FrameConfig::new(Checksum::for_tls(false));
    let (mut rd, mut wr) = tokio::io::split(stream);
    let mut hello = [0u8; ConnectPacket::SIZE];
    if rd.read_exact(&mut hello).await.is_err() {
        return;
    }
    let (client, _) = ConnectPacket::decode(&hello).unwrap();
    let rng = chaos.rng.clone();
    // 10%: hang up before the hello.
    if rng.below(10) == 0 {
        // but keep reading a little: anything that arrives is recorded (it must be nothing).
        let _ = tokio::time::timeout(
            Duration::from_millis(5),
            record_frames(&chaos, &mut rd, cfg),
        )
        .await;
        return;
    }
    let version = ProtocolVersion(chaos.version.load(Ordering::SeqCst));
    let _ = client;
    if wr
        .write_all(&ConnectPacket::server(version, listen).encode().unwrap())
        .await
        .is_err()
    {
        return;
    }
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (kill_tx, mut kill_rx) = mpsc::unbounded_channel::<()>();
    chaos.live.lock().unwrap().push(kill_tx);
    let writer = tokio::spawn(async move {
        while let Some(bytes) = out_rx.recv().await {
            if wr.write_all(&bytes).await.is_err() {
                return;
            }
        }
    });
    let mut decoder = FrameDecoder::new(cfg);
    let mut buf = vec![0u8; 4096];
    'conn: loop {
        let n = tokio::select! {
            r = rd.read(&mut buf) => match r { Ok(0) | Err(_) => break 'conn, Ok(n) => n },
            _ = kill_rx.recv() => break 'conn,
        };
        decoder.push(&buf[..n]);
        while let Some(f) = decoder.next_frame().expect("client frames") {
            let f = f.into_owned();
            if f.token == well_known::PING_PACKET {
                let req: PingRequest = fdb_native_wire::decode(&f.payload).unwrap();
                let _ = out_tx.send(frame(
                    cfg,
                    req.reply.token(),
                    &fdb_native_wire::encode(&PING_REPLY_OK).unwrap(),
                ));
                continue;
            }
            let Ok(req) = fdb_native_wire::decode::<EchoRequest>(&f.payload) else {
                continue;
            };
            let ep = endpoint_index(f.token).expect("an endpoint of the pool");
            {
                let mut tap = chaos.tap.lock().unwrap();
                *tap.seen.entry(req.value).or_default() += 1;
                tap.endpoint_of.insert(req.value, ep);
            }
            let (gone, unauth) = {
                let e = chaos.eps.lock().unwrap();
                (e.gone[ep as usize], e.unauthorized[ep as usize])
            };
            if unauth {
                let _ = out_tx.send(frame(
                    cfg,
                    well_known::UNAUTHORIZED_ENDPOINT,
                    &fdb_native_wire::encode(&f.token).unwrap(),
                ));
                continue;
            }
            if gone {
                let _ = out_tx.send(frame(
                    cfg,
                    well_known::ENDPOINT_NOT_FOUND,
                    &fdb_native_wire::encode(&f.token).unwrap(),
                ));
                continue;
            }
            *chaos
                .tap
                .lock()
                .unwrap()
                .delivered
                .entry(req.value)
                .or_default() += 1;
            let reply = req.reply.token();
            let roll = rng.below(1000);
            match roll {
                0..=699 => {
                    let _ = out_tx.send(frame(cfg, reply, &echo_ok(req.value + ECHO_ADDS)));
                }
                700..=849 => {
                    let tx = out_tx.clone();
                    let delay = Duration::from_millis(rng.below(40));
                    let v = req.value;
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        let _ = tx.send(frame(cfg, reply, &echo_ok(v + ECHO_ADDS)));
                    });
                }
                850..=929 => {}           // silent
                930..=969 => break 'conn, // hang up right after taking it
                970..=979 if chaos.endpoint_failures => {
                    // broken promise: the endpoint is gone from now on
                    chaos.eps.lock().unwrap().gone[ep as usize] = true;
                    let _ = out_tx.send(frame(cfg, reply, &echo_error(1100)));
                }
                980..=984 if chaos.endpoint_failures => {
                    // the endpoint goes away without answering
                    chaos.eps.lock().unwrap().gone[ep as usize] = true;
                }
                _ => {
                    let _ = out_tx.send(frame(cfg, reply, &echo_ok(req.value + ECHO_ADDS)));
                }
            }
        }
    }
    drop(out_tx);
    writer.abort();
}

async fn record_frames<R: tokio::io::AsyncRead + Unpin>(
    chaos: &Chaos,
    rd: &mut R,
    cfg: FrameConfig,
) {
    let mut decoder = FrameDecoder::new(cfg);
    let mut buf = vec![0u8; 4096];
    loop {
        let n = match rd.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        decoder.push(&buf[..n]);
        while let Ok(Some(f)) = decoder.next_frame() {
            if let Ok(req) = fdb_native_wire::decode::<EchoRequest>(f.payload) {
                *chaos.tap.lock().unwrap().seen.entry(req.value).or_default() += 1;
            }
        }
    }
}

fn start_chaos(
    net: &Arc<Net>,
    addr: std::net::SocketAddr,
    seed: u64,
    endpoint_failures: bool,
) -> Arc<Chaos> {
    let chaos = Arc::new(Chaos {
        tap: Mutex::default(),
        eps: Mutex::new(Endpoints {
            gone: [false; 8],
            unauthorized: [
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                endpoint_failures,
            ],
        }),
        version: AtomicU64::new(V74.0),
        rng: Rng::new(seed ^ 0xC4A0),
        live: Mutex::default(),
        endpoint_failures,
    });
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    net.set(
        addr,
        Behavior::Accept {
            tx: tx.clone(),
            capacity: 8 * 1024,
        },
    );
    let c = chaos.clone();
    tokio::spawn(async move {
        while let Some(stream) = rx.recv().await {
            tokio::spawn(chaos_conn(c.clone(), stream, addr));
        }
    });
    // keep tx alive in the net
    let _ = tx;
    chaos
}

#[derive(Debug, Clone)]
enum Outcome {
    Ok(u64),
    Err(RequestError),
    Dropped,
}

async fn one_request(peer: &PeerHandle, ep: u64, value: u64, mode: u64, rng: &Rng) -> Outcome {
    let fut = peer.request_as::<EchoReply, _>(endpoint(ep), echo_encode(value));
    match mode {
        0..=5 => match fut.await {
            Ok(r) => Outcome::Ok(r.value),
            Err(e) => Outcome::Err(e),
        },
        6..=8 => {
            let after = Duration::from_millis(rng.below(30));
            match tokio::time::timeout(after, fut).await {
                Ok(Ok(r)) => Outcome::Ok(r.value),
                Ok(Err(e)) => Outcome::Err(e),
                Err(_) => Outcome::Dropped,
            }
        }
        _ => {
            let mut fut = Box::pin(fut);
            match futures_lite::future::poll_once(&mut fut).await {
                Some(Ok(r)) => Outcome::Ok(r.value),
                Some(Err(e)) => Outcome::Err(e),
                None => Outcome::Dropped,
            }
        }
    }
}

struct Stats {
    ok: usize,
    errs: HashMap<String, usize>,
    dropped: usize,
}

async fn stress(seed: u64, clients: u64, per_client: u64, endpoint_failures: bool) -> Stats {
    let net = Net::new();
    let addr = sock(4500);
    let chaos = start_chaos(&net, addr, seed, endpoint_failures);
    let (sink, mut events) = event_channel();
    let mut c = PeerManagerConfig::default().without_jitter();
    c.seed = Some(seed);
    c.command_queue_len = 16;
    c.max_pending_output = 4096;
    c.handshake_timeout = Duration::from_millis(500);
    let mgr = PeerManager::new(MemRuntime::new(&net), c, sink);
    let rng = Rng::new(seed);
    let outcomes: Arc<Mutex<HashMap<u64, Outcome>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));

    let chaos_task = {
        let rng = rng.clone();
        let chaos = chaos.clone();
        let net = net.clone();
        let peer = mgr.peer(&na(addr, false));
        let stop = stop.clone();
        tokio::spawn(async move {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<tokio::io::DuplexStream>();
            let _ = tx;
            while !stop.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(rng.below(25))).await;
                match rng.below(10) {
                    0 | 1 => peer.close_connection(),
                    2 | 3 => {
                        let live = std::mem::take(&mut *chaos.live.lock().unwrap());
                        for k in live {
                            let _ = k.send(());
                        }
                    }
                    4 => {
                        let v = if chaos.version.load(Ordering::SeqCst) == V74.0 {
                            V80
                        } else {
                            V74
                        };
                        chaos.version.store(v.0, Ordering::SeqCst);
                    }
                    _ => {}
                }
            }
            let _ = net;
        })
    };

    let mut tasks = Vec::new();
    for client in 0..clients {
        let peer = mgr.peer(&na(addr, false));
        let rng = Rng::new(seed.wrapping_mul(31).wrapping_add(client));
        let outcomes = outcomes.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..per_client {
                let value = client * 1_000_000 + i;
                let ep = rng.below(ENDPOINTS);
                let mode = rng.below(10);
                let out = one_request(&peer, ep, value, mode, &rng).await;
                outcomes.lock().unwrap().insert(value, out);
                if rng.below(4) == 0 {
                    tokio::time::sleep(Duration::from_millis(rng.below(5))).await;
                }
            }
        }));
    }
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(600), t)
            .await
            .expect("a client hung: some request never completed")
            .unwrap();
    }
    stop.store(true, Ordering::SeqCst);
    chaos_task.await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), mgr.shutdown())
        .await
        .expect("shutdown hung");
    // Let in-flight server reads settle.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut kinds: std::collections::BTreeMap<String, usize> = Default::default();
    while let Some(rec) = events.try_next() {
        let k: String = format!("{:?}", rec.event)
            .split([' ', '(', '{'])
            .next()
            .unwrap()
            .to_string();
        let k = match &rec.event {
            fdb_native_transport::peer::PeerEvent::Closed {
                error, established, ..
            } => format!(
                "Closed/{}/{}",
                established,
                format!("{error:?}").chars().take(28).collect::<String>()
            ),
            _ => k,
        };
        *kinds.entry(k).or_default() += 1;
    }
    if seed == 1 || seed == 2 || seed == 100 {
        println!("seed {seed} events: {kinds:?}");
    }
    let tap = chaos.tap.lock().unwrap();
    let outcomes = outcomes.lock().unwrap();
    let mut stats = Stats {
        ok: 0,
        errs: HashMap::new(),
        dropped: 0,
    };
    for (value, seen) in &tap.seen {
        assert!(
            *seen <= 1,
            "seed {seed}: request {value} reached the server {seen} times"
        );
    }
    for (value, out) in outcomes.iter() {
        let seen = tap.seen.get(value).copied().unwrap_or(0);
        let delivered = tap.delivered.get(value).copied().unwrap_or(0);
        match out {
            Outcome::Ok(v) => {
                stats.ok += 1;
                assert_eq!(
                    *v,
                    value + ECHO_ADDS,
                    "seed {seed}: misrouted reply for {value}"
                );
                assert_eq!(
                    delivered, 1,
                    "seed {seed}: a reply for {value}, which was never delivered"
                );
            }
            Outcome::Err(e) => {
                *stats
                    .errs
                    .entry(
                        format!("{:?}", std::mem::discriminant(e))
                            .chars()
                            .take(0)
                            .collect::<String>()
                            + &short(e),
                    )
                    .or_default() += 1;
                if e.certainly_not_delivered() {
                    match e {
                        RequestError::Unauthorized { .. } => assert_eq!(
                            delivered, 0,
                            "seed {seed}: {value} completed {e:?} but was delivered"
                        ),
                        _ => assert_eq!(
                            seen, 0,
                            "seed {seed}: {value} completed {e:?} (certainly not delivered) but the server got it"
                        ),
                    }
                }
                assert!(
                    !matches!(
                        e,
                        RequestError::Backpressure | RequestError::TooManyPending { .. }
                    ),
                    "seed {seed}: backpressure surfaced: {e:?}"
                );
            }
            Outcome::Dropped => stats.dropped += 1,
        }
    }
    stats
}

fn short(e: &RequestError) -> String {
    let s = format!("{e:?}");
    s.split(['(', ' ', '{']).next().unwrap_or("").to_string()
        + match e {
            RequestError::EndpointNotFound {
                maybe_delivered: true,
                ..
            } => "/maybe",
            RequestError::EndpointNotFound {
                maybe_delivered: false,
                ..
            } => "/not",
            _ => "",
        }
}

#[tokio::test(start_paused = true)]
async fn wiretap_stress_paused_many_seeds() {
    let mut total_ok = 0;
    let mut total: HashMap<String, usize> = HashMap::new();
    let mut dropped = 0;
    for seed in 41..=100u64 {
        let s = stress(seed, 12, 150, seed % 2 == 0).await;
        total_ok += s.ok;
        dropped += s.dropped;
        for (k, v) in s.errs {
            *total.entry(k).or_default() += v;
        }
    }
    println!("paused: ok {total_ok} dropped {dropped} errors {total:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiretap_stress_multi_thread() {
    let mut total_ok = 0;
    let mut total: HashMap<String, usize> = HashMap::new();
    let mut dropped = 0;
    for seed in 200..=201u64 {
        let s = stress(seed, 16, 100, seed % 2 == 0).await;
        total_ok += s.ok;
        dropped += s.dropped;
        for (k, v) in s.errs {
            *total.entry(k).or_default() += v;
        }
    }
    println!("multi: ok {total_ok} dropped {dropped} errors {total:?}");
}
