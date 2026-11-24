//! The simulated network: delivery, latency and every fault, and the write promises of the
//! streams under both settings of the `Runtime` safety constants.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use fdb_native_sim::{KillMode, NetConfig, NodeId, Sim, SimRuntime, SimStream, TraceLevel};
use fdb_native_transport::runtime::Runtime;
use futures_io::AsyncWrite;
use futures_lite::future::{or, poll_fn};
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};

const SERVER: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1)), 4500);

struct World {
    sim: Sim,
    client: NodeId,
    server: NodeId,
}

fn world(seed: u64, net: NetConfig) -> World {
    let sim = Sim::with_net(seed, TraceLevel::Quiet, net);
    let client = sim.add_node("client", IpAddr::V4(Ipv4Addr::new(10, 0, 9, 1)));
    let server = sim.add_node("server", SERVER.ip());
    World {
        sim,
        client,
        server,
    }
}

/// Accepts one connection and reads everything until EOF or an error; returns the bytes.
fn reader(w: &World) -> fdb_native_sim::SimTask<(Vec<u8>, Option<io::ErrorKind>)> {
    let listener = w.sim.listen(w.server, SERVER);
    w.sim.spawn_on(w.server, async move {
        let mut stream = listener.accept().await.expect("a connection");
        let mut got = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) => return (got, None),
                Ok(n) => got.extend_from_slice(&buf[..n]),
                Err(e) => return (got, Some(e.kind())),
            }
        }
    })
}

fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 + i / 251) as u8).collect()
}

#[test]
fn bytes_arrive_in_order_through_partial_writes_and_short_reads() {
    let mut net = NetConfig::default();
    net.partial_write_chance = 0.5;
    net.short_read_chance = 0.5;
    net.pending_write_chance = 0.2;
    net.slow_chance = 0.1;
    net.pipe_capacity = 4096;
    let w = world(1, net);
    let rt: SimRuntime = SimRuntime::new(&w.sim, w.client);
    let server = reader(&w);
    let data = pattern(200_000);
    let (got, err) = w
        .sim
        .block_on(async {
            let mut stream = rt.connect(SERVER).await.unwrap();
            stream.write_all(&data).await.unwrap();
            stream.close().await.unwrap();
            server.await.unwrap()
        })
        .unwrap();
    assert_eq!(err, None);
    assert!(
        got == data,
        "{} of {} bytes, in order",
        got.len(),
        data.len()
    );
    let stats = w.sim.net_stats();
    assert!(stats.partial_writes > 0 && stats.pending_writes > 0);
}

#[test]
fn latency_and_clogging_delay_delivery() {
    let mut net = NetConfig::default();
    net.latency = Duration::from_millis(10);
    net.jitter = Duration::ZERO;
    let w = world(2, net);
    let rt: SimRuntime = SimRuntime::new(&w.sim, w.client);
    let listener = w.sim.listen(w.server, SERVER);
    let sim = w.sim.clone();
    w.sim
        .block_on(async {
            let mut client = rt.connect(SERVER).await.unwrap();
            let mut server = listener.accept().await.unwrap();
            let t0 = sim.now();
            client.write_all(b"ping").await.unwrap();
            let mut buf = [0u8; 4];
            server.read_exact(&mut buf).await.unwrap();
            assert_eq!(sim.now() - t0, Duration::from_millis(10));
            // A clogged address: what is sent through it arrives once the clog is over.
            sim.clog_ip(SERVER.ip(), Duration::from_secs(2));
            let t1 = sim.now();
            client.write_all(b"pong").await.unwrap();
            server.read_exact(&mut buf).await.unwrap();
            assert_eq!(sim.now() - t1, Duration::from_secs(2));
        })
        .unwrap();
}

#[test]
fn connects_are_refused_without_a_listener_and_hang_to_a_dead_machine() {
    let w = world(3, NetConfig::default());
    let rt: SimRuntime = SimRuntime::new(&w.sim, w.client);
    let sim = w.sim.clone();
    w.sim
        .block_on(async {
            let err = rt.connect(SERVER).await.map(drop).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
            sim.kill_node(w.server, KillMode::PowerOff);
            let hung = or(async { rt.connect(SERVER).await.map(|_| false) }, async {
                rt.sleep(Duration::from_secs(30)).await;
                Ok(true)
            })
            .await
            .unwrap();
            assert!(hung, "a connect to a powered-off machine hangs");
        })
        .unwrap();
}

#[test]
fn a_partition_holds_data_until_it_heals() {
    let w = world(4, NetConfig::default());
    let rt: SimRuntime = SimRuntime::new(&w.sim, w.client);
    let listener = w.sim.listen(w.server, SERVER);
    let sim = w.sim.clone();
    let client_ip = w.sim.node_ip(w.client);
    w.sim
        .block_on(async {
            let mut client = rt.connect(SERVER).await.unwrap();
            let mut server = listener.accept().await.unwrap();
            sim.partition(&[client_ip], &[SERVER.ip()]);
            client.write_all(b"held").await.unwrap();
            let mut buf = [0u8; 4];
            let read = or(
                async { server.read_exact(&mut buf).await.map(|_| true) },
                async {
                    rt.sleep(Duration::from_secs(60)).await;
                    Ok(false)
                },
            )
            .await
            .unwrap();
            assert!(!read, "nothing crosses a partition");
            sim.heal_partition(&[client_ip], &[SERVER.ip()]);
            server.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"held");
        })
        .unwrap();
}

#[test]
fn a_reset_cuts_the_stream_and_both_ends_see_errors() {
    let mut net = NetConfig::default();
    net.latency = Duration::from_millis(5);
    let w = world(5, net);
    let rt: SimRuntime = SimRuntime::new(&w.sim, w.client);
    let server = reader(&w);
    let sim = w.sim.clone();
    let data = pattern(100_000);
    let (got, err, write_err) = w
        .sim
        .block_on(async {
            let mut client = rt.connect(SERVER).await.unwrap();
            client.write_all(&data).await.unwrap();
            // In flight: reset it.
            sim.reset_connection(client.connection_id());
            let write_err = client.write_all(b"more").await.unwrap_err().kind();
            let (got, err) = server.await.unwrap();
            (got, err, write_err)
        })
        .unwrap();
    assert_eq!(write_err, io::ErrorKind::ConnectionReset);
    assert_eq!(err, Some(io::ErrorKind::ConnectionReset));
    assert!(got.len() < data.len(), "the reset cut what was in flight");
    assert_eq!(got, data[..got.len()], "what arrived is a prefix");
}

#[test]
fn a_half_open_connection_swallows_writes_and_never_ends() {
    let w = world(6, NetConfig::default());
    let rt: SimRuntime = SimRuntime::new(&w.sim, w.client);
    let listener = w.sim.listen(w.server, SERVER);
    let sim = w.sim.clone();
    w.sim
        .block_on(async {
            let mut client = rt.connect(SERVER).await.unwrap();
            let _server = listener.accept().await.unwrap();
            sim.half_open_connection(client.connection_id());
            client.write_all(b"into the void").await.unwrap();
            let mut buf = [0u8; 1];
            let ended = or(async { client.read(&mut buf).await.map(|_| true) }, async {
                rt.sleep(Duration::from_secs(600)).await;
                Ok(false)
            })
            .await
            .unwrap();
            assert!(!ended, "no data, no EOF, no error: only a timeout can tell");
        })
        .unwrap();
}

#[test]
fn a_crash_ends_connections_and_a_power_off_leaves_them_half_open() {
    for (mode, ends) in [(KillMode::Reboot, true), (KillMode::PowerOff, false)] {
        let w = world(7, NetConfig::default());
        let rt: SimRuntime = SimRuntime::new(&w.sim, w.client);
        let listener = w.sim.listen(w.server, SERVER);
        let sim = w.sim.clone();
        let accepted = Arc::new(Mutex::new(None::<SimStream>));
        let keep = accepted.clone();
        let server_task = w.sim.spawn_on(w.server, async move {
            *keep.lock().unwrap() = listener.accept().await;
            std::future::pending::<()>().await;
        });
        w.sim
            .block_on(async {
                let mut client = rt.connect(SERVER).await.unwrap();
                rt.sleep(Duration::from_millis(10)).await;
                sim.kill_node(w.server, mode);
                let mut buf = [0u8; 1];
                let ended = or(async { client.read(&mut buf).await.map(|_| true) }, async {
                    rt.sleep(Duration::from_secs(60)).await;
                    Ok(false)
                })
                .await;
                assert_eq!(ended.unwrap_or(true), ends, "{mode:?}");
            })
            .unwrap();
        drop(server_task);
    }
}

/// Writes `data` with raw `poll_write` calls on a stream whose writes often reset the
/// connection or are pending; returns how many bytes the writes reported taken, and what
/// the server received.
fn reported_and_received<const E: bool, const P: bool>(seed: u64) -> (usize, Vec<u8>, Vec<String>) {
    let mut net = NetConfig::default();
    net.write_reset_chance = 0.02;
    net.pending_write_chance = 0.3;
    net.untrusted_take_chance = 1.0;
    net.partial_write_chance = 0.3;
    let w = world(seed, net);
    let rt = SimRuntime::<E, P>::new(&w.sim, w.client);
    let server = reader(&w);
    let data = pattern(50_000);
    let reported = w
        .sim
        .block_on(async {
            let mut stream = rt.connect(SERVER).await.unwrap();
            let mut at = 0;
            while at < data.len() {
                let chunk = &data[at..(at + 1000).min(data.len())];
                let written = poll_fn(|cx| match Pin::new(&mut stream).poll_write(cx, chunk) {
                    // A pending write is retried with the same bytes, as the contract says.
                    Poll::Pending => Poll::Pending,
                    ready => ready,
                })
                .await;
                match written {
                    Ok(n) => at += n,
                    Err(_) => break,
                }
            }
            drop(stream);
            at
        })
        .unwrap();
    let (got, _) = w.sim.block_on(server).unwrap().unwrap();
    (reported, got, w.sim.net_violations())
}

/// With `WRITE_ERRORS_TAKE_NOTHING` and `PENDING_TAKES_NOTHING`, exactly what the writes
/// reported arrives (at most: a reset may cut it); nothing a failed or pending write was
/// offered does.
#[test]
fn trusted_streams_take_nothing_on_errors_or_pending() {
    for seed in 0..20 {
        let (reported, got, violations) = reported_and_received::<true, true>(seed);
        assert!(violations.is_empty(), "{violations:?}");
        assert!(
            got.len() <= reported,
            "seed {seed}: {} > {reported}",
            got.len()
        );
        assert_eq!(got, pattern(got.len()));
    }
}

/// Without the promises the stream uses its freedom: bytes of failed (and pending) writes
/// arrive although no write reported them.
#[test]
fn untrusted_streams_deliver_what_failed_writes_took() {
    let mut beyond = 0;
    for seed in 0..20 {
        let (reported, got, violations) = reported_and_received::<false, false>(seed);
        assert!(violations.is_empty(), "{violations:?}");
        assert_eq!(
            got,
            pattern(got.len()),
            "a prefix, in order, never duplicated"
        );
        if got.len() > reported {
            beyond += 1;
        }
    }
    assert!(
        beyond > 0,
        "no failed write ever delivered unreported bytes"
    );
}

/// After an untrusted pending write that took bytes, the next write must offer the same
/// bytes again; anything else is recorded as a breach of the stream contract.
#[test]
fn a_pending_write_must_be_retried_with_the_same_bytes() {
    let mut net = NetConfig::default();
    net.pending_write_chance = 1.0;
    net.untrusted_take_chance = 1.0;
    let w = world(9, net);
    let rt = SimRuntime::<true, false>::new(&w.sim, w.client);
    let _server = reader(&w);
    w.sim
        .block_on(async {
            let mut stream = rt.connect(SERVER).await.unwrap();
            let mut first = true;
            poll_fn(|cx| {
                let buf: &[u8] = if first { b"abcdef" } else { b"zzzzzz" };
                first = false;
                match Pin::new(&mut stream).poll_write(cx, buf) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(r) => Poll::Ready(r),
                }
            })
            .await
            .unwrap();
        })
        .unwrap();
    assert_eq!(w.sim.net_violations().len(), 1);
}
