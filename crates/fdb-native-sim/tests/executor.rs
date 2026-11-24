//! The deterministic executor and [`SimRuntime`] against the `Runtime` contract: virtual
//! time, seeded scheduling, aborts, panics, detaching, deadlocks and node bookkeeping.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fdb_native_sim::{Sim, SimError, SimRuntime, TraceLevel};
use fdb_native_transport::runtime::{JoinError, Runtime, TaskHandle};

fn sim(seed: u64) -> (Sim, SimRuntime) {
    let sim = Sim::new(seed, TraceLevel::Quiet);
    let node = sim.add_node("client", IpAddr::V4(Ipv4Addr::new(10, 0, 9, 1)));
    let rt = SimRuntime::new(&sim, node);
    (sim, rt)
}

/// Yields once to the executor.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn time_is_virtual_and_moves_only_when_idle() {
    let (sim, rt) = sim(1);
    let wall = std::time::Instant::now();
    let t0 = rt.now();
    sim.block_on(async {
        // Busy tasks do not move the clock.
        let busy = rt.spawn(async {
            for _ in 0..100 {
                yield_now().await;
            }
        });
        busy.await.unwrap();
        assert_eq!(rt.now(), t0);
        rt.sleep(Duration::from_secs(30)).await;
        assert_eq!(rt.now() - t0, Duration::from_secs(30), "exact, no rounding");
        // `sleep_until` is absolute; a past deadline completes at once.
        rt.sleep_until(t0 + Duration::from_secs(90)).await;
        assert_eq!(rt.now() - t0, Duration::from_secs(90));
        let before = rt.now();
        rt.sleep_until(t0).await;
        assert_eq!(rt.now(), before);
    })
    .unwrap();
    assert!(wall.elapsed() < Duration::from_secs(5));
}

#[test]
fn a_sleep_fixes_its_deadline_when_created() {
    let (sim, rt) = sim(1);
    sim.block_on(async {
        let t0 = rt.now();
        let sleep = rt.sleep(Duration::from_secs(1));
        rt.sleep(Duration::from_millis(600)).await;
        sleep.await;
        assert_eq!(rt.now() - t0, Duration::from_secs(1));
    })
    .unwrap();
}

/// The order in which ready tasks run is a function of the seed: the same seed gives the
/// same interleaving, other seeds other ones.
#[test]
fn scheduling_is_seeded() {
    let order = |seed| {
        let (sim, rt) = sim(seed);
        let log = Arc::new(Mutex::new(Vec::new()));
        sim.block_on(async {
            let tasks: Vec<_> = (0..8)
                .map(|i| {
                    let log = log.clone();
                    rt.spawn(async move {
                        for step in 0..4 {
                            log.lock().unwrap().push((i, step));
                            yield_now().await;
                        }
                    })
                })
                .collect();
            for t in tasks {
                t.await.unwrap();
            }
        })
        .unwrap();
        let log = log.lock().unwrap().clone();
        (log, sim.trace_hash())
    };
    let (a, hash_a) = order(42);
    let (b, hash_b) = order(42);
    assert_eq!(a, b);
    assert_eq!(hash_a, hash_b);
    let others: Vec<_> = (0..5).map(order).collect();
    assert!(
        others.iter().any(|(o, _)| *o != a),
        "every seed ran the same order"
    );
    assert!(others.iter().all(|(_, h)| *h != hash_a));
}

#[test]
fn abort_drops_the_task_at_its_next_suspension_point() {
    let (sim, rt) = sim(3);
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = SetOnDrop(dropped.clone());
    sim.block_on(async {
        let r = rt.clone();
        let task = rt.spawn(async move {
            let _guard = guard;
            r.sleep(Duration::from_secs(3600)).await;
        });
        rt.sleep(Duration::from_millis(1)).await;
        assert!(!task.is_finished());
        task.abort();
        // Not inside `abort`: the loop drops it.
        assert!(!dropped.load(Ordering::SeqCst));
        task.abort();
        assert_eq!(task.await, Err(JoinError::Aborted));
        assert!(dropped.load(Ordering::SeqCst));
    })
    .unwrap();
    assert!(sim.live_tasks(rt.node()).is_empty());
}

#[test]
fn dropping_a_handle_detaches_the_task() {
    let (sim, rt) = sim(4);
    let done = Arc::new(AtomicBool::new(false));
    let flag = done.clone();
    sim.block_on(async {
        let r = rt.clone();
        drop(rt.spawn(async move {
            r.sleep(Duration::from_secs(10)).await;
            flag.store(true, Ordering::SeqCst);
        }));
        rt.sleep(Duration::from_secs(11)).await;
    })
    .unwrap();
    assert!(done.load(Ordering::SeqCst));
}

#[test]
fn a_panicking_task_is_reported_and_recorded() {
    let (sim, rt) = sim(5);
    sim.block_on(async {
        let task = rt.spawn(async { panic!("boom") });
        assert_eq!(task.await, Err(JoinError::Panicked));
        assert_eq!(rt.spawn(async { 7 }).await, Ok(7));
    })
    .unwrap();
    assert_eq!(sim.panics(), vec!["boom".to_owned()]);
}

#[test]
fn a_future_that_can_never_finish_is_a_deadlock() {
    let (sim, rt) = sim(6);
    let r = rt.clone();
    let _parked = rt.spawn(async move {
        std::future::pending::<()>().await;
        drop(r);
    });
    let err = sim.block_on(std::future::pending::<()>()).unwrap_err();
    assert!(
        matches!(err, SimError::Deadlock { ref live, .. } if live.len() == 1),
        "{err}"
    );
}

#[test]
fn a_time_limit_ends_a_run() {
    let (sim, rt) = sim(7);
    let err = sim
        .block_on_within(Duration::from_secs(5), rt.sleep(Duration::from_secs(60)))
        .unwrap_err();
    assert_eq!(
        err,
        SimError::TimeLimit {
            limit: Duration::from_secs(5)
        }
    );
}

/// `live_tasks` lists a node's unfinished tasks with where they were spawned (the leak check
/// of the harness); killing a node aborts them.
#[test]
fn live_tasks_and_killing_a_node() {
    let (sim, rt) = sim(8);
    let other = sim.add_node("server", IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1)));
    let server_rt: SimRuntime = SimRuntime::new(&sim, other);
    let r = rt.clone();
    let _forever = rt.spawn(async move { r.sleep(Duration::from_secs(3600)).await });
    let s = server_rt.clone();
    let _also = server_rt.spawn(async move { s.sleep(Duration::from_secs(3600)).await });
    sim.run_for(Duration::from_secs(1)).unwrap();
    let live = sim.live_tasks(rt.node());
    assert_eq!(live.len(), 1);
    assert!(
        live[0].location.contains("tests/executor.rs"),
        "{}",
        live[0]
    );
    sim.kill_node(other, fdb_native_sim::KillMode::Reboot);
    sim.run_for(Duration::from_millis(1)).unwrap();
    assert!(sim.live_tasks(other).is_empty());
    assert_eq!(sim.live_tasks(rt.node()).len(), 1);
}
