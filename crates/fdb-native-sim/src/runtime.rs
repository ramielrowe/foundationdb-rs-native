//! [`SimRuntime`]: the transport's [`Runtime`] over a [`Sim`].

use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use fdb_native_transport::runtime::{Instant, Runtime};

use crate::executor::{NodeId, Sim, SimTask};
use crate::net::{self, SimStream, Trust};

/// The [`Runtime`] of one node of a [`Sim`]: its tasks run on the simulation's executor
/// (tagged with the node), its clock is the virtual one and its connections are the
/// simulated network's, dialed from the node's address.
///
/// The const parameters are the stream's write promises,
/// [`WRITE_ERRORS_TAKE_NOTHING`](Runtime::WRITE_ERRORS_TAKE_NOTHING) and
/// [`PENDING_TAKES_NOTHING`](Runtime::PENDING_TAKES_NOTHING). The streams keep exactly
/// what they declare, and use every freedom the declaration leaves them (see
/// [Writes and at-most-once](crate::net#writes-and-at-most-once)): with `true` a failed or
/// pending write took nothing; with `false` it may have put bytes on the wire.
pub struct SimRuntime<
    const ERRORS_TAKE_NOTHING: bool = true,
    const PENDING_TAKES_NOTHING: bool = true,
> {
    sim: Sim,
    node: NodeId,
}

impl<const E: bool, const P: bool> SimRuntime<E, P> {
    /// The runtime of `node`.
    pub fn new(sim: &Sim, node: NodeId) -> Self {
        SimRuntime {
            sim: sim.clone(),
            node,
        }
    }

    /// The node.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// The simulation.
    pub fn sim(&self) -> &Sim {
        &self.sim
    }
}

impl<const E: bool, const P: bool> Clone for SimRuntime<E, P> {
    fn clone(&self) -> Self {
        SimRuntime {
            sim: self.sim.clone(),
            node: self.node,
        }
    }
}

impl<const E: bool, const P: bool> fmt::Debug for SimRuntime<E, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimRuntime")
            .field("node", &self.node)
            .field("write_errors_take_nothing", &E)
            .field("pending_takes_nothing", &P)
            .finish()
    }
}

impl<const E: bool, const P: bool> Runtime for SimRuntime<E, P> {
    type Stream = SimStream;
    type Task<T: Send + 'static> = SimTask<T>;

    const WRITE_ERRORS_TAKE_NOTHING: bool = E;
    const PENDING_TAKES_NOTHING: bool = P;

    /// Virtual timers fire exactly at their deadline.
    const TIMER_RESOLUTION: Duration = Duration::ZERO;

    /// The same node of the same simulation.
    fn same_runtime(&self, other: &Self) -> bool {
        self.sim.same_sim(&other.sim) && self.node == other.node
    }

    fn now(&self) -> Instant {
        self.sim.now()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.sim.sleep(duration)
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        self.sim.sleep_until(deadline)
    }

    #[track_caller]
    fn spawn<T, F>(&self, future: F) -> SimTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.sim.spawn_on(self.node, future)
    }

    fn connect(&self, addr: SocketAddr) -> impl Future<Output = io::Result<SimStream>> + Send {
        net::connect(
            self.sim.shared.clone(),
            self.node,
            addr,
            Trust {
                errors_take_nothing: E,
                pending_takes_nothing: P,
            },
        )
    }

    async fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        let latency = Duration::from_micros(100);
        self.sim.sleep(latency).await;
        net::resolve(&self.sim.shared, host, port)
    }
}
