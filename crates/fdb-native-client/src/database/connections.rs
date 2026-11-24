//! The connections the databases of a process share: a registry of the peer managers in use,
//! keyed by what makes connections interchangeable (see [Sharing
//! connections](crate::database#sharing-connections)).
//!
//! The registry holds weak references only: it keeps no connection, task or runtime alive, and
//! an entry whose manager is shut down (its last database shut down or dropped) is never
//! handed out again; the next database makes a new one. What it compares (the connection
//! knobs and the TLS material, key and password included) is the manager's own
//! ([`PeerManager::config`], [`ClientTls`]'s material), read through the weak reference: none
//! of it outlives the connections. It is one mutex over a short list, taken when a database
//! is opened, never on a request.

use std::any::Any;
use std::sync::{Mutex, PoisonError};

use fdb_native_transport::peer::{DiscardEvents, PeerManager, PeerManagerConfig, WeakPeerManager};
use fdb_native_transport::runtime::Runtime;

use super::{ClientTls, TlsMaterial};
use crate::error::Error;

/// A registered manager, of any runtime type.
trait Registered: Send + Sync {
    /// Whether it can never be shared again (every user left).
    fn is_shut_down(&self) -> bool;
    fn as_any(&self) -> &dyn Any;
}

impl<R: Runtime> Registered for WeakPeerManager<R, ClientTls> {
    fn is_shut_down(&self) -> bool {
        WeakPeerManager::is_shut_down(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

static REGISTRY: Mutex<Vec<Box<dyn Registered>>> = Mutex::new(Vec::new());

/// A user of the connections of a database on `rt` with `config` and `tls`: a share of the
/// registered manager of the same runtime ([`Runtime::same_runtime`]), configuration and TLS
/// material if one is in use, else a new manager (registered for the next databases).
pub(crate) fn shared<R: Runtime>(
    rt: &R,
    config: &PeerManagerConfig,
    tls: &TlsMaterial,
) -> Result<PeerManager<R, ClientTls>, Error> {
    let mut registry = REGISTRY.lock().unwrap_or_else(PoisonError::into_inner);
    registry.retain(|manager| !manager.is_shut_down());
    for manager in registry.iter() {
        let Some(weak) = manager
            .as_any()
            .downcast_ref::<WeakPeerManager<R, ClientTls>>()
        else {
            continue;
        };
        let same = |theirs: &PeerManagerConfig, wrapper: &ClientTls| {
            theirs == config && wrapper.material == *tls
        };
        if let Some(peers) = weak.share_on_if(rt, same) {
            return Ok(peers);
        }
    }
    let peers = own(rt, config, tls)?;
    registry.push(Box::new(peers.downgrade()));
    Ok(peers)
}

/// A manager of its own (not registered, never shared).
pub(crate) fn own<R: Runtime>(
    rt: &R,
    config: &PeerManagerConfig,
    tls: &TlsMaterial,
) -> Result<PeerManager<R, ClientTls>, Error> {
    let wrapper = ClientTls::from_material(tls)?;
    Ok(PeerManager::with_stream_wrapper(
        rt.clone(),
        config.clone(),
        wrapper,
        DiscardEvents,
    ))
}
