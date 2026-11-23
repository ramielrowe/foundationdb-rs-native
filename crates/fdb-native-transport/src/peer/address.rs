//! Which address of an endpoint to connect to, and how peers are keyed.

use std::net::SocketAddr;

use fdb_native_wire::{Endpoint, NetworkAddress, NetworkAddressList};

/// What identifies a peer: where to dial and whether to speak TLS. (FDB compares whole
/// `NetworkAddress`es, including the `FLAG_PRIVATE` bit and nothing else; a server has one
/// public address per mode, so the bit never tells two peers apart, and `from_hostname` is
/// bookkeeping.)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PeerKey {
    pub(crate) addr: SocketAddr,
    pub(crate) tls: bool,
}

impl PeerKey {
    pub(crate) fn new(address: &NetworkAddress) -> PeerKey {
        PeerKey {
            addr: address.socket_addr(),
            tls: address.is_tls(),
        }
    }
}

/// `Endpoint::choosePrimaryAddress()` for a **client**: which of an endpoint's addresses to
/// connect to, given whether this process has TLS configured (`local_tls`).
///
/// FDB applies it to every endpoint it deserializes (`FlowTransport.h`, 7.4:169-176 and
/// 8.0:178-185; the decoder here does not, see [`Endpoint`]):
///
/// ```text
/// if (addresses.secondaryAddress.present() &&
///     ((!local.secondaryAddress.present() && addresses.address.isTLS() != local.address.isTLS()) ||
///      (local.secondaryAddress.present() && !addresses.address.isTLS())))
///     swap(addresses.address, addresses.secondaryAddress.get());
/// ```
///
/// A client has a single local address (no secondary), so the rule is: **if the endpoint has
/// a secondary address and its primary's TLS-ness differs from ours, swap them**, so that the
/// address of our own TLS-ness becomes the primary. (A process that itself listens on both a
/// plain and a TLS address, where the second half applies, always prefers the TLS address; a
/// client is not one.) Without a secondary address nothing changes, even if the only
/// address is of the wrong TLS-ness: the connection then fails or is refused as FDB's would.
///
/// `local_tls` is whether this process speaks TLS (`g_network->getLocalAddresses().address
/// .isTLS()`); for a [`PeerManager`](super::PeerManager) it is
/// [`StreamWrapper::tls_enabled`](super::StreamWrapper::tls_enabled).
///
/// ```
/// use fdb_native_transport::peer::choose_primary_address;
/// use fdb_native_wire::{IpAddress, NetworkAddress, NetworkAddressList};
///
/// let ip = IpAddress::V4(0x0a00_0001);
/// let tls = NetworkAddress::new(ip, 4501, true, true);
/// let plain = NetworkAddress::new(ip, 4500, true, false);
/// let list = NetworkAddressList { address: tls, secondary_address: Some(plain) };
///
/// // A client without TLS connects to the plain address; one with TLS keeps the TLS one.
/// assert_eq!(choose_primary_address(&list, false).address, plain);
/// assert_eq!(choose_primary_address(&list, true).address, tls);
/// ```
pub fn choose_primary_address(
    addresses: &NetworkAddressList,
    local_tls: bool,
) -> NetworkAddressList {
    let mut chosen = *addresses;
    if let Some(secondary) = chosen.secondary_address {
        if chosen.address.is_tls() != local_tls {
            chosen.secondary_address = Some(chosen.address);
            chosen.address = secondary;
        }
    }
    chosen
}

/// [`choose_primary_address`] applied to an endpoint's address list.
pub fn choose_primary_endpoint(endpoint: &Endpoint, local_tls: bool) -> Endpoint {
    Endpoint {
        addresses: choose_primary_address(&endpoint.addresses, local_tls),
        token: endpoint.token,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fdb_native_wire::{IpAddress, Uid};

    fn addr(port: u16, tls: bool) -> NetworkAddress {
        NetworkAddress::new(IpAddress::V4(0x0a00_0001), port, true, tls)
    }

    fn list(primary: NetworkAddress, secondary: Option<NetworkAddress>) -> NetworkAddressList {
        NetworkAddressList {
            address: primary,
            secondary_address: secondary,
        }
    }

    #[test]
    fn the_address_of_our_tls_mode_becomes_primary() {
        let plain = addr(4500, false);
        let tls = addr(4501, true);
        // A plain client keeps a plain primary and swaps in the plain secondary.
        assert_eq!(
            choose_primary_address(&list(plain, Some(tls)), false),
            list(plain, Some(tls))
        );
        assert_eq!(
            choose_primary_address(&list(tls, Some(plain)), false),
            list(plain, Some(tls))
        );
        // A TLS client the other way around.
        assert_eq!(
            choose_primary_address(&list(plain, Some(tls)), true),
            list(tls, Some(plain))
        );
        assert_eq!(
            choose_primary_address(&list(tls, Some(plain)), true),
            list(tls, Some(plain))
        );
    }

    #[test]
    fn without_a_secondary_nothing_changes() {
        for (primary_tls, local_tls) in [(false, true), (true, false), (true, true)] {
            let l = list(addr(4500, primary_tls), None);
            assert_eq!(choose_primary_address(&l, local_tls), l);
        }
    }

    #[test]
    fn it_applies_to_endpoints() {
        let token = Uid::new(1, 2);
        let e = Endpoint::new(list(addr(1, true), Some(addr(2, false))), token);
        let chosen = choose_primary_endpoint(&e, false);
        assert_eq!(chosen.addresses.address, addr(2, false));
        assert_eq!(chosen.addresses.secondary_address, Some(addr(1, true)));
        assert_eq!(chosen.token, token);
    }

    #[test]
    fn peer_keys_ignore_the_private_and_hostname_flags() {
        let a = addr(4500, false);
        let mut b = a;
        b.flags |= NetworkAddress::FLAG_PRIVATE;
        b.from_hostname = true;
        assert_eq!(PeerKey::new(&a), PeerKey::new(&b));
        assert_ne!(PeerKey::new(&a), PeerKey::new(&addr(4500, true)));
    }
}
