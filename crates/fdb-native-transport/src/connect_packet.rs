//! FlowTransport's connection handshake packet.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use fdb_native_wire::ProtocolVersion;
use fdb_native_wire::binary::{DecodeError, EncodeError, Reader, Writer};

/// FlowTransport's `ConnectPacket` (`fdbrpc/FlowTransport.actor.cpp`; `FlowTransport.cpp` in
/// 8.0): the first bytes each side sends on a new connection, written with a `PacketWriter`
/// (the binary format, unversioned) and read with a `BinaryReader` that assumes the version
/// the packet itself carries. The struct is the same in 7.4 and 8.0.
///
/// ```text
/// u32 connectPacketLength    bytes that follow (40)
/// u64 protocolVersion        the sender's version, with the object-serializer flag
/// u16 canonicalRemotePort    where the sender listens (0 for a client)
/// u64 connectionId           see below
/// u32 canonicalRemoteIp4     IPv4 address, host order (the 4 bytes read as a u32, LE)
/// u16 flags                  bit 0: the address is IPv6
/// u8  canonicalRemoteIp6[16] IPv6 address, network order
/// ```
///
/// # Connection ids
///
/// `connectionId` is the sender's `transportId`. A plain client sends 0
/// ([`ConnectPacket::client`]); `fdbserver` and the simulator send 1; the multi-version
/// client API picks a random id above 1 for the connections of one client, so that a server
/// can tell they belong together ([`ConnectPacket::multi_version_client`]). A receiver
/// treats only ids above 1 as multi-version (`connectionId > 1`).
///
/// # Reading and validating
///
/// A receiver rejects a length above 40 (`serialization_failed`); the server reads 14 bytes
/// first (the `CONNECT_PACKET_V0_SIZE` packet) to learn the length, so a sender must send at
/// least that much. [`decode`](ConnectPacket::decode) reads the 44-byte form 7.4 and 8.0
/// send and checks only the length; [`check`](ConnectPacket::check) is what the receiver
/// does with the version and the port afterwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectPacket {
    /// The sender's protocol version, which has the object-serializer flag set.
    pub protocol_version: ProtocolVersion,
    /// The port the sender listens on, or 0 (a client, or a connection that cannot be
    /// reversed).
    pub canonical_remote_port: u16,
    /// `connectionId`: 0 for a plain client, 1 for a server, above 1 for a multi-version
    /// client's connections.
    pub connection_id: u64,
    /// `canonicalRemoteIp4`: the IPv4 address as an integer (`10.0.0.1` is `0x0a000001`).
    pub canonical_remote_ip4: u32,
    /// `flags`: [`ConnectPacket::FLAG_IPV6`] or 0.
    pub flags: u16,
    /// `canonicalRemoteIp6`: the IPv6 address, valid if the IPv6 flag is set.
    pub canonical_remote_ip6: [u8; 16],
}

/// Why [`ConnectPacket::check`] refused a packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectPacketError {
    /// The packet's version is not [valid](ProtocolVersion::is_valid). FDB builds the
    /// packet's `BinaryReader` with `AssumeVersion(protocolVersion)`, which asserts on it.
    #[error("connect packet has an invalid protocol version {0}")]
    InvalidVersion(ProtocolVersion),
    /// The packet's version lacks the object-serializer flag, so the peer speaks the old
    /// non-flatbuffers protocol.
    #[error("connect packet version {0} has no object-serializer flag")]
    MissingObjectSerializerFlag(ProtocolVersion),
    /// The peer's version is not [compatible](ProtocolVersion::is_compatible) with the
    /// local one. (FDB keeps such a connection open when the peer is a recent multi-version
    /// client so that it can read the version; a client talking to a cluster treats it as
    /// the failure it is.)
    #[error("peer protocol version {peer} is not compatible with {local}")]
    Incompatible {
        /// This process's version.
        local: ProtocolVersion,
        /// The peer's version as sent.
        peer: ProtocolVersion,
    },
    /// On an outgoing connection, the peer announced a different port from the one dialed
    /// (`ASSERT(pkt.canonicalRemotePort == peerAddress.port)` in FDB).
    #[error("peer announced port {announced} but {dialed} was dialed")]
    PortMismatch {
        /// The port that was dialed.
        dialed: u16,
        /// The port in the packet.
        announced: u16,
    },
}

impl ConnectPacket {
    /// `connectPacketLength` of the packets this crate reads and writes: the 40 bytes after
    /// the length.
    pub const LENGTH: u32 = 40;
    /// The whole packet, length included.
    pub const SIZE: usize = 44;
    /// `FLAG_IPV6`.
    pub const FLAG_IPV6: u16 = 1;

    /// A plain client's packet, as a process that does not use the multi-version API sends
    /// it: connection id 0, and no listening address (the client's own address is
    /// `0.0.0.0:0`, which `prependConnectPacket` sends when the local and destination TLS
    /// modes match, and which it also sends for a mixed TLS/non-TLS connection). The
    /// object-serializer flag is added to `version`.
    pub fn client(version: ProtocolVersion) -> ConnectPacket {
        ConnectPacket::with_connection_id(version, 0)
    }

    /// A multi-version client's packet: the same as [`client`](ConnectPacket::client) with a
    /// connection id derived from `random`. `MultiVersionApi` draws 8 random bytes and
    /// adds 2 if the id is 0 or 1, so that the id is above 1; this does the same to the
    /// `u64` you give it.
    pub fn multi_version_client(version: ProtocolVersion, random: u64) -> ConnectPacket {
        let id = if random <= 1 { random + 2 } else { random };
        ConnectPacket::with_connection_id(version, id)
    }

    /// [`multi_version_client`](ConnectPacket::multi_version_client) with a random id from
    /// the standard library's per-process random state (not a cryptographic source; the id
    /// only has to be unlikely to repeat).
    pub fn random_multi_version_client(version: ProtocolVersion) -> ConnectPacket {
        let random = RandomState::new().build_hasher().finish();
        ConnectPacket::multi_version_client(version, random)
    }

    /// A listening process's packet, as `fdbserver` sends it: connection id 1 and the
    /// address it listens on.
    pub fn server(version: ProtocolVersion, listen: SocketAddr) -> ConnectPacket {
        let mut packet = ConnectPacket::with_connection_id(version, 1);
        packet.canonical_remote_port = listen.port();
        match listen.ip() {
            IpAddr::V4(ip) => packet.canonical_remote_ip4 = u32::from(ip),
            IpAddr::V6(ip) => {
                packet.flags |= Self::FLAG_IPV6;
                packet.canonical_remote_ip6 = ip.octets();
            }
        }
        packet
    }

    fn with_connection_id(version: ProtocolVersion, connection_id: u64) -> ConnectPacket {
        ConnectPacket {
            protocol_version: version.with_object_serializer_flag(),
            canonical_remote_port: 0,
            connection_id,
            canonical_remote_ip4: 0,
            flags: 0,
            canonical_remote_ip6: [0; 16],
        }
    }

    /// The address the sender listens on, if it says: `None` when the port is 0.
    pub fn canonical_remote_address(&self) -> Option<SocketAddr> {
        if self.canonical_remote_port == 0 {
            return None;
        }
        let ip = if self.flags & Self::FLAG_IPV6 != 0 {
            IpAddr::V6(Ipv6Addr::from(self.canonical_remote_ip6))
        } else {
            IpAddr::V4(Ipv4Addr::from(self.canonical_remote_ip4))
        };
        Some(SocketAddr::new(ip, self.canonical_remote_port))
    }

    /// Whether the connection id marks a multi-version client's connection
    /// (`connectionId > 1`).
    pub fn is_multi_version(&self) -> bool {
        self.connection_id > 1
    }

    /// What a receiver does with a packet it has read (`connectionReader`):
    ///
    /// 1. the version must be [valid](ProtocolVersion::is_valid) (C++ asserts: the reader is
    ///    `AssumeVersion(protocolVersion)`);
    /// 2. it must have the object-serializer flag and be
    ///    [compatible](ProtocolVersion::is_compatible) with `local` (flags ignored);
    /// 3. on an outgoing connection, `dialed_port` is `Some(port)` and the packet must
    ///    announce that port. Pass `None` for an incoming connection, where the port (if
    ///    not 0) is only the address to reconnect to.
    pub fn check(
        &self,
        local: ProtocolVersion,
        dialed_port: Option<u16>,
    ) -> Result<(), ConnectPacketError> {
        let peer = self.protocol_version;
        if !peer.is_valid() {
            return Err(ConnectPacketError::InvalidVersion(peer));
        }
        if !peer.has_object_serializer_flag() {
            return Err(ConnectPacketError::MissingObjectSerializerFlag(peer));
        }
        if !peer.is_compatible(local) {
            return Err(ConnectPacketError::Incompatible { local, peer });
        }
        match dialed_port {
            Some(dialed) if dialed != self.canonical_remote_port => {
                Err(ConnectPacketError::PortMismatch {
                    dialed,
                    announced: self.canonical_remote_port,
                })
            }
            _ => Ok(()),
        }
    }

    /// The packet's 44 bytes (`prependConnectPacket`).
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = Writer::unversioned();
        w.write(&Self::LENGTH)?;
        w.write(&self.protocol_version)?;
        w.write(&self.canonical_remote_port)?;
        w.write(&self.connection_id)?;
        w.write(&self.canonical_remote_ip4)?;
        w.write(&self.flags)?;
        w.write_bytes(&self.canonical_remote_ip6)?;
        Ok(w.into_bytes())
    }

    /// Reads a packet from the start of `bytes` and returns it with the number of bytes it
    /// took, so a caller reading a stream can continue after it. Fails with
    /// [`UnexpectedEof`](DecodeError::UnexpectedEof) if `bytes` ends before the packet does
    /// (read more and retry), and with [`Invalid`](DecodeError::Invalid) for a length that
    /// is not 40 (FDB rejects a longer one with `serialization_failed`; a shorter one cannot
    /// hold the fields). It does not look at the version: see
    /// [`check`](ConnectPacket::check).
    pub fn decode(bytes: &[u8]) -> Result<(ConnectPacket, usize), DecodeError> {
        let mut r = Reader::unversioned(bytes);
        let length = r.read_u32()?;
        if length != Self::LENGTH {
            return Err(DecodeError::Invalid("connect packet length is not 40"));
        }
        let packet = ConnectPacket {
            protocol_version: r.read()?,
            canonical_remote_port: r.read()?,
            connection_id: r.read()?,
            canonical_remote_ip4: r.read()?,
            flags: r.read()?,
            canonical_remote_ip6: r.read_array()?,
        };
        Ok((packet, r.position()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V: ProtocolVersion = ProtocolVersion::V7_4;

    #[test]
    fn a_client_packet_passes_the_check_of_the_same_version() {
        // The flagged version a client sends is compatible with the unflagged local one.
        let packet = ConnectPacket::client(V);
        assert!(packet.protocol_version.has_object_serializer_flag());
        assert_eq!(packet.connection_id, 0);
        assert_eq!(packet.check(V, None), Ok(()));
        assert_eq!(packet.check(V.with_object_serializer_flag(), None), Ok(()));
        assert_eq!(packet.check(V, Some(0)), Ok(()));
    }

    #[test]
    fn check_rejects_what_fdb_rejects() {
        let good = ConnectPacket::client(V);
        let mut p = good;
        p.protocol_version = ProtocolVersion(0);
        assert_eq!(
            p.check(V, None),
            Err(ConnectPacketError::InvalidVersion(ProtocolVersion(0)))
        );
        // An invalid version is reported before the missing flag.
        p.protocol_version = ProtocolVersion(ProtocolVersion::OBJECT_SERIALIZER_FLAG);
        assert!(matches!(
            p.check(V, None),
            Err(ConnectPacketError::InvalidVersion(_))
        ));

        p.protocol_version = V;
        assert_eq!(
            p.check(V, None),
            Err(ConnectPacketError::MissingObjectSerializerFlag(V))
        );

        p.protocol_version = ProtocolVersion::V8_0.with_object_serializer_flag();
        assert_eq!(
            p.check(V, None),
            Err(ConnectPacketError::Incompatible {
                local: V,
                peer: p.protocol_version
            })
        );
        assert_eq!(p.check(ProtocolVersion::V8_0, None), Ok(()));

        let mut p = good;
        p.canonical_remote_port = 4500;
        assert_eq!(p.check(V, None), Ok(()));
        assert_eq!(p.check(V, Some(4500)), Ok(()));
        assert_eq!(
            p.check(V, Some(4501)),
            Err(ConnectPacketError::PortMismatch {
                dialed: 4501,
                announced: 4500
            })
        );
    }

    #[test]
    fn connection_ids() {
        assert!(!ConnectPacket::client(V).is_multi_version());
        let server = ConnectPacket::server(V, "10.0.1.2:4500".parse().unwrap());
        assert_eq!(server.connection_id, 1);
        assert!(!server.is_multi_version());
        for (random, id) in [(0, 2), (1, 3), (2, 2), (u64::MAX, u64::MAX)] {
            let packet = ConnectPacket::multi_version_client(V, random);
            assert_eq!(packet.connection_id, id);
            assert!(packet.is_multi_version());
        }
        for _ in 0..8 {
            assert!(ConnectPacket::random_multi_version_client(V).is_multi_version());
        }
    }

    #[test]
    fn server_addresses() {
        let v4: SocketAddr = "10.0.1.2:4500".parse().unwrap();
        let packet = ConnectPacket::server(V, v4);
        assert_eq!(packet.canonical_remote_ip4, 0x0a00_0102);
        assert_eq!(packet.flags, 0);
        assert_eq!(packet.canonical_remote_address(), Some(v4));
        let v6: SocketAddr = "[fddb::1]:4501".parse().unwrap();
        let packet = ConnectPacket::server(V, v6);
        assert_eq!(packet.flags, ConnectPacket::FLAG_IPV6);
        assert_eq!(packet.canonical_remote_address(), Some(v6));
        assert_eq!(ConnectPacket::client(V).canonical_remote_address(), None);
        // The bytes the packet encodes to read back the same.
        let bytes = packet.encode().unwrap();
        assert_eq!(bytes.len(), ConnectPacket::SIZE);
        assert_eq!(
            ConnectPacket::decode(&bytes).unwrap(),
            (packet, ConnectPacket::SIZE)
        );
    }

    #[test]
    fn decode_reads_the_length_and_no_more() {
        let mut bytes = ConnectPacket::client(V).encode().unwrap();
        for cut in 0..bytes.len() {
            assert!(ConnectPacket::decode(&bytes[..cut]).is_err(), "{cut}");
        }
        for bad in [0u32, 14, 39, 41, 1 << 31, u32::MAX] {
            bytes[..4].copy_from_slice(&bad.to_le_bytes());
            assert!(ConnectPacket::decode(&bytes).is_err(), "{bad}");
        }
        // Version 0 decodes; check() is what refuses it.
        let mut zero = ConnectPacket::client(V);
        zero.protocol_version = ProtocolVersion(0);
        let (decoded, _) = ConnectPacket::decode(&zero.encode().unwrap()).unwrap();
        assert_eq!(decoded.protocol_version, ProtocolVersion(0));
    }
}
