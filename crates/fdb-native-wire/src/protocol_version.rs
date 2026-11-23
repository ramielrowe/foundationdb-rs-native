//! The protocol version a message is read or written for.

use core::fmt;

/// An FDB protocol version (`ProtocolVersion` in `flow/ProtocolVersion.h`), as exchanged
/// in the FlowTransport connect packet.
///
/// The codec does not interpret it. [`Reader`](crate::Reader) and
/// [`Writer`](crate::Writer) carry it so that [`Decode`](crate::Decode) and
/// [`Encode`](crate::Encode) impls can pick the slot layout of the peer's version (FDB 8.0,
/// for example, removed fields from the middle of some messages).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProtocolVersion(pub u64);

impl ProtocolVersion {
    /// The version's numeric value.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#018x}", self.0)
    }
}
