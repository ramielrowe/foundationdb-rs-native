//! Runtime-agnostic FoundationDB connection layer, built on `fdb-native-wire`.
//!
//! [`runtime`] is the small async runtime abstraction (with tokio and smol adapters), [`framing`] the
//! packet framing codec, [`ConnectPacket`] the handshake packet, [`connection`] the sans-IO
//! state machine of one connection (handshake, dispatch, pings and liveness), [`rpc`] the
//! sans-IO request/reply layer over it (reply tokens, pending requests, at-most-once failures),
//! and [`peer`] the async peer manager that drives connections over a runtime (one per address,
//! reconnect backoff, failure state, requests).
//!
//! # Features
//!
//! * `tokio` (default): `runtime::TokioRuntime`, the adapter for a tokio runtime.
//! * `smol`: `runtime::SmolRuntime`, the adapter for the smol family (async-io sockets and
//!   timers, async-executor tasks, `blocking` for DNS); no tokio involved. The adapters are
//!   independent: enable either or both, or neither and bring your own [`runtime::Runtime`].
//!   The transport is generic over the runtime, so choosing one is choosing the type argument
//!   of [`peer::PeerManager`]. Tests that run the transport on smol (`tests/smol_*.rs`) need
//!   the feature: `cargo nextest run -p fdb-native-transport --features smol`.
//! * `tls`: the rustls client layer (the `tls` module), over any runtime's stream.
//! * `tls-aws-lc-rs`: `aws-lc-rs` instead of `ring` as the crypto provider.

mod connect_packet;
pub mod connection;
pub mod framing;
pub mod peer;
pub mod rpc;
pub mod runtime;
#[cfg(feature = "tls")]
pub mod tls;

pub use connect_packet::{ConnectPacket, ConnectPacketError, process_transport_id};
pub use connection::{
    Connection, ConnectionConfig, ConnectionError, ConnectionState, Event, IoError, MonitorConfig,
    SendError,
};
pub use framing::{
    CHECKSUM_LEN, Checksum, ConsumeError, DEFAULT_PACKET_LIMIT, DEFAULT_PACKET_WARNING, Frame,
    FrameConfig, FrameDecoder, FrameError, FrameWriteError, LENGTH_LEN, MAX_SPARE, OwnedFrame,
    TOKEN_LEN, parse_frame,
};
pub use rpc::{
    Completion, EndpointFailure, RegistryConfig, ReplyRegistry, RequestError, RequestState,
    RpcEvent,
};

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold() {}
}
