//! Runtime-agnostic FoundationDB connection layer, built on `fdb-native-wire`.
//!
//! [`runtime`] is the small async runtime abstraction (with a tokio adapter), [`framing`] the
//! packet framing codec, [`ConnectPacket`] the handshake packet, [`connection`] the sans-IO
//! state machine of one connection (handshake, dispatch, pings and liveness), and [`rpc`] the
//! sans-IO request/reply layer over it (reply tokens, pending requests, at-most-once failures).

mod connect_packet;
pub mod connection;
pub mod framing;
pub mod rpc;
pub mod runtime;

pub use connect_packet::{ConnectPacket, ConnectPacketError, process_transport_id};
pub use connection::{
    Connection, ConnectionConfig, ConnectionError, ConnectionState, Event, MonitorConfig, SendError,
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
