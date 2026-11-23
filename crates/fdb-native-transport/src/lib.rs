//! Runtime-agnostic FoundationDB connection layer, built on `fdb-native-wire`.
//!
//! [`runtime`] is the small async runtime abstraction (with a tokio adapter), [`framing`] the
//! packet framing codec, [`ConnectPacket`] the handshake packet, and [`connection`] the sans-IO
//! state machine of one connection (handshake, dispatch, pings and liveness).

mod connect_packet;
pub mod connection;
pub mod framing;
pub mod runtime;

pub use connect_packet::{ConnectPacket, ConnectPacketError};
pub use connection::{
    Connection, ConnectionConfig, ConnectionError, ConnectionState, Event, MonitorConfig, SendError,
};
pub use framing::{
    CHECKSUM_LEN, Checksum, DEFAULT_PACKET_LIMIT, DEFAULT_PACKET_WARNING, Frame, FrameConfig,
    FrameDecoder, FrameError, FrameWriteError, LENGTH_LEN, OwnedFrame, TOKEN_LEN, parse_frame,
};

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold() {}
}
