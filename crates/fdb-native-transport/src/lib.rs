//! Runtime-agnostic FoundationDB connection layer, built on `fdb-native-wire`.
//!
//! A `Runtime` trait and a tokio adapter will live here.

mod connect_packet;
pub mod framing;
pub mod runtime;

pub use connect_packet::{ConnectPacket, ConnectPacketError};
pub use framing::{
    CHECKSUM_LEN, Checksum, DEFAULT_PACKET_LIMIT, DEFAULT_PACKET_WARNING, Frame, FrameConfig,
    FrameDecoder, FrameError, FrameWriteError, LENGTH_LEN, OwnedFrame, TOKEN_LEN, parse_frame,
};

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold() {}
}
