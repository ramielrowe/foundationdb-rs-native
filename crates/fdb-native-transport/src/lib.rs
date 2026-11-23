//! Runtime-agnostic FoundationDB connection layer, built on `fdb-native-wire`.
//!
//! A `Runtime` trait and a tokio adapter will live here.

mod connect_packet;

pub use connect_packet::{ConnectPacket, ConnectPacketError};

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold() {}
}
