//! Public API of the pure-Rust native FoundationDB client.
//!
//! Built on `fdb-native-schema` and `fdb-native-transport`.

pub mod cluster_file;

pub use cluster_file::{
    ClusterConnectionString, ClusterFile, ClusterFileError, ClusterFileLocation, ClusterFileSource,
    ConnectionStringError, Coordinator, ResolvedCoordinator,
};

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold() {}
}
