//! `GetHealthMetricsReply.serialized`.

use std::collections::BTreeMap;

use fdb_native_wire::binary::{
    Decode, DecodeError, Encode, EncodeError, ReadVersioning, Reader, Versioning, Writer,
    from_slice, to_vec,
};
use fdb_native_wire::{ProtocolVersion, Uid};

/// `HealthMetrics::StorageStats` (`fdbclient/FDBTypes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StorageStats {
    /// `storageQueue`.
    pub storage_queue: i64,
    /// `storageDurabilityLag`.
    pub storage_durability_lag: i64,
    /// `diskUsage`.
    pub disk_usage: f64,
    /// `cpuUsage`.
    pub cpu_usage: f64,
}

impl Encode for StorageStats {
    fn serialize(&self, w: &mut Writer) -> Result<(), EncodeError> {
        w.write(&self.storage_queue)?;
        w.write(&self.storage_durability_lag)?;
        w.write(&self.disk_usage)?;
        w.write(&self.cpu_usage)
    }
}

impl<'de> Decode<'de> for StorageStats {
    fn deserialize(r: &mut Reader<'de>) -> Result<Self, DecodeError> {
        Ok(StorageStats {
            storage_queue: r.read()?,
            storage_durability_lag: r.read()?,
            disk_usage: r.read()?,
            cpu_usage: r.read()?,
        })
    }
}

/// `HealthMetrics` (`fdbclient/FDBTypes.h`): the cluster's health as the ratekeeper
/// reports it. Inside a flatbuffers `GetHealthMetricsReply` it travels pre-serialized in
/// the binary format, as the `serialized` string, behind an `IncludeVersion` prefix:
/// [`HealthMetrics::from_blob`] reads that string.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HealthMetrics {
    /// `worstStorageQueue`.
    pub worst_storage_queue: i64,
    /// `limitingStorageQueue`.
    pub limiting_storage_queue: i64,
    /// `worstStorageDurabilityLag`.
    pub worst_storage_durability_lag: i64,
    /// `limitingStorageDurabilityLag`.
    pub limiting_storage_durability_lag: i64,
    /// `worstTLogQueue`.
    pub worst_t_log_queue: i64,
    /// `tpsLimit`.
    pub tps_limit: f64,
    /// `batchLimited`.
    pub batch_limited: bool,
    /// `storageStats`, by storage server id.
    pub storage_stats: BTreeMap<Uid, StorageStats>,
    /// `tLogQueue`, by log server id.
    pub t_log_queue: BTreeMap<Uid, i64>,
}

impl HealthMetrics {
    /// Reads `GetHealthMetricsReply::serialized`.
    ///
    /// Stricter than C++ in one way: bytes after the metrics are an error
    /// ([`TrailingBytes`](DecodeError::TrailingBytes)), where `BinaryReader::fromStringRef`
    /// ignores them. The ratekeeper writes none.
    pub fn from_blob(blob: &[u8]) -> Result<HealthMetrics, DecodeError> {
        from_slice(blob, ReadVersioning::INCLUDE_8_0)
    }

    /// The `serialized` string a server of protocol `version` sends.
    pub fn to_blob(&self, version: ProtocolVersion) -> Result<Vec<u8>, EncodeError> {
        to_vec(self, Versioning::Include(version))
    }
}

// The members in `serializer` argument order, which is not declaration order.
impl Encode for HealthMetrics {
    fn serialize(&self, w: &mut Writer) -> Result<(), EncodeError> {
        w.write(&self.worst_storage_queue)?;
        w.write(&self.worst_storage_durability_lag)?;
        w.write(&self.worst_t_log_queue)?;
        w.write(&self.tps_limit)?;
        w.write(&self.batch_limited)?;
        w.write(&self.storage_stats)?;
        w.write(&self.t_log_queue)?;
        w.write(&self.limiting_storage_queue)?;
        w.write(&self.limiting_storage_durability_lag)
    }
}

impl<'de> Decode<'de> for HealthMetrics {
    fn deserialize(r: &mut Reader<'de>) -> Result<Self, DecodeError> {
        let worst_storage_queue = r.read()?;
        let worst_storage_durability_lag = r.read()?;
        let worst_t_log_queue = r.read()?;
        let tps_limit = r.read()?;
        let batch_limited = r.read()?;
        let storage_stats = r.read()?;
        let t_log_queue = r.read()?;
        let limiting_storage_queue = r.read()?;
        let limiting_storage_durability_lag = r.read()?;
        Ok(HealthMetrics {
            worst_storage_queue,
            limiting_storage_queue,
            worst_storage_durability_lag,
            limiting_storage_durability_lag,
            worst_t_log_queue,
            tps_limit,
            batch_limited,
            storage_stats,
            t_log_queue,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_round_trip_and_trailing_bytes() {
        let m = HealthMetrics {
            worst_storage_queue: 1,
            tps_limit: 2.5,
            storage_stats: [(Uid::new(1, 2), StorageStats::default())].into(),
            ..HealthMetrics::default()
        };
        let mut blob = m.to_blob(ProtocolVersion::V7_4).unwrap();
        assert_eq!(HealthMetrics::from_blob(&blob).unwrap(), m);
        blob.push(0);
        assert_eq!(
            HealthMetrics::from_blob(&blob),
            Err(DecodeError::TrailingBytes(1))
        );
    }
}
