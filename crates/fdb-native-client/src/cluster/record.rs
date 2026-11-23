//! Where the connection string lives: in memory only, or in a cluster file that the monitor
//! keeps up to date (C++'s `IClusterConnectionRecord`).

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cluster_file::{
    ClusterConnectionString, ClusterFile, ClusterFileError, read_cluster_file,
};

/// What `ClusterConnectionFile::persist` writes before the connection string
/// (`fdbclient/ClusterConnectionFile.actor.cpp` 7.4:153-155).
pub const CLUSTER_FILE_HEADER: &str =
    "# DO NOT EDIT!\n# This file is auto-generated, it is not to be edited by hand\n";

/// The connection string a [`ClusterMonitor`](super::ClusterMonitor) starts from, and where
/// it is stored.
///
/// * [`memory`](ConnectionRecord::memory) is `ClusterConnectionMemoryRecord`: a forward
///   changes only the monitor's copy.
/// * [`open`](ConnectionRecord::open) (or `From<ClusterFile>`) is a loaded
///   `ClusterConnectionFile`: before each request the file is read and compared with the
///   string in memory (a difference is reported to the cluster as
///   `incorrect_cluster_file_contents`, and adopted when every coordinator has failed), and
///   after the cluster moved (a forward, then a successful reply from the new coordinators)
///   the file is atomically replaced with the new string.
/// * [`file`](ConnectionRecord::file) is `ClusterConnectionFile(filename, contents)`: a file
///   that is to hold `connection_string`; it is written once a coordinator has answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionRecord {
    pub(crate) connection_string: ClusterConnectionString,
    pub(crate) path: Option<PathBuf>,
    /// `connectionStringNeedsPersisted`.
    pub(crate) needs_persist: bool,
}

impl ConnectionRecord {
    /// A connection string kept in memory only.
    pub fn memory(connection_string: ClusterConnectionString) -> ConnectionRecord {
        ConnectionRecord {
            connection_string,
            path: None,
            needs_persist: false,
        }
    }

    /// Reads the cluster file at `path`, which the monitor then keeps up to date.
    pub fn open(path: impl Into<PathBuf>) -> Result<ConnectionRecord, ClusterFileError> {
        let path = path.into();
        let connection_string = read_cluster_file(&path)?;
        Ok(ConnectionRecord {
            connection_string,
            path: Some(path),
            needs_persist: false,
        })
    }

    /// A cluster file at `path` that is to hold `connection_string`: it is (re)written once
    /// a coordinator of that string has answered, and not checked before.
    pub fn file(
        path: impl Into<PathBuf>,
        connection_string: ClusterConnectionString,
    ) -> ConnectionRecord {
        ConnectionRecord {
            connection_string,
            path: Some(path.into()),
            needs_persist: true,
        }
    }

    /// The connection string.
    pub fn connection_string(&self) -> &ClusterConnectionString {
        &self.connection_string
    }

    /// The cluster file, if the record has one.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// `upToDate`: whether the file still holds the string in memory. `Ok(None)` if it does
    /// (or there is no file, or it has not been written yet); otherwise what the file holds
    /// now, `None` inside if it cannot be read or parsed (C++ swallows that error and
    /// reports the file as out of date).
    pub(crate) fn check_file(&self) -> Option<Option<ClusterConnectionString>> {
        let path = self.path.as_ref()?;
        if self.needs_persist {
            return None;
        }
        match read_cluster_file(path) {
            // C++ compares `toString()`s.
            Ok(stored) if stored.to_string() == self.connection_string.to_string() => None,
            Ok(stored) => Some(Some(stored)),
            Err(_) => Some(None),
        }
    }

    /// `setAndPersistConnectionString`: takes `connection_string` and writes it to the file,
    /// if there is one.
    pub(crate) fn set_and_persist(
        &mut self,
        connection_string: ClusterConnectionString,
    ) -> io::Result<()> {
        self.connection_string = connection_string;
        self.persist()
    }

    /// `notifyConnected`: writes a string that has not been written yet.
    pub(crate) fn notify_connected(&mut self) -> io::Result<()> {
        if self.needs_persist {
            self.persist()
        } else {
            Ok(())
        }
    }

    /// `persist`: atomically replaces the file with the header and the string. Clears the
    /// pending-write flag even if the write fails, as C++ does.
    fn persist(&mut self) -> io::Result<()> {
        self.needs_persist = false;
        let Some(path) = &self.path else {
            return Ok(());
        };
        atomic_replace(
            path,
            format!("{CLUSTER_FILE_HEADER}{}\n", self.connection_string).as_bytes(),
        )
    }
}

impl From<ClusterFile> for ConnectionRecord {
    /// A loaded cluster file, as [`ConnectionRecord::open`] makes.
    fn from(file: ClusterFile) -> ConnectionRecord {
        ConnectionRecord {
            connection_string: file.connection_string,
            path: Some(file.location.path),
            needs_persist: false,
        }
    }
}

/// `atomicReplace`: writes a sibling temporary file, syncs it, and renames it over `path`.
fn atomic_replace(path: &Path, contents: &[u8]) -> io::Result<()> {
    let mut name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no file name"))?
        .to_os_string();
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    name.push(format!(".{}-{n}.part", std::process::id()));
    let temp = path.with_file_name(name);
    let result = (|| {
        let mut file = fs::File::create(&temp)?;
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&temp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}
