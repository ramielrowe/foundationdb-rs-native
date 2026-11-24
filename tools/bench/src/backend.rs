//! The two clients under test behind one interface: `Native` (fdb-native-client) and `CClient`
//! (libfdb_c 7.4.10, through the `foundationdb` crate). Every method is the same unit of work
//! on both sides; the workloads in `workloads.rs` are generic over this trait.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;

pub type BoxErr = String;

fn es<E: std::fmt::Display>(e: E) -> BoxErr {
    e.to_string()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    WantAll,
    Iterator,
}

impl Mode {
    pub fn name(self) -> &'static str {
        match self {
            Mode::WantAll => "wantall",
            Mode::Iterator => "iterator",
        }
    }
}

#[derive(Clone, Debug)]
pub enum Op {
    Get(Vec<u8>),
    Set(Vec<u8>, Vec<u8>),
}

pub type WatchFuture = Pin<Box<dyn Future<Output = Result<(), BoxErr>> + Send>>;

pub trait Backend: Clone + Send + Sync + 'static {
    /// A fresh transaction, one `get`, dropped without commit (read version + the read).
    fn get(&self, key: &[u8]) -> impl Future<Output = Result<bool, BoxErr>> + Send;

    /// One transaction, `keys.len()` sequential gets. Returns the latency of each get in
    /// microseconds, excluding the first (which pays for the read version).
    fn get_seq(&self, keys: &[Vec<u8>]) -> impl Future<Output = Result<Vec<u32>, BoxErr>> + Send;

    /// `db.run` of one `set`: a transaction that writes one key and commits.
    fn set_commit(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> impl Future<Output = Result<(), BoxErr>> + Send;

    /// `db.run` of the ops in order (each read awaited before the next op), then commit.
    fn run_ops(&self, ops: &[Op]) -> impl Future<Output = Result<(), BoxErr>> + Send;

    /// Scans `[begin, end)` in one transaction, row by row. Returns `(rows, bytes)`.
    fn scan(
        &self,
        begin: &[u8],
        end: &[u8],
        mode: Mode,
    ) -> impl Future<Output = Result<(u64, u64), BoxErr>> + Send;

    /// A transaction that watches `key` and commits (the watch starts at commit). Returns
    /// the future that resolves when the key changes.
    fn watch(&self, key: &[u8]) -> impl Future<Output = Result<WatchFuture, BoxErr>> + Send;
}

// ---------------------------------------------------------------------------------------------
// fdb-native-client

#[derive(Clone)]
pub struct Native {
    db: fdb_native_client::Database,
}

impl Native {
    /// Must be called inside a tokio runtime.
    pub fn open(cluster_file: &Path) -> Result<Native, BoxErr> {
        let db = fdb_native_client::Database::open(Some(cluster_file)).map_err(es)?;
        Ok(Native { db })
    }

    pub async fn shutdown(&self) {
        self.db.shutdown().await;
    }
}

impl Backend for Native {
    async fn get(&self, key: &[u8]) -> Result<bool, BoxErr> {
        let tx = self.db.create_transaction();
        let v = tx.get(key).await.map_err(es)?;
        Ok(v.is_some())
    }

    async fn get_seq(&self, keys: &[Vec<u8>]) -> Result<Vec<u32>, BoxErr> {
        let tx = self.db.create_transaction();
        let mut lat = Vec::with_capacity(keys.len());
        for (i, k) in keys.iter().enumerate() {
            let t = Instant::now();
            tx.get(k).await.map_err(es)?;
            if i > 0 {
                lat.push(t.elapsed().as_micros() as u32);
            }
        }
        Ok(lat)
    }

    async fn set_commit(&self, key: &[u8], value: &[u8]) -> Result<(), BoxErr> {
        self.db
            .run(|tx, _| async move {
                tx.set(key, value);
                Ok(())
            })
            .await
            .map_err(es)
    }

    async fn run_ops(&self, ops: &[Op]) -> Result<(), BoxErr> {
        self.db
            .run(|tx, _| async move {
                for op in ops {
                    match op {
                        Op::Get(k) => {
                            tx.get(k).await?;
                        }
                        Op::Set(k, v) => tx.set(k, v),
                    }
                }
                Ok(())
            })
            .await
            .map_err(es)
    }

    async fn scan(&self, begin: &[u8], end: &[u8], mode: Mode) -> Result<(u64, u64), BoxErr> {
        use fdb_native_client::{KeySelector, RangeOptions, StreamingMode};
        let mode = match mode {
            Mode::WantAll => StreamingMode::WantAll,
            Mode::Iterator => StreamingMode::Iterator,
        };
        let tx = self.db.create_transaction();
        let mut rows = tx.get_ranges_keyvalues(
            KeySelector::first_greater_or_equal(begin.to_vec()),
            KeySelector::first_greater_or_equal(end.to_vec()),
            RangeOptions::default().mode(mode),
        );
        let (mut n, mut bytes) = (0u64, 0u64);
        while let Some(row) = rows.next().await {
            let row = row.map_err(es)?;
            n += 1;
            bytes += (row.key.len() + row.value.len()) as u64;
        }
        Ok((n, bytes))
    }

    async fn watch(&self, key: &[u8]) -> Result<WatchFuture, BoxErr> {
        let tx = self.db.create_transaction();
        let w = tx.watch(key).await.map_err(es)?;
        tx.commit().await.map_err(es)?;
        Ok(Box::pin(async move { w.await.map_err(es) }))
    }
}

// ---------------------------------------------------------------------------------------------
// libfdb_c through the `foundationdb` crate

#[derive(Clone)]
pub struct CClient {
    db: Arc<foundationdb::Database>,
}

impl CClient {
    /// `foundationdb::boot()` must have run (its guard is held by the caller).
    pub fn open(cluster_file: &Path) -> Result<CClient, BoxErr> {
        let db =
            foundationdb::Database::new(Some(cluster_file.to_str().ok_or("path")?)).map_err(es)?;
        Ok(CClient { db: Arc::new(db) })
    }
}

impl Backend for CClient {
    async fn get(&self, key: &[u8]) -> Result<bool, BoxErr> {
        let tx = self.db.create_trx().map_err(es)?;
        let v = tx.get(key, false).await.map_err(es)?;
        Ok(v.is_some())
    }

    async fn get_seq(&self, keys: &[Vec<u8>]) -> Result<Vec<u32>, BoxErr> {
        let tx = self.db.create_trx().map_err(es)?;
        let mut lat = Vec::with_capacity(keys.len());
        for (i, k) in keys.iter().enumerate() {
            let t = Instant::now();
            tx.get(k, false).await.map_err(es)?;
            if i > 0 {
                lat.push(t.elapsed().as_micros() as u32);
            }
        }
        Ok(lat)
    }

    async fn set_commit(&self, key: &[u8], value: &[u8]) -> Result<(), BoxErr> {
        self.db
            .run(|tx, _| async move {
                tx.set(key, value);
                Ok(())
            })
            .await
            .map_err(es)
    }

    async fn run_ops(&self, ops: &[Op]) -> Result<(), BoxErr> {
        self.db
            .run(|tx, _| async move {
                for op in ops {
                    match op {
                        Op::Get(k) => {
                            tx.get(k, false).await?;
                        }
                        Op::Set(k, v) => tx.set(k, v),
                    }
                }
                Ok(())
            })
            .await
            .map_err(es)
    }

    async fn scan(&self, begin: &[u8], end: &[u8], mode: Mode) -> Result<(u64, u64), BoxErr> {
        use foundationdb::RangeOption;
        use foundationdb::options::StreamingMode;
        let tx = self.db.create_trx().map_err(es)?;
        let mut opt = RangeOption::from((begin, end));
        opt.mode = match mode {
            Mode::WantAll => StreamingMode::WantAll,
            Mode::Iterator => StreamingMode::Iterator,
        };
        let mut rows = tx.get_ranges_keyvalues(opt, false);
        let (mut n, mut bytes) = (0u64, 0u64);
        while let Some(row) = rows.next().await {
            let row = row.map_err(es)?;
            n += 1;
            bytes += (row.key().len() + row.value().len()) as u64;
        }
        Ok((n, bytes))
    }

    async fn watch(&self, key: &[u8]) -> Result<WatchFuture, BoxErr> {
        let tx = self.db.create_trx().map_err(es)?;
        let w = tx.watch(key);
        tx.commit().await.map_err(es)?;
        Ok(Box::pin(async move { w.await.map_err(es) }))
    }
}
