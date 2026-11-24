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

    /// One transaction: set `key` = `value`, commit; returns the committed version.
    fn set_commit_version(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> impl Future<Output = Result<i64, BoxErr>> + Send;

    /// A fresh transaction: its read version, then `get(key)`.
    fn rv_get(
        &self,
        key: &[u8],
    ) -> impl Future<Output = Result<(i64, Option<Vec<u8>>), BoxErr>> + Send;

    /// A fresh transaction with its read version set to `rv`: `get(key)`.
    fn get_at(
        &self,
        key: &[u8],
        rv: i64,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, BoxErr>> + Send;

    /// A fresh transaction: its read version only.
    fn grv(&self) -> impl Future<Output = Result<i64, BoxErr>> + Send;
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

    async fn set_commit_version(&self, key: &[u8], value: &[u8]) -> Result<i64, BoxErr> {
        let tx = self.db.create_transaction();
        tx.set(key, value);
        tx.commit().await.map_err(es)?;
        Ok(tx.committed_version())
    }

    async fn rv_get(&self, key: &[u8]) -> Result<(i64, Option<Vec<u8>>), BoxErr> {
        let tx = self.db.create_transaction();
        let rv = tx.get_read_version().await.map_err(es)?;
        let v = tx.get(key).await.map_err(es)?;
        Ok((rv, v.map(|v| v.to_vec())))
    }

    async fn get_at(&self, key: &[u8], rv: i64) -> Result<Option<Vec<u8>>, BoxErr> {
        let tx = self.db.create_transaction();
        tx.set_read_version(rv);
        Ok(tx.get(key).await.map_err(es)?.map(|v| v.to_vec()))
    }

    async fn grv(&self) -> Result<i64, BoxErr> {
        let tx = self.db.create_transaction();
        tx.get_read_version().await.map_err(es)
    }
}

// ---------------------------------------------------------------------------------------------
// libfdb_c through the `foundationdb` crate

#[derive(Clone)]
pub struct CClient {
    dbs: Arc<Vec<foundationdb::Database>>,
    next: Arc<std::sync::atomic::AtomicUsize>,
}

impl CClient {
    /// `foundationdb::boot()` must have run (its guard is held by the caller). Opens `dbs`
    /// database handles; every operation takes the next one, round robin. With the network
    /// running `ClientThreadsPerVersion(N)` the handles are spread over the N client threads,
    /// so `dbs` should be N to use them all.
    pub fn open(cluster_file: &Path, dbs: usize) -> Result<CClient, BoxErr> {
        let path = cluster_file.to_str().ok_or("path")?;
        let dbs = (0..dbs.max(1))
            .map(|_| foundationdb::Database::new(Some(path)).map_err(es))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CClient {
            dbs: Arc::new(dbs),
            next: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    fn db(&self) -> &foundationdb::Database {
        let i = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        &self.dbs[i % self.dbs.len()]
    }
}

impl Backend for CClient {
    async fn get(&self, key: &[u8]) -> Result<bool, BoxErr> {
        let tx = self.db().create_trx().map_err(es)?;
        let v = tx.get(key, false).await.map_err(es)?;
        Ok(v.is_some())
    }

    async fn get_seq(&self, keys: &[Vec<u8>]) -> Result<Vec<u32>, BoxErr> {
        let tx = self.db().create_trx().map_err(es)?;
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
        self.db()
            .run(|tx, _| async move {
                tx.set(key, value);
                Ok(())
            })
            .await
            .map_err(es)
    }

    async fn run_ops(&self, ops: &[Op]) -> Result<(), BoxErr> {
        self.db()
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
        let tx = self.db().create_trx().map_err(es)?;
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
        let tx = self.db().create_trx().map_err(es)?;
        let w = tx.watch(key);
        tx.commit().await.map_err(es)?;
        Ok(Box::pin(async move { w.await.map_err(es) }))
    }

    async fn set_commit_version(&self, key: &[u8], value: &[u8]) -> Result<i64, BoxErr> {
        let tx = self.db().create_trx().map_err(es)?;
        tx.set(key, value);
        let c = tx.commit().await.map_err(es)?;
        c.committed_version().map_err(es)
    }

    async fn rv_get(&self, key: &[u8]) -> Result<(i64, Option<Vec<u8>>), BoxErr> {
        let tx = self.db().create_trx().map_err(es)?;
        let rv = tx.get_read_version().await.map_err(es)?;
        let v = tx.get(key, false).await.map_err(es)?;
        Ok((rv, v.map(|v| v.to_vec())))
    }

    async fn get_at(&self, key: &[u8], rv: i64) -> Result<Option<Vec<u8>>, BoxErr> {
        let tx = self.db().create_trx().map_err(es)?;
        tx.set_read_version(rv);
        Ok(tx.get(key, false).await.map_err(es)?.map(|v| v.to_vec()))
    }

    async fn grv(&self) -> Result<i64, BoxErr> {
        let tx = self.db().create_trx().map_err(es)?;
        tx.get_read_version().await.map_err(es)
    }
}
