//! In-memory [`Database`] implementation for reth dev nodes.
//!
//! Provides a fully functional, in-memory replacement for the MDBX-backed
//! `DatabaseEnv` — no libmdbx dependency, no filesystem I/O.
//!
//! # Architecture
//!
//! All table data lives in a shared `Arc<RwLock<Inner>>`. Read transactions take a
//! point-in-time snapshot (clone) of the table data; write transactions accumulate
//! changes in a local overlay and atomically commit on success.
//!
//! ## Cursor model
//!
//! Cursors materialise a sorted `Vec<(Vec<u8>, Vec<u8>)>` from the underlying
//! `BTreeMap` at construction time and navigate by index. Write cursors additionally
//! hold a `RefCell` reference back to the mutable table so that mutations are
//! reflected immediately.

#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    ops::{Bound, RangeBounds},
    path::PathBuf,
    sync::Arc,
};

use parking_lot::{Mutex, RwLock};
use reth_db_api::{
    common::{PairResult, ValueOnlyResult},
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    database::Database,
    database_metrics::DatabaseMetrics,
    table::{Compress, Decode, Decompress, DupSort, Encode, IntoVec, Table, TableImporter},
    transaction::{DbTx, DbTxMut},
    DatabaseError, DatabaseWriteOperation,
};
use reth_storage_errors::db::{DatabaseErrorInfo, DatabaseWriteError};

// ─────────────────────────────────────────────────────────────────────────────
// Low-level table storage
// ─────────────────────────────────────────────────────────────────────────────

/// Raw byte-level storage for a single database table.
///
/// `Plain` tables use `key → value`. `DupSort` tables use `key → {value₁, value₂, …}`
/// where values within each key are kept in sorted order via a `BTreeSet`.
#[derive(Clone, Debug)]
enum TableStore {
    /// A regular table: `key_bytes → value_bytes`.
    Plain(BTreeMap<Vec<u8>, Vec<u8>>),
    /// A `DUPSORT` table: `key_bytes → BTreeSet<value_bytes>`.
    DupSort(BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>>),
}

impl TableStore {
    /// Whether this table is a `DupSort` table.
    const fn is_dupsort(&self) -> bool {
        matches!(self, Self::DupSort(_))
    }

    // ── plain table helpers ───────────────────────────────────────────────

    fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        match self {
            Self::Plain(data) => data.get(key),
            Self::DupSort(data) => data.get(key).and_then(|set| set.iter().next()),
        }
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        match self {
            Self::Plain(data) => {
                data.insert(key, value);
            }
            Self::DupSort(_) => {}
        }
    }

    fn remove(&mut self, key: &[u8]) -> bool {
        match self {
            Self::Plain(data) => data.remove(key).is_some(),
            Self::DupSort(_) => false,
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Plain(data) => data.clear(),
            Self::DupSort(data) => data.clear(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Plain(data) => data.len(),
            Self::DupSort(data) => data.values().map(|s| s.len()).sum(),
        }
    }

    /// Returns all entries as sorted `(key, value)` pairs.
    /// For `DupSort` tables this flattens the inner sets.
    fn entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Plain(data) => data.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            Self::DupSort(data) => {
                let mut out = Vec::new();
                for (k, set) in data {
                    for v in set {
                        out.push((k.clone(), v.clone()));
                    }
                }
                out
            }
        }
    }

    // ── DupSort helpers ───────────────────────────────────────────────────

    /// Get all values for a given key (`DupSort` only).
    fn dup_get_values(&self, key: &[u8]) -> Vec<Vec<u8>> {
        match self {
            Self::DupSort(data) => {
                data.get(key).map(|s| s.iter().cloned().collect()).unwrap_or_default()
            }
            Self::Plain(_) => Vec::new(),
        }
    }

    /// Insert a value into the `DupSort` value-set for `key`.
    fn dup_put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if let Self::DupSort(data) = self {
            data.entry(key).or_default().insert(value);
        }
    }

    /// Remove all values for a given key (`DupSort` only).
    fn dup_remove_key(&mut self, key: &[u8]) {
        if let Self::DupSort(data) = self {
            data.remove(key);
        }
    }

    /// Remove a specific value for a given key (`DupSort` only).
    fn dup_remove_value(&mut self, key: &[u8], value: &[u8]) {
        if let Self::DupSort(data) = self &&
            let Some(set) = data.get_mut(key)
        {
            set.remove(value);
            if set.is_empty() {
                data.remove(key);
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Inner shared state
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
struct Inner {
    /// `table_name → TableStore`
    tables: HashMap<&'static str, TableStore>,
}

impl Inner {
    fn table(&self, name: &'static str) -> &TableStore {
        self.tables.get(name).expect("table not registered")
    }

    fn table_mut(&mut self, name: &'static str) -> &mut TableStore {
        self.tables.get_mut(name).expect("table not registered")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryDatabase
// ─────────────────────────────────────────────────────────────────────────────

/// A fully in-memory [`Database`] implementation.
///
/// All state lives in a shared `Arc<RwLock<Inner>>`. Suitable for dev nodes and
/// testing where no persistence is required.
#[derive(Clone, Debug)]
pub struct MemoryDatabase {
    inner: Arc<RwLock<Inner>>,
    /// Ensures only one write transaction is active at a time, matching MDBX semantics.
    write_lock: Arc<Mutex<()>>,
}

impl Default for MemoryDatabase {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryDatabase {
    /// Create a new, empty [`MemoryDatabase`] with all reth tables pre-registered.
    pub fn new() -> Self {
        let mut inner = Inner::default();
        register_tables(&mut inner);
        Self { inner: Arc::new(RwLock::new(inner)), write_lock: Arc::new(Mutex::new(())) }
    }

    /// Snapshot the entire database state for later restoration (e.g. `anvil_snapshot`).
    pub fn snapshot(&self) -> MemoryDatabaseSnapshot {
        let inner = self.inner.read();
        MemoryDatabaseSnapshot { tables: inner.tables.clone() }
    }

    /// Restore a previously taken snapshot (e.g. `anvil_revert`).
    ///
    /// Note: the transaction ID counter is not reverted — it keeps incrementing.
    pub fn restore(&self, snapshot: MemoryDatabaseSnapshot) {
        let mut inner = self.inner.write();
        inner.tables = snapshot.tables;
    }
}

/// An opaque snapshot of a [`MemoryDatabase`] at a point in time.
#[derive(Clone, Debug)]
pub struct MemoryDatabaseSnapshot {
    tables: HashMap<&'static str, TableStore>,
}

impl Database for MemoryDatabase {
    type TX = MemoryTx;
    type TXMut = MemoryTxMut;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        let snapshot = self.inner.read().clone();
        Ok(MemoryTx { inner: snapshot })
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        // Block until no other write transaction is active, matching MDBX semantics.
        // We use `lock()` then `mem::forget` the guard so the mutex stays locked
        // until `MemoryTxMut` is dropped, which calls `force_unlock`.
        let guard = self.write_lock.lock();
        std::mem::forget(guard);
        let snapshot = self.inner.read().clone();
        Ok(MemoryTxMut {
            overlay: Arc::new(parking_lot::Mutex::new(snapshot)),
            db: Arc::clone(&self.inner),
            write_lock: Arc::clone(&self.write_lock),
        })
    }

    fn path(&self) -> PathBuf {
        PathBuf::from(":memory:")
    }

    fn oldest_reader_txnid(&self) -> Option<u64> {
        None
    }

    fn last_txnid(&self) -> Option<u64> {
        None
    }
}

impl DatabaseMetrics for MemoryDatabase {}

// ─────────────────────────────────────────────────────────────────────────────
// Table registration
// ─────────────────────────────────────────────────────────────────────────────

/// Register all reth tables into the Inner map.
///
/// Tables are registered in the same order as the canonical `tables!` macro in
/// `reth-db-api` so that DBI indices match the MDBX implementation.
fn register_tables(inner: &mut Inner) {
    use reth_db_api::tables::Tables;

    for table in Tables::ALL {
        let store = if table.is_dupsort() {
            TableStore::DupSort(BTreeMap::new())
        } else {
            TableStore::Plain(BTreeMap::new())
        };
        inner.tables.insert(table.name(), store);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryTx — read-only transaction
// ─────────────────────────────────────────────────────────────────────────────

/// A read-only snapshot transaction over a [`MemoryDatabase`].
#[derive(Debug)]
pub struct MemoryTx {
    inner: Inner,
}

impl DbTx for MemoryTx {
    type Cursor<T: Table> = MemoryCursor<T>;
    type DupCursor<T: DupSort> = MemoryDupCursor<T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        let encoded = key.encode();
        self.get_by_encoded_key::<T>(&encoded)
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let store = self.inner.table(T::NAME);
        match store.get(key.as_ref()) {
            None => Ok(None),
            Some(bytes) => Ok(Some(decode_value::<T>(bytes)?)),
        }
    }

    fn commit(self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn abort(self) {}

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        let store = self.inner.table(T::NAME);
        Ok(MemoryCursor::new(store.entries()))
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        let store = self.inner.table(T::NAME);
        Ok(MemoryDupCursor::new(store.entries()))
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        Ok(self.inner.table(T::NAME).len())
    }

    fn disable_long_read_transaction_safety(&mut self) {}
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryTxMut — read-write transaction
// ─────────────────────────────────────────────────────────────────────────────

/// A read-write transaction over a [`MemoryDatabase`].
///
/// Changes are buffered in `overlay` and atomically committed to `db` on
/// [`Self::commit`]. `DbTxMut` methods take `&self` (interior mutability via
/// `Mutex`).
pub struct MemoryTxMut {
    /// Local working copy — mutations go here. Wrapped in Arc so cursors can
    /// share a reference to it without lifetime issues.
    overlay: Arc<OverlayMutex>,
    /// Handle to the shared database state (for commit).
    db: Arc<RwLock<Inner>>,
    /// Held locked for the lifetime of the transaction to enforce single-writer semantics.
    /// Unlocked on drop.
    write_lock: Arc<Mutex<()>>,
}

impl fmt::Debug for MemoryTxMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryTxMut").finish_non_exhaustive()
    }
}

impl Drop for MemoryTxMut {
    fn drop(&mut self) {
        // SAFETY: the mutex was locked via `lock()` + `mem::forget(guard)` in `tx_mut()`.
        unsafe { self.write_lock.force_unlock() };
    }
}

impl DbTx for MemoryTxMut {
    type Cursor<T: Table> = MemoryCursor<T>;
    type DupCursor<T: DupSort> = MemoryDupCursor<T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        let encoded = key.encode();
        self.get_by_encoded_key::<T>(&encoded)
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let overlay = self.overlay.lock();
        let store = overlay.table(T::NAME);
        match store.get(key.as_ref()) {
            None => Ok(None),
            Some(bytes) => Ok(Some(decode_value::<T>(bytes)?)),
        }
    }

    fn commit(self) -> Result<(), DatabaseError> {
        let overlay = self.overlay.lock().clone();
        let mut db = self.db.write();
        *db = overlay;
        Ok(())
    }

    fn abort(self) {
        // Drop impl releases the write lock.
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        let overlay = self.overlay.lock();
        let entries = overlay.table(T::NAME).entries();
        Ok(MemoryCursor::new(entries))
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        let overlay = self.overlay.lock();
        let entries = overlay.table(T::NAME).entries();
        Ok(MemoryDupCursor::new(entries))
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        Ok(self.overlay.lock().table(T::NAME).len())
    }

    fn disable_long_read_transaction_safety(&mut self) {}
}

impl DbTxMut for MemoryTxMut {
    type CursorMut<T: Table> = MemoryWriteCursor<T>;
    type DupCursorMut<T: DupSort> = MemoryDupWriteCursor<T>;

    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let key_bytes = key.encode().into_vec();
        let val_bytes = encode_value::<T>(&value);
        let mut overlay = self.overlay.lock();
        let store = overlay.table_mut(T::NAME);
        if store.is_dupsort() {
            store.dup_put(key_bytes, val_bytes);
        } else {
            store.put(key_bytes, val_bytes);
        }
        Ok(())
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let key_bytes = key.encode().into_vec();
        let mut overlay = self.overlay.lock();
        let store = overlay.table_mut(T::NAME);
        if store.is_dupsort() {
            if let Some(v) = value {
                let val_bytes = encode_value::<T>(&v);
                let had = store.dup_get_values(&key_bytes).contains(&val_bytes);
                store.dup_remove_value(&key_bytes, &val_bytes);
                Ok(had)
            } else {
                let had = !store.dup_get_values(&key_bytes).is_empty();
                store.dup_remove_key(&key_bytes);
                Ok(had)
            }
        } else {
            Ok(store.remove(&key_bytes))
        }
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        self.overlay.lock().table_mut(T::NAME).clear();
        Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        // We pass a snapshot of entries for navigation plus the overlay reference.
        // The cursor snapshot is refreshed after each write for correctness.
        let entries = self.overlay.lock().table(T::NAME).entries();
        Ok(MemoryWriteCursor::new(entries, Arc::clone(&self.overlay) as _))
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        let entries = self.overlay.lock().table(T::NAME).entries();
        Ok(MemoryDupWriteCursor::new(entries, Arc::clone(&self.overlay) as _))
    }
}

impl TableImporter for MemoryTxMut {}

// ─────────────────────────────────────────────────────────────────────────────
// Value encoding / decoding helpers
// ─────────────────────────────────────────────────────────────────────────────

fn encode_value<T: Table>(value: &T::Value) -> Vec<u8> {
    let mut buf = Vec::new();
    value.compress_to_buf(&mut buf);
    buf
}

fn decode_value<T: Table>(bytes: &[u8]) -> Result<T::Value, DatabaseError> {
    T::Value::decompress(bytes).map_err(|_| DatabaseError::Decode)
}

fn decode_key<T: Table>(bytes: &[u8]) -> Result<T::Key, DatabaseError> {
    T::Key::decode(bytes)
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryCursor — read-only plain cursor
// ─────────────────────────────────────────────────────────────────────────────

/// A read-only cursor over a plain (non-DupSort) table.
///
/// Cursor state is a sorted `Vec` snapshot; navigation is by index.
pub struct MemoryCursor<T: Table> {
    /// Sorted `(key_bytes, value_bytes)` snapshot.
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Current position. `None` means "before first" or "past last" (exhausted).
    pos: Option<usize>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Table> fmt::Debug for MemoryCursor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryCursor")
            .field("pos", &self.pos)
            .field("len", &self.entries.len())
            .finish()
    }
}

impl<T: Table> MemoryCursor<T> {
    const fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { entries, pos: None, _marker: std::marker::PhantomData }
    }

    fn current_kv(&self) -> PairResult<T> {
        match self.pos {
            None => Ok(None),
            Some(i) => {
                let (k, v) = &self.entries[i];
                Ok(Some((decode_key::<T>(k)?, decode_value::<T>(v)?)))
            }
        }
    }

    /// Binary-search for first index with key >= `target_bytes`.
    fn seek_bytes(&mut self, target: &[u8]) -> PairResult<T> {
        match self.entries.binary_search_by(|(k, _)| k.as_slice().cmp(target)) {
            Ok(i) | Err(i) => {
                if i < self.entries.len() {
                    self.pos = Some(i);
                } else {
                    self.pos = None;
                }
            }
        }
        self.current_kv()
    }
}

impl<T: Table> DbCursorRO<T> for MemoryCursor<T> {
    fn first(&mut self) -> PairResult<T> {
        self.pos = if self.entries.is_empty() { None } else { Some(0) };
        self.current_kv()
    }

    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> {
        let target = key.encode();
        let target = target.as_ref();
        match self.entries.binary_search_by(|(k, _)| k.as_slice().cmp(target)) {
            Ok(mut i) => {
                // Binary search may land anywhere in a run of equal keys —
                // walk back to the first occurrence.
                while i > 0 && self.entries[i - 1].0.as_slice() == target {
                    i -= 1;
                }
                self.pos = Some(i);
                self.current_kv()
            }
            Err(_) => {
                self.pos = None;
                Ok(None)
            }
        }
    }

    fn seek(&mut self, key: T::Key) -> PairResult<T> {
        let encoded = key.encode();
        self.seek_bytes(encoded.as_ref())
    }

    fn next(&mut self) -> PairResult<T> {
        self.pos = match self.pos {
            None => None,
            Some(i) => {
                let next = i + 1;
                (next < self.entries.len()).then_some(next)
            }
        };
        self.current_kv()
    }

    fn prev(&mut self) -> PairResult<T> {
        self.pos = match self.pos {
            None | Some(0) => None,
            Some(i) => Some(i - 1),
        };
        self.current_kv()
    }

    fn last(&mut self) -> PairResult<T> {
        self.pos = if self.entries.is_empty() { None } else { Some(self.entries.len() - 1) };
        self.current_kv()
    }

    fn current(&mut self) -> PairResult<T> {
        self.current_kv()
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            None => self.first(),
            Some(k) => self.seek(k),
        }
        .transpose();
        Ok(Walker::new(self, start))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(k) => self.seek(k.clone()),
            Bound::Excluded(k) => {
                self.seek(k.clone())?;
                // advance past the excluded key
                if self.current_kv()?.as_ref().map(|(ck, _)| ck == k).unwrap_or(false) {
                    self.next()
                } else {
                    self.current_kv()
                }
            }
            Bound::Unbounded => self.first(),
        }
        .transpose();

        let end_key = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Ok(RangeWalker::new(self, start, end_key))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            None => self.last(),
            Some(k) => {
                // position at last entry <= key
                let encoded = k.encode();
                let target = encoded.as_ref();
                match self.entries.binary_search_by(|(ek, _)| ek.as_slice().cmp(target)) {
                    Ok(i) => {
                        self.pos = Some(i);
                        self.current_kv()
                    }
                    Err(0) => {
                        self.pos = None;
                        Ok(None)
                    }
                    Err(i) => {
                        self.pos = Some(i - 1);
                        self.current_kv()
                    }
                }
            }
        }
        .transpose();
        Ok(ReverseWalker::new(self, start))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryWriteCursor — read-write plain cursor
// ─────────────────────────────────────────────────────────────────────────────

type OverlayMutex = parking_lot::Mutex<Inner>;

/// A read-write cursor over a plain (non-DupSort) table.
pub struct MemoryWriteCursor<T: Table> {
    inner: MemoryCursor<T>,
    /// Reference to the overlay so writes can be reflected.
    overlay: Arc<OverlayMutex>,
    /// Table name for write operations.
    table_name: &'static str,
}

impl<T: Table> fmt::Debug for MemoryWriteCursor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryWriteCursor").field("pos", &self.inner.pos).finish()
    }
}

impl<T: Table> MemoryWriteCursor<T> {
    const fn new(entries: Vec<(Vec<u8>, Vec<u8>)>, overlay: Arc<OverlayMutex>) -> Self {
        Self { inner: MemoryCursor::new(entries), overlay, table_name: T::NAME }
    }

    /// Refresh the cursor's snapshot from the overlay (after a write).
    fn refresh(&mut self) {
        let current_key = self.inner.pos.map(|i| self.inner.entries[i].0.clone());
        let new_entries = self.overlay.lock().table(self.table_name).entries();
        self.inner.entries = new_entries;
        // Reposition cursor to same key (or nearest).
        if let Some(key) = current_key {
            match self.inner.entries.binary_search_by(|(k, _)| k.as_slice().cmp(&key)) {
                Ok(i) | Err(i) => self.inner.pos = (i < self.inner.entries.len()).then_some(i),
            }
        }
    }
}

impl<T: Table> DbCursorRO<T> for MemoryWriteCursor<T> {
    fn first(&mut self) -> PairResult<T> {
        self.inner.first()
    }
    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> {
        self.inner.seek_exact(key)
    }
    fn seek(&mut self, key: T::Key) -> PairResult<T> {
        self.inner.seek(key)
    }
    fn next(&mut self) -> PairResult<T> {
        self.inner.next()
    }
    fn prev(&mut self) -> PairResult<T> {
        self.inner.prev()
    }
    fn last(&mut self) -> PairResult<T> {
        self.inner.last()
    }
    fn current(&mut self) -> PairResult<T> {
        self.inner.current()
    }
    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            None => self.first(),
            Some(k) => self.seek(k),
        }
        .transpose();
        Ok(Walker::new(self, start))
    }
    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(k) => self.seek(k.clone()),
            Bound::Excluded(k) => {
                self.seek(k.clone())?;
                if self.current()?.as_ref().map(|(ck, _)| ck == k).unwrap_or(false) {
                    self.next()
                } else {
                    self.current()
                }
            }
            Bound::Unbounded => self.first(),
        }
        .transpose();
        let end_key = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(RangeWalker::new(self, start, end_key))
    }
    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            None => self.last(),
            Some(k) => {
                let encoded = k.encode();
                let target = encoded.as_ref();
                match self.inner.entries.binary_search_by(|(ek, _)| ek.as_slice().cmp(target)) {
                    Ok(i) => {
                        self.inner.pos = Some(i);
                        self.inner.current_kv()
                    }
                    Err(0) => {
                        self.inner.pos = None;
                        Ok(None)
                    }
                    Err(i) => {
                        self.inner.pos = Some(i - 1);
                        self.inner.current_kv()
                    }
                }
            }
        }
        .transpose();
        Ok(ReverseWalker::new(self, start))
    }
}

impl<T: Table> DbCursorRW<T> for MemoryWriteCursor<T> {
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let key_bytes = key.encode().into_vec();
        let val_bytes = encode_value::<T>(value);
        self.overlay.lock().table_mut(self.table_name).put(key_bytes, val_bytes);
        self.refresh();
        Ok(())
    }

    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let key_bytes = key.encode().into_vec();
        {
            let overlay = self.overlay.lock();
            let store = overlay.table(self.table_name);
            if store.get(&key_bytes).is_some() {
                return Err(DatabaseWriteError {
                    info: DatabaseErrorInfo { message: "key already exists".into(), code: 0 },
                    operation: DatabaseWriteOperation::CursorInsert,
                    table_name: self.table_name,
                    key: key_bytes,
                }
                .into());
            }
        }
        let val_bytes = encode_value::<T>(value);
        self.overlay.lock().table_mut(self.table_name).put(key_bytes, val_bytes);
        self.refresh();
        Ok(())
    }

    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.upsert(key, value)
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        if let Some(i) = self.inner.pos {
            let key_bytes = self.inner.entries[i].0.clone();
            self.overlay.lock().table_mut(self.table_name).remove(&key_bytes);
            self.refresh();
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryDupCursor — read-only DupSort cursor
// ─────────────────────────────────────────────────────────────────────────────

/// A read-only cursor over a `DupSort` table.
///
/// Internally treats the flattened `(key, value)` pairs the same as
/// [`MemoryCursor`] — the dupsort grouping is handled by key equality.
pub struct MemoryDupCursor<T: DupSort> {
    inner: MemoryCursor<T>,
}

impl<T: DupSort> fmt::Debug for MemoryDupCursor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryDupCursor").field("pos", &self.inner.pos).finish()
    }
}

impl<T: DupSort> MemoryDupCursor<T> {
    const fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { inner: MemoryCursor::new(entries) }
    }

    fn current_key_bytes(&self) -> Option<Vec<u8>> {
        self.inner.pos.map(|i| self.inner.entries[i].0.clone())
    }
}

impl<T: DupSort> DbCursorRO<T> for MemoryDupCursor<T> {
    fn first(&mut self) -> PairResult<T> {
        self.inner.first()
    }
    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> {
        self.inner.seek_exact(key)
    }
    fn seek(&mut self, key: T::Key) -> PairResult<T> {
        self.inner.seek(key)
    }
    fn next(&mut self) -> PairResult<T> {
        self.inner.next()
    }
    fn prev(&mut self) -> PairResult<T> {
        self.inner.prev()
    }
    fn last(&mut self) -> PairResult<T> {
        self.inner.last()
    }
    fn current(&mut self) -> PairResult<T> {
        self.inner.current()
    }
    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            None => self.first(),
            Some(k) => self.seek(k),
        }
        .transpose();
        Ok(Walker::new(self, start))
    }
    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(k) => self.seek(k.clone()),
            Bound::Excluded(k) => {
                self.seek(k.clone())?;
                if self.current()?.as_ref().map(|(ck, _)| ck == k).unwrap_or(false) {
                    self.next()
                } else {
                    self.current()
                }
            }
            Bound::Unbounded => self.first(),
        }
        .transpose();
        let end_key = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(RangeWalker::new(self, start, end_key))
    }
    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            None => self.last(),
            Some(k) => {
                let encoded = k.encode();
                let target = encoded.as_ref();
                match self.inner.entries.binary_search_by(|(ek, _)| ek.as_slice().cmp(target)) {
                    Ok(i) => {
                        self.inner.pos = Some(i);
                        self.inner.current_kv()
                    }
                    Err(0) => {
                        self.inner.pos = None;
                        Ok(None)
                    }
                    Err(i) => {
                        self.inner.pos = Some(i - 1);
                        self.inner.current_kv()
                    }
                }
            }
        }
        .transpose();
        Ok(ReverseWalker::new(self, start))
    }
}

impl<T: DupSort> DbDupCursorRO<T> for MemoryDupCursor<T> {
    fn prev_dup(&mut self) -> PairResult<T> {
        let cur_key = match self.current_key_bytes() {
            None => return Ok(None),
            Some(k) => k,
        };
        let prev_pos = self.inner.pos.and_then(|i| i.checked_sub(1));
        if let Some(pos) = prev_pos &&
            self.inner.entries[pos].0 == cur_key
        {
            self.inner.pos = Some(pos);
            return self.inner.current_kv();
        }
        Ok(None)
    }

    fn next_dup(&mut self) -> PairResult<T> {
        let cur_key = match self.current_key_bytes() {
            None => return Ok(None),
            Some(k) => k,
        };
        // peek at next
        let next_pos = self.inner.pos.map(|i| i + 1);
        if let Some(pos) = next_pos &&
            pos < self.inner.entries.len() &&
            self.inner.entries[pos].0 == cur_key
        {
            self.inner.pos = Some(pos);
            return self.inner.current_kv();
        }
        Ok(None)
    }

    fn next_no_dup(&mut self) -> PairResult<T> {
        let cur_key = match self.current_key_bytes() {
            None => return self.inner.next(),
            Some(k) => k,
        };
        // skip all entries with the same key
        loop {
            match self.inner.next()? {
                None => return Ok(None),
                Some((k, v)) if k.clone().encode().as_ref() != cur_key.as_slice() => {
                    return Ok(Some((k, v)));
                }
                _ => {}
            }
        }
    }

    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        Ok(self.next_dup()?.map(|(_, v)| v))
    }

    fn last_dup(&mut self) -> ValueOnlyResult<T> {
        let cur_key = match self.current_key_bytes() {
            None => return Ok(None),
            Some(k) => k,
        };
        // advance to last entry with the same key
        let mut last_val = None;
        while let Some(i) = self.inner.pos {
            if i < self.inner.entries.len() && self.inner.entries[i].0 == cur_key {
                last_val = Some(self.inner.entries[i].1.clone());
                self.inner.pos = Some(i + 1);
            } else {
                // step back to last matching
                self.inner.pos = Some(i.saturating_sub(1));
                break;
            }
        }
        match last_val {
            None => Ok(None),
            Some(bytes) => Ok(Some(decode_value::<T>(&bytes)?)),
        }
    }

    fn seek_by_key_subkey(&mut self, key: T::Key, subkey: T::SubKey) -> ValueOnlyResult<T> {
        // Position at start of key's entries
        let key_encoded = key.encode();
        let key_bytes = key_encoded.as_ref();
        let subkey_encoded = subkey.encode();
        let subkey_bytes = subkey_encoded.as_ref();

        // Find first entry with this key
        match self.inner.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key_bytes)) {
            Err(_) => {
                self.inner.pos = None;
                Ok(None)
            }
            Ok(mut i) => {
                // binary_search may land in the middle of dups — walk back to first
                while i > 0 && self.inner.entries[i - 1].0 == self.inner.entries[i].0 {
                    i -= 1;
                }
                // Now scan forward for value >= subkey
                while i < self.inner.entries.len() &&
                    self.inner.entries[i].0.as_slice() == key_bytes
                {
                    if self.inner.entries[i].1.as_slice() >= subkey_bytes {
                        self.inner.pos = Some(i);
                        return Ok(Some(decode_value::<T>(&self.inner.entries[i].1)?));
                    }
                    i += 1;
                }
                self.inner.pos = None;
                Ok(None)
            }
        }
    }

    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        subkey: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match (key, subkey) {
            (None, None) => self.first(),
            (Some(k), None) => self.seek_exact(k),
            (None, Some(sk)) => {
                // seek_by_key_subkey needs a key — ambiguous, just use first
                let _ = sk;
                self.first()
            }
            (Some(k), Some(sk)) => self.seek_by_key_subkey(k, sk).map(|v| {
                // reconstruct the pair from current position
                v.and_then(|_| self.inner.current_kv().ok().flatten())
            }),
        }
        .transpose();
        Ok(DupWalker { cursor: self, start })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MemoryDupWriteCursor — read-write DupSort cursor
// ─────────────────────────────────────────────────────────────────────────────

/// A read-write cursor over a `DupSort` table.
pub struct MemoryDupWriteCursor<T: DupSort> {
    inner: MemoryDupCursor<T>,
    overlay: Arc<OverlayMutex>,
    table_name: &'static str,
}

impl<T: DupSort> fmt::Debug for MemoryDupWriteCursor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryDupWriteCursor").finish()
    }
}

impl<T: DupSort> MemoryDupWriteCursor<T> {
    const fn new(entries: Vec<(Vec<u8>, Vec<u8>)>, overlay: Arc<OverlayMutex>) -> Self {
        Self { inner: MemoryDupCursor::new(entries), overlay, table_name: T::NAME }
    }

    fn refresh(&mut self) {
        let current_key = self.inner.current_key_bytes();
        let current_val = self.inner.inner.pos.map(|i| self.inner.inner.entries[i].1.clone());
        let new_entries = self.overlay.lock().table(self.table_name).entries();
        self.inner.inner.entries = new_entries;
        if let (Some(k), Some(v)) = (current_key, current_val) {
            match self.inner.inner.entries.binary_search_by(|(ek, ev)| {
                ek.as_slice().cmp(&k).then_with(|| ev.as_slice().cmp(&v))
            }) {
                Ok(i) | Err(i) => {
                    self.inner.inner.pos = (i < self.inner.inner.entries.len()).then_some(i)
                }
            }
        }
    }
}

impl<T: DupSort> DbCursorRO<T> for MemoryDupWriteCursor<T> {
    fn first(&mut self) -> PairResult<T> {
        self.inner.first()
    }
    fn seek_exact(&mut self, key: T::Key) -> PairResult<T> {
        self.inner.seek_exact(key)
    }
    fn seek(&mut self, key: T::Key) -> PairResult<T> {
        self.inner.seek(key)
    }
    fn next(&mut self) -> PairResult<T> {
        self.inner.next()
    }
    fn prev(&mut self) -> PairResult<T> {
        self.inner.prev()
    }
    fn last(&mut self) -> PairResult<T> {
        self.inner.last()
    }
    fn current(&mut self) -> PairResult<T> {
        self.inner.current()
    }
    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            None => self.first(),
            Some(k) => self.seek(k),
        }
        .transpose();
        Ok(Walker::new(self, start))
    }
    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match range.start_bound() {
            Bound::Included(k) => self.seek(k.clone()),
            Bound::Excluded(k) => {
                self.seek(k.clone())?;
                if self.current()?.as_ref().map(|(ck, _)| ck == k).unwrap_or(false) {
                    self.next()
                } else {
                    self.current()
                }
            }
            Bound::Unbounded => self.first(),
        }
        .transpose();
        let end_key = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(RangeWalker::new(self, start, end_key))
    }
    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match start_key {
            None => self.last(),
            Some(k) => {
                let encoded = k.encode();
                let target = encoded.as_ref();
                match self.inner.inner.entries.binary_search_by(|(ek, _)| ek.as_slice().cmp(target))
                {
                    Ok(i) => {
                        self.inner.inner.pos = Some(i);
                        self.inner.inner.current_kv()
                    }
                    Err(0) => {
                        self.inner.inner.pos = None;
                        Ok(None)
                    }
                    Err(i) => {
                        self.inner.inner.pos = Some(i - 1);
                        self.inner.inner.current_kv()
                    }
                }
            }
        }
        .transpose();
        Ok(ReverseWalker::new(self, start))
    }
}

impl<T: DupSort> DbDupCursorRO<T> for MemoryDupWriteCursor<T> {
    fn prev_dup(&mut self) -> PairResult<T> {
        self.inner.prev_dup()
    }
    fn next_dup(&mut self) -> PairResult<T> {
        self.inner.next_dup()
    }
    fn next_no_dup(&mut self) -> PairResult<T> {
        self.inner.next_no_dup()
    }
    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        self.inner.next_dup_val()
    }
    fn last_dup(&mut self) -> ValueOnlyResult<T> {
        self.inner.last_dup()
    }
    fn seek_by_key_subkey(&mut self, key: T::Key, subkey: T::SubKey) -> ValueOnlyResult<T> {
        self.inner.seek_by_key_subkey(key, subkey)
    }
    fn walk_dup(
        &mut self,
        key: Option<T::Key>,
        subkey: Option<T::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError>
    where
        Self: Sized,
    {
        let start = match (key, subkey) {
            (Some(k), None) => self.seek_exact(k),
            (None, _) => self.first(),
            (Some(k), Some(sk)) => self
                .seek_by_key_subkey(k, sk)
                .map(|v| v.and_then(|_| self.inner.inner.current_kv().ok().flatten())),
        }
        .transpose();
        Ok(DupWalker { cursor: self, start })
    }
}

impl<T: DupSort> DbCursorRW<T> for MemoryDupWriteCursor<T> {
    fn upsert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        let key_bytes = key.encode().into_vec();
        let val_bytes = encode_value::<T>(value);
        self.overlay.lock().table_mut(self.table_name).dup_put(key_bytes, val_bytes);
        self.refresh();
        Ok(())
    }

    fn insert(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        // DupSort insert: add the value to the key's set (duplicate-safe)
        self.upsert(key, value)
    }

    fn append(&mut self, key: T::Key, value: &T::Value) -> Result<(), DatabaseError> {
        self.upsert(key, value)
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        if let Some(i) = self.inner.inner.pos {
            let key_bytes = self.inner.inner.entries[i].0.clone();
            let val_bytes = self.inner.inner.entries[i].1.clone();
            self.overlay.lock().table_mut(self.table_name).dup_remove_value(&key_bytes, &val_bytes);
            self.refresh();
        }
        Ok(())
    }
}

impl<T: DupSort> DbDupCursorRW<T> for MemoryDupWriteCursor<T> {
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        if let Some(key) = self.inner.current_key_bytes() {
            self.overlay.lock().table_mut(self.table_name).dup_remove_key(&key);
            self.refresh();
        }
        Ok(())
    }

    fn append_dup(&mut self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        let key_bytes = key.encode().into_vec();
        let val_bytes = encode_value::<T>(&value);
        self.overlay.lock().table_mut(self.table_name).dup_put(key_bytes, val_bytes);
        self.refresh();
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256};
    use reth_db_api::{
        tables::{CanonicalHeaders, PlainAccountState, PlainStorageState},
        transaction::{DbTx, DbTxMut},
    };
    use reth_primitives_traits::{Account, StorageEntry};

    fn make_db() -> MemoryDatabase {
        MemoryDatabase::new()
    }

    #[test]
    fn test_plain_put_get_delete() {
        let db = make_db();
        let tx = db.tx_mut().unwrap();

        let addr = Address::repeat_byte(1);
        let account = Account { nonce: 1, balance: U256::from(100), bytecode_hash: None };

        tx.put::<PlainAccountState>(addr, account).unwrap();

        let got = tx.get::<PlainAccountState>(addr).unwrap();
        assert_eq!(got, Some(account));

        tx.delete::<PlainAccountState>(addr, None).unwrap();
        assert_eq!(tx.get::<PlainAccountState>(addr).unwrap(), None);

        tx.commit().unwrap();
    }

    #[test]
    fn test_commit_visible_to_new_tx() {
        let db = make_db();
        {
            let tx = db.tx_mut().unwrap();
            let addr = Address::repeat_byte(2);
            let account = Account { nonce: 5, balance: U256::from(42), bytecode_hash: None };
            tx.put::<PlainAccountState>(addr, account).unwrap();
            tx.commit().unwrap();
        }
        {
            let tx = db.tx().unwrap();
            let addr = Address::repeat_byte(2);
            let got = tx.get::<PlainAccountState>(addr).unwrap();
            assert!(got.is_some());
            assert_eq!(got.unwrap().nonce, 5);
        }
    }

    #[test]
    fn test_abort_not_visible() {
        let db = make_db();
        {
            let tx = db.tx_mut().unwrap();
            let addr = Address::repeat_byte(3);
            let account = Account { nonce: 99, balance: U256::ZERO, bytecode_hash: None };
            tx.put::<PlainAccountState>(addr, account).unwrap();
            tx.abort(); // don't commit
        }
        {
            let tx = db.tx().unwrap();
            let addr = Address::repeat_byte(3);
            assert_eq!(tx.get::<PlainAccountState>(addr).unwrap(), None);
        }
    }

    #[test]
    fn test_cursor_walk_plain() {
        let db = make_db();
        let tx = db.tx_mut().unwrap();
        for i in 0u64..5 {
            tx.put::<CanonicalHeaders>(i, B256::repeat_byte(i as u8)).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.tx().unwrap();
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
        let entries: Vec<_> = cursor.walk(None).unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].0, 0);
        assert_eq!(entries[4].0, 4);
    }

    #[test]
    fn test_cursor_seek_exact() {
        let db = make_db();
        let tx = db.tx_mut().unwrap();
        for i in 0u64..5 {
            tx.put::<CanonicalHeaders>(i, B256::repeat_byte(i as u8)).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.tx().unwrap();
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
        let result = cursor.seek_exact(2).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().0, 2);

        let none = cursor.seek_exact(99).unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn test_cursor_walk_range() {
        let db = make_db();
        let tx = db.tx_mut().unwrap();
        for i in 0u64..10 {
            tx.put::<CanonicalHeaders>(i, B256::repeat_byte(i as u8)).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.tx().unwrap();
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
        let entries: Vec<_> =
            cursor.walk_range(3u64..=6u64).unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].0, 3);
        assert_eq!(entries[3].0, 6);
    }

    #[test]
    fn test_cursor_walk_back() {
        let db = make_db();
        let tx = db.tx_mut().unwrap();
        for i in 0u64..5 {
            tx.put::<CanonicalHeaders>(i, B256::repeat_byte(i as u8)).unwrap();
        }
        tx.commit().unwrap();

        let tx = db.tx().unwrap();
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
        let entries: Vec<_> = cursor.walk_back(None).unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].0, 4);
        assert_eq!(entries[4].0, 0);
    }

    #[test]
    fn test_dupsort_put_walk() {
        let db = make_db();
        let tx = db.tx_mut().unwrap();

        let addr = Address::repeat_byte(0xAA);
        let entry1 = StorageEntry { key: B256::repeat_byte(1), value: U256::from(10) };
        let entry2 = StorageEntry { key: B256::repeat_byte(2), value: U256::from(20) };

        tx.put::<PlainStorageState>(addr, entry1).unwrap();
        tx.put::<PlainStorageState>(addr, entry2).unwrap();
        tx.commit().unwrap();

        let tx = db.tx().unwrap();
        let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
        let entries: Vec<_> =
            cursor.walk_dup(Some(addr), None).unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_snapshot_restore() {
        let db = make_db();

        let addr = Address::repeat_byte(1);
        let account = Account { nonce: 1, balance: U256::from(100), bytecode_hash: None };
        {
            let tx = db.tx_mut().unwrap();
            tx.put::<PlainAccountState>(addr, account).unwrap();
            tx.commit().unwrap();
        }

        let snap = db.snapshot();

        {
            let tx = db.tx_mut().unwrap();
            tx.delete::<PlainAccountState>(addr, None).unwrap();
            tx.commit().unwrap();
        }
        assert_eq!(db.tx().unwrap().get::<PlainAccountState>(addr).unwrap(), None);

        db.restore(snap);
        assert_eq!(db.tx().unwrap().get::<PlainAccountState>(addr).unwrap(), Some(account));
    }

    #[test]
    fn test_clear() {
        let db = make_db();
        let tx = db.tx_mut().unwrap();
        for i in 0u64..5 {
            tx.put::<CanonicalHeaders>(i, B256::repeat_byte(i as u8)).unwrap();
        }
        tx.clear::<CanonicalHeaders>().unwrap();
        assert_eq!(tx.entries::<CanonicalHeaders>().unwrap(), 0);
        tx.commit().unwrap();
    }
}
