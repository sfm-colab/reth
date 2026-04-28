use crate::{
    providers::{
        BlockchainProvider, DatabaseProvider, NodeTypesForProvider, ProviderFactory,
        ProviderNodeTypes,
    },
    AccountReader, BlockHashReader, BlockNumReader, DatabaseProviderFactory, HeaderProvider,
    ProviderError, ProviderResult, PruneCheckpointReader, RocksDBProviderFactory,
};
use alloy_consensus::{constants::KECCAK_EMPTY, BlockHeader};
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::{keccak256, Address, Bytes, B256, U256};
use reth_db::BlockNumberList;
use reth_db_api::{
    cursor::{DbCursorRO, DbDupCursorRO},
    models::{storage_sharded_key::StorageShardedKey, ShardedKey},
    transaction::DbTx,
    AccountsHistory, Bytecodes, HashedAccounts, HashedStorages, PlainAccountState,
    PlainStorageState, StoragesHistory,
};
use reth_primitives_traits::Account;
use reth_prune_types::PruneSegment;
use reth_storage_api::{
    DBProvider, DumpedAccount, StateDumpHeader, StateDumpProvider, StateDumpSink, StateDumpSummary,
    StorageSettingsCache,
};
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    io,
    time::{Duration, Instant},
};
use tracing::info;

/// Log export progress every N accounts.
const LOG_INTERVAL_ACCOUNTS: usize = 100_000;
/// Poll long-running storage loops every N items to decide whether to emit a timed progress log.
const LOG_POLL_STORAGE_INTERVAL: usize = 10_000;
/// Emit a timed progress log at most once per interval while exporting a single large account.
const LOG_INTERVAL_DURATION: Duration = Duration::from_secs(30);

impl<TX, N> StateDumpProvider for DatabaseProvider<TX, N>
where
    TX: DbTx + 'static,
    N: NodeTypesForProvider,
{
    fn dump_state<S>(&self, sink: &mut S) -> ProviderResult<StateDumpSummary>
    where
        S: StateDumpSink + ?Sized,
    {
        let header = self.state_dump_header()?;
        sink.on_header(&header)?;

        let counters = if self.cached_storage_settings().use_hashed_state() {
            export_hashed_state(self, sink)?
        } else {
            export_plain_state(self, sink)?
        };

        Ok(StateDumpSummary {
            header,
            accounts: counters.accounts,
            storage_slots: counters.storage_slots,
        })
    }
}

impl<N> StateDumpProvider for ProviderFactory<N>
where
    N: ProviderNodeTypes,
{
    fn dump_state<S>(&self, sink: &mut S) -> ProviderResult<StateDumpSummary>
    where
        S: StateDumpSink + ?Sized,
    {
        self.provider()?.dump_state(sink)
    }
}

impl<N> StateDumpProvider for BlockchainProvider<N>
where
    N: ProviderNodeTypes,
{
    fn dump_state<S>(&self, sink: &mut S) -> ProviderResult<StateDumpSummary>
    where
        S: StateDumpSink + ?Sized,
    {
        DatabaseProviderFactory::database_provider_ro(self)?.dump_state(sink)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ExportCounters {
    accounts: usize,
    storage_slots: usize,
}

struct ProgressReporter {
    last_log: Instant,
}

struct HashedStateProgress {
    address: Address,
    history_slots: usize,
    db_slots_scanned: usize,
    written_slots: usize,
    phase: &'static str,
}

impl ProgressReporter {
    fn new() -> Self {
        Self { last_log: Instant::now() }
    }

    fn maybe_log_account_progress(
        &mut self,
        counters: ExportCounters,
        address: Address,
        account_history_slots: usize,
        account_written_slots: usize,
    ) {
        if !counters.accounts.is_multiple_of(LOG_INTERVAL_ACCOUNTS) &&
            self.last_log.elapsed() < LOG_INTERVAL_DURATION
        {
            return;
        }

        self.last_log = Instant::now();
        info!(
            target: "reth::provider::state_dump",
            accounts = counters.accounts,
            storage_slots = counters.storage_slots,
            current_address = %address,
            current_account_history_slots = account_history_slots,
            current_account_written_slots = account_written_slots,
            "Exporting state"
        );
    }

    fn maybe_log_hashed_state(
        &mut self,
        counters: ExportCounters,
        items_processed: usize,
        progress: HashedStateProgress,
    ) {
        if !items_processed.is_multiple_of(LOG_POLL_STORAGE_INTERVAL) ||
            self.last_log.elapsed() < LOG_INTERVAL_DURATION
        {
            return;
        }

        self.last_log = Instant::now();
        info!(
            target: "reth::provider::state_dump",
            accounts = counters.accounts,
            storage_slots = counters.storage_slots,
            current_address = %progress.address,
            current_account_history_slots = progress.history_slots,
            current_account_db_slots_scanned = progress.db_slots_scanned,
            current_account_written_slots = progress.written_slots,
            phase = progress.phase,
            "Exporting hashed state"
        );
    }
}

struct UniqueAccountHistoryIter<I> {
    inner: I,
    last: Option<Address>,
}

impl<I> UniqueAccountHistoryIter<I> {
    const fn new(inner: I) -> Self {
        Self { inner, last: None }
    }
}

impl<I> Iterator for UniqueAccountHistoryIter<I>
where
    I: Iterator<Item = ProviderResult<(ShardedKey<Address>, BlockNumberList)>>,
{
    type Item = ProviderResult<Address>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = match self.inner.next()? {
                Ok(next) => next,
                Err(err) => return Some(Err(err)),
            };
            let (key, _) = next;
            if self.last == Some(key.key) {
                continue;
            }
            self.last = Some(key.key);
            return Some(Ok(key.key));
        }
    }
}

struct UniqueStorageHistoryIter<I> {
    inner: I,
    last: Option<(Address, B256)>,
}

impl<I> UniqueStorageHistoryIter<I> {
    const fn new(inner: I) -> Self {
        Self { inner, last: None }
    }
}

impl<I> Iterator for UniqueStorageHistoryIter<I>
where
    I: Iterator<Item = ProviderResult<(StorageShardedKey, BlockNumberList)>>,
{
    type Item = ProviderResult<(Address, B256)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = match self.inner.next()? {
                Ok(next) => next,
                Err(err) => return Some(Err(err)),
            };
            let (key, _) = next;
            let current = (key.address, key.sharded_key.key);
            if self.last == Some(current) {
                continue;
            }
            self.last = Some(current);
            return Some(Ok(current));
        }
    }
}

impl<TX, N> DatabaseProvider<TX, N>
where
    TX: DbTx + 'static,
    N: NodeTypesForProvider,
{
    fn state_dump_header(&self) -> ProviderResult<StateDumpHeader> {
        let block_number = self.best_block_number()?;
        let block_hash = self.block_hash(block_number)?.ok_or_else(|| {
            ProviderError::HeaderNotFound(BlockHashOrNumber::Number(block_number))
        })?;

        let header = self.header_by_number(block_number)?.ok_or_else(|| {
            ProviderError::HeaderNotFound(BlockHashOrNumber::Number(block_number))
        })?;

        Ok(StateDumpHeader { block_number, block_hash, state_root: header.state_root() })
    }
}

fn export_plain_state<Provider, S>(
    provider: &Provider,
    sink: &mut S,
) -> ProviderResult<ExportCounters>
where
    Provider: DBProvider<Tx: DbTx>,
    S: StateDumpSink + ?Sized,
{
    let tx = provider.tx_ref();
    let mut accounts_cursor = tx.cursor_read::<PlainAccountState>()?;
    let mut storage_cursor = tx.cursor_dup_read::<PlainStorageState>()?;
    let mut counters = ExportCounters::default();
    let mut progress = ProgressReporter::new();

    for entry in accounts_cursor.walk(None)? {
        let (address, account) = entry?;
        let code = load_account_code(provider, &account)?;
        let mut storage = BTreeMap::new();

        for storage_entry in storage_cursor.walk_dup(Some(address), None)? {
            let (_, storage_entry) = storage_entry?;
            if storage_entry.value == U256::ZERO {
                continue;
            }

            storage.insert(storage_entry.key, B256::from(storage_entry.value.to_be_bytes()));
        }

        counters.storage_slots += storage.len();
        counters.accounts += 1;
        sink.on_account(dumped_account(address, account, code, storage))?;
        progress.maybe_log_account_progress(counters, address, 0, 0);
    }

    Ok(counters)
}

fn export_hashed_state<TX, N, S>(
    provider: &DatabaseProvider<TX, N>,
    sink: &mut S,
) -> ProviderResult<ExportCounters>
where
    TX: DbTx + 'static,
    N: NodeTypesForProvider,
    S: StateDumpSink + ?Sized,
{
    ensure_history_available(provider)?;

    let rocksdb = provider.rocksdb_provider();
    let mut account_history = UniqueAccountHistoryIter::new(rocksdb.iter::<AccountsHistory>()?);
    let mut storage_history = UniqueStorageHistoryIter::new(rocksdb.iter::<StoragesHistory>()?);
    let mut next_storage = storage_history.next().transpose()?;
    let mut hashed_storage_cursor = provider.tx_ref().cursor_dup_read::<HashedStorages>()?;
    let mut counters = ExportCounters::default();
    let mut progress = ProgressReporter::new();

    for address in &mut account_history {
        let address = address?;

        let Some(account) = provider.basic_account(&address)? else {
            skip_account_storage_history(
                &mut storage_history,
                &mut next_storage,
                address,
                counters,
                &mut progress,
            )?;
            continue;
        };

        let code = load_account_code(provider, &account)?;
        let account_storage_slots = collect_account_storage_slots(
            &mut storage_history,
            &mut next_storage,
            address,
            counters,
            &mut progress,
        )?;
        let account_history_slots = account_storage_slots.len();
        let storage = current_hashed_account_storage(
            &mut hashed_storage_cursor,
            address,
            account_storage_slots,
            counters,
            &mut progress,
        )?;
        let account_written_slots = storage.len();

        counters.storage_slots += storage.len();
        counters.accounts += 1;
        sink.on_account(dumped_account(address, account, code, storage))?;
        progress.maybe_log_account_progress(
            counters,
            address,
            account_history_slots,
            account_written_slots,
        );
    }

    if let Some((storage_address, slot_key)) = next_storage {
        return Err(state_dump_error(format!(
            "storage history is inconsistent: leftover slot {slot_key} for address {storage_address}"
        )));
    }

    Ok(counters)
}

fn skip_account_storage_history<I>(
    storage_history: &mut UniqueStorageHistoryIter<I>,
    next_storage: &mut Option<(Address, B256)>,
    address: Address,
    counters: ExportCounters,
    progress: &mut ProgressReporter,
) -> ProviderResult<()>
where
    I: Iterator<Item = ProviderResult<(StorageShardedKey, BlockNumberList)>>,
{
    let mut skipped_slots = 0usize;

    while let Some((storage_address, slot_key)) = next_storage.as_ref().copied() {
        if storage_address < address {
            return Err(state_dump_error(format!(
                "storage history is inconsistent: found slot {slot_key} for {storage_address} before its account entry"
            )));
        }
        if storage_address > address {
            break;
        }

        skipped_slots += 1;
        progress.maybe_log_hashed_state(
            counters,
            skipped_slots,
            HashedStateProgress {
                address,
                history_slots: skipped_slots,
                db_slots_scanned: 0,
                written_slots: 0,
                phase: "skipping stale storage history",
            },
        );

        *next_storage = storage_history.next().transpose()?;
    }

    Ok(())
}

fn collect_account_storage_slots<I>(
    storage_history: &mut UniqueStorageHistoryIter<I>,
    next_storage: &mut Option<(Address, B256)>,
    address: Address,
    counters: ExportCounters,
    progress: &mut ProgressReporter,
) -> ProviderResult<Vec<(B256, B256)>>
where
    I: Iterator<Item = ProviderResult<(StorageShardedKey, BlockNumberList)>>,
{
    let mut account_storage_slots = Vec::new();

    while let Some((storage_address, slot_key)) = next_storage.as_ref().copied() {
        if storage_address < address {
            return Err(state_dump_error(format!(
                "storage history is inconsistent: found slot {slot_key} for {storage_address} before its account entry"
            )));
        }
        if storage_address > address {
            break;
        }

        account_storage_slots.push((keccak256(slot_key), slot_key));
        progress.maybe_log_hashed_state(
            counters,
            account_storage_slots.len(),
            HashedStateProgress {
                address,
                history_slots: account_storage_slots.len(),
                db_slots_scanned: 0,
                written_slots: 0,
                phase: "collecting storage history",
            },
        );

        *next_storage = storage_history.next().transpose()?;
    }

    account_storage_slots.sort_unstable_by_key(|(hashed_slot, _)| *hashed_slot);
    Ok(account_storage_slots)
}

fn current_hashed_account_storage<CURSOR>(
    hashed_storage_cursor: &mut CURSOR,
    address: Address,
    account_storage_slots: Vec<(B256, B256)>,
    counters: ExportCounters,
    progress: &mut ProgressReporter,
) -> ProviderResult<BTreeMap<B256, B256>>
where
    CURSOR: DbCursorRO<HashedStorages> + DbDupCursorRO<HashedStorages>,
{
    let hashed_address = keccak256(address);

    if account_storage_slots.is_empty() {
        if hashed_storage_cursor.seek_exact(hashed_address)?.is_some() {
            return Err(state_dump_error(format!(
                "storage history is inconsistent: live storage for address {address} is missing history entries"
            )));
        }
        return Ok(BTreeMap::new());
    }

    let mut storage = BTreeMap::new();
    let history_slots = account_storage_slots.len();
    let mut account_storage_slots = account_storage_slots.into_iter().peekable();
    let mut db_slots_scanned = 0usize;
    let mut written_slots = 0usize;

    for storage_entry in hashed_storage_cursor.walk_dup(Some(hashed_address), None)? {
        let (_, storage_entry) = storage_entry?;
        db_slots_scanned += 1;

        let mut matched_history_slot = false;
        while let Some((hashed_slot, plain_slot)) = account_storage_slots.peek().copied() {
            match hashed_slot.cmp(&storage_entry.key) {
                Ordering::Less => {
                    account_storage_slots.next();
                }
                Ordering::Equal => {
                    matched_history_slot = true;
                    if storage_entry.value != U256::ZERO {
                        written_slots += 1;
                        storage.insert(plain_slot, B256::from(storage_entry.value.to_be_bytes()));
                    }

                    account_storage_slots.next();
                    break;
                }
                Ordering::Greater => {
                    if storage_entry.value != U256::ZERO {
                        return Err(state_dump_error(format!(
                            "storage history is inconsistent: live hashed slot {} for address {address} is missing history entry",
                            storage_entry.key
                        )));
                    }
                    break;
                }
            }
        }

        if !matched_history_slot &&
            account_storage_slots.peek().is_none() &&
            storage_entry.value != U256::ZERO
        {
            return Err(state_dump_error(format!(
                "storage history is inconsistent: live hashed slot {} for address {address} is missing history entry",
                storage_entry.key
            )));
        }

        progress.maybe_log_hashed_state(
            counters,
            db_slots_scanned,
            HashedStateProgress {
                address,
                history_slots,
                db_slots_scanned,
                written_slots,
                phase: "walking current hashed storage",
            },
        );
    }

    Ok(storage)
}

fn ensure_history_available<TX, N>(provider: &DatabaseProvider<TX, N>) -> ProviderResult<()>
where
    TX: DbTx + 'static,
    N: NodeTypesForProvider,
{
    for segment in [PruneSegment::AccountHistory, PruneSegment::StorageHistory] {
        if provider
            .get_prune_checkpoint(segment)?
            .is_some_and(|checkpoint| checkpoint.block_number.is_some())
        {
            return Err(state_dump_error(format!(
                "cannot export state from a storage_v2 database with pruned {segment} history"
            )));
        }
    }

    let rocksdb = provider.rocksdb_provider();
    if provider.tx_ref().entries::<HashedAccounts>()? > 0 &&
        rocksdb.first::<AccountsHistory>()?.is_none()
    {
        return Err(state_dump_error(
            "cannot export state from a storage_v2 database without account history indices",
        ));
    }

    if provider.tx_ref().entries::<HashedStorages>()? > 0 &&
        rocksdb.first::<StoragesHistory>()?.is_none()
    {
        return Err(state_dump_error(
            "cannot export state from a storage_v2 database without storage history indices",
        ));
    }

    Ok(())
}

fn load_account_code<Provider>(
    provider: &Provider,
    account: &Account,
) -> ProviderResult<Option<Bytes>>
where
    Provider: DBProvider<Tx: DbTx>,
{
    let Some(bytecode_hash) = account.bytecode_hash else {
        return Ok(None);
    };
    if bytecode_hash == KECCAK_EMPTY {
        return Ok(None);
    }

    let code = provider
        .tx_ref()
        .get::<Bytecodes>(bytecode_hash)?
        .ok_or_else(|| state_dump_error(format!("missing bytecode for hash {bytecode_hash}")))?;
    Ok(Some(code.original_bytes()))
}

const fn dumped_account(
    address: Address,
    account: Account,
    code: Option<Bytes>,
    storage: BTreeMap<B256, B256>,
) -> DumpedAccount {
    DumpedAccount { address, nonce: account.nonce, balance: account.balance, code, storage }
}

fn state_dump_error(message: impl Into<String>) -> ProviderError {
    ProviderError::other(io::Error::other(message.into()))
}
