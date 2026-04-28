//! Backend-independent canonical-state dump API.
//!
//! This is intended for dev nodes and tooling that need to enumerate canonical
//! account state without depending on backend table layout.

use alloc::{collections::BTreeMap, vec::Vec};
use alloy_primitives::{Address, Bytes, B256, U256};
use reth_storage_errors::provider::{ProviderError, ProviderResult};

/// Metadata for the canonical state being dumped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateDumpHeader {
    /// Canonical block number sampled by the dump.
    pub block_number: u64,
    /// Canonical block hash sampled by the dump.
    pub block_hash: B256,
    /// State root committed in the sampled block header.
    pub state_root: B256,
}

/// A single account emitted by a state dump.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpedAccount {
    /// Account address.
    pub address: Address,
    /// Account nonce.
    pub nonce: u64,
    /// Account balance.
    pub balance: U256,
    /// Raw bytecode if the account is a contract.
    pub code: Option<Bytes>,
    /// Non-zero storage slots keyed by the original unhashed storage key.
    pub storage: BTreeMap<B256, B256>,
}

/// Summary returned after a state dump completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateDumpSummary {
    /// Dump metadata.
    pub header: StateDumpHeader,
    /// Number of accounts emitted.
    pub accounts: usize,
    /// Number of non-zero storage slots emitted.
    pub storage_slots: usize,
}

/// Complete in-memory state dump.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpedState {
    /// Dump metadata.
    pub header: StateDumpHeader,
    /// Dumped accounts.
    pub accounts: Vec<DumpedAccount>,
}

/// Sink for streaming state dump output.
pub trait StateDumpSink {
    /// Called once before account emission starts.
    fn on_header(&mut self, _header: &StateDumpHeader) -> ProviderResult<()> {
        Ok(())
    }

    /// Called once per dumped account.
    fn on_account(&mut self, account: DumpedAccount) -> ProviderResult<()>;
}

/// In-memory collector for consumers that need the full dump as a value.
#[derive(Debug, Default)]
pub struct StateDumpCollector {
    header: Option<StateDumpHeader>,
    accounts: Vec<DumpedAccount>,
}

impl StateDumpCollector {
    /// Returns the collected dump, if a header was emitted.
    pub fn into_state(self) -> ProviderResult<DumpedState> {
        self.header
            .map(|header| DumpedState { header, accounts: self.accounts })
            .ok_or(ProviderError::InvalidStorageOutput)
    }
}

impl StateDumpSink for StateDumpCollector {
    fn on_header(&mut self, header: &StateDumpHeader) -> ProviderResult<()> {
        self.header = Some(*header);
        Ok(())
    }

    fn on_account(&mut self, account: DumpedAccount) -> ProviderResult<()> {
        self.accounts.push(account);
        Ok(())
    }
}

/// Read-side trait for enumerating canonical state.
pub trait StateDumpProvider {
    /// Dump canonical state at the latest canonical tip.
    fn dump_state<S>(&self, sink: &mut S) -> ProviderResult<StateDumpSummary>
    where
        S: StateDumpSink + ?Sized;

    /// Collect a state dump into memory.
    ///
    /// This is a convenience method for concrete providers; use [`Self::dump_state`] for trait
    /// objects.
    fn dump_state_collect(&self) -> ProviderResult<DumpedState>
    where
        Self: Sized,
    {
        let mut collector = StateDumpCollector::default();
        self.dump_state(&mut collector)?;
        collector.into_state()
    }
}
