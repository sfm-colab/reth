//! Command exporting the latest canonical state as a JSONL dump.

use crate::common::{AccessRights, CliNodeTypes, Environment, EnvironmentArgs};
use alloy_consensus::BlockHeader as AlloyBlockHeader;
use alloy_primitives::{Address, B256};
use alloy_rlp::Encodable;
use clap::Parser;
use eyre::{eyre, Result};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_cli::chainspec::ChainSpecParser;
use reth_fs_util::{create_file, write};
use reth_node_api::NodePrimitives;
use reth_provider::{
    BlockHashReader, BlockNumReader, HeaderProvider, ProviderError, ProviderResult,
};
use reth_storage_api::{DumpedAccount, StateDumpHeader, StateDumpProvider, StateDumpSink};
use reth_tasks::Runtime;
use serde::Serialize;
use std::{
    io::{BufWriter, Result as IoResult, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::info;

/// Export the latest canonical state as a JSONL dump.
#[derive(Debug, Parser)]
pub struct ExportStateCommand<C: ChainSpecParser> {
    #[command(flatten)]
    env: EnvironmentArgs<C>,

    /// Optional file where the latest canonical header will be written as raw RLP bytes.
    ///
    /// This can be passed to `reth init-state --without-evm --header <HEADER_FILE>` for
    /// non-genesis imports.
    #[arg(long, value_name = "HEADER_FILE", verbatim_doc_comment)]
    header: Option<PathBuf>,

    /// Destination JSONL file for the exported state dump.
    #[arg(value_name = "STATE_DUMP_FILE", verbatim_doc_comment)]
    output: PathBuf,
}

impl<C: ChainSpecParser<ChainSpec: EthChainSpec + EthereumHardforks>> ExportStateCommand<C> {
    /// Execute `export-state` command.
    pub async fn execute<N>(self, runtime: Runtime) -> Result<()>
    where
        N: CliNodeTypes<
            ChainSpec = C::ChainSpec,
            Primitives: NodePrimitives<BlockHeader: AlloyBlockHeader + Encodable>,
        >,
    {
        let Environment { provider_factory, .. } = self.env.init::<N>(AccessRights::RO, runtime)?;
        let provider = provider_factory.provider()?;

        if let Some(path) = self.header.as_deref() {
            let block_number = provider.best_block_number()?;
            let block_hash = provider
                .block_hash(block_number)?
                .ok_or_else(|| eyre!("Block hash not found for block {block_number}"))?;
            let header = provider
                .header_by_number(block_number)?
                .ok_or_else(|| eyre!("Header not found for block {block_number}"))?;
            write_parent_dir(path)?;
            write(path, alloy_rlp::encode(&header))?;
            info!(
                target: "reth::cli",
                block = block_number,
                hash = ?block_hash,
                path = %path.display(),
                "Wrote header sidecar"
            );
        }

        write_parent_dir(&self.output)?;
        let file = create_file(&self.output)?;
        let mut writer = CountingWriter::new(BufWriter::new(file));
        let mut sink = JsonlStateDumpWriter::new(&mut writer);

        let summary = provider.dump_state(&mut sink)?;
        writer.flush()?;

        info!(
            target: "reth::cli",
            block = summary.header.block_number,
            hash = ?summary.header.block_hash,
            state_root = ?summary.header.state_root,
            accounts = summary.accounts,
            storage_slots = summary.storage_slots,
            bytes_written = writer.bytes_written(),
            output = %self.output.display(),
            "State export complete"
        );

        Ok(())
    }
}

impl<C: ChainSpecParser> ExportStateCommand<C> {
    /// Returns the underlying chain being used to run this command.
    pub fn chain_spec(&self) -> Option<&Arc<C::ChainSpec>> {
        Some(&self.env.chain)
    }
}

struct CountingWriter<W> {
    inner: W,
    bytes_written: u64,
}

impl<W> CountingWriter<W> {
    const fn new(inner: W) -> Self {
        Self { inner, bytes_written: 0 }
    }

    const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let written = self.inner.write(buf)?;
        self.bytes_written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.inner.flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> IoResult<()> {
        self.inner.write_all(buf)?;
        self.bytes_written += buf.len() as u64;
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct StateRootLine {
    root: B256,
}

struct JsonlStateDumpWriter<'a, W> {
    writer: &'a mut W,
}

impl<'a, W> JsonlStateDumpWriter<'a, W> {
    const fn new(writer: &'a mut W) -> Self {
        Self { writer }
    }
}

impl<W: Write> StateDumpSink for JsonlStateDumpWriter<'_, W> {
    fn on_header(&mut self, header: &StateDumpHeader) -> ProviderResult<()> {
        serde_json::to_writer(&mut *self.writer, &StateRootLine { root: header.state_root })
            .map_err(ProviderError::other)?;
        self.writer.write_all(b"\n").map_err(ProviderError::other)
    }

    fn on_account(&mut self, account: DumpedAccount) -> ProviderResult<()> {
        write_account_line(self.writer, &account)
    }
}

fn write_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        reth_fs_util::create_dir_all(parent)?;
    }
    Ok(())
}

fn write_account_line<W: Write>(writer: &mut W, account: &DumpedAccount) -> ProviderResult<()> {
    writer.write_all(br#"{"balance":"#).map_err(ProviderError::other)?;
    serde_json::to_writer(&mut *writer, &account.balance).map_err(ProviderError::other)?;
    writer.write_all(br#","nonce":"#).map_err(ProviderError::other)?;
    serde_json::to_writer(&mut *writer, &account.nonce).map_err(ProviderError::other)?;

    if let Some(code) = account.code.as_ref() {
        writer.write_all(br#","code":"#).map_err(ProviderError::other)?;
        serde_json::to_writer(&mut *writer, code).map_err(ProviderError::other)?;
    }

    let mut first_storage_entry = true;
    for (key, value) in &account.storage {
        write_storage_entry(writer, &mut first_storage_entry, *key, *value)?;
    }

    finish_account_line(writer, account.address, !first_storage_entry)
}

fn write_storage_entry<W: Write>(
    writer: &mut W,
    first_storage_entry: &mut bool,
    key: B256,
    value: B256,
) -> ProviderResult<()> {
    if *first_storage_entry {
        writer.write_all(br#","storage":{"#).map_err(ProviderError::other)?;
        *first_storage_entry = false;
    } else {
        writer.write_all(b",").map_err(ProviderError::other)?;
    }

    serde_json::to_writer(&mut *writer, &key).map_err(ProviderError::other)?;
    writer.write_all(b":").map_err(ProviderError::other)?;
    serde_json::to_writer(&mut *writer, &value).map_err(ProviderError::other)?;
    Ok(())
}

fn finish_account_line<W: Write>(
    writer: &mut W,
    address: Address,
    wrote_storage: bool,
) -> ProviderResult<()> {
    if wrote_storage {
        writer.write_all(b"}").map_err(ProviderError::other)?;
    }

    writer.write_all(br#","address":"#).map_err(ProviderError::other)?;
    serde_json::to_writer(&mut *writer, &address).map_err(ProviderError::other)?;
    writer.write_all(b"}\n").map_err(ProviderError::other)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_state::without_evm::read_header_from_file;
    use alloy_consensus::Header;
    use alloy_genesis::{Genesis, GenesisAccount};
    use alloy_primitives::{Bytes, U256};
    use clap::Parser;
    use reth_chainspec::{Chain, ChainSpec};
    use reth_db_api::{models::storage_sharded_key::StorageShardedKey, StoragesHistory};
    use reth_db_common::init::init_genesis_with_settings;
    use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
    use reth_provider::{
        test_utils::{create_test_provider_factory_with_chain_spec, MockNodeTypesWithDB},
        BlockNumReader, DBProvider, DatabaseProviderFactory, ProviderFactory,
        PruneCheckpointWriter, RocksDBProviderFactory, StorageSettings,
    };
    use reth_prune_types::{PruneCheckpoint, PruneMode, PruneSegment};
    use std::collections::BTreeMap;
    use tempfile::NamedTempFile;

    #[test]
    fn parse_export_state_command() {
        let cmd: ExportStateCommand<EthereumChainSpecParser> = ExportStateCommand::parse_from([
            "reth",
            "--chain",
            "sepolia",
            "--header",
            "header.rlp",
            "state.jsonl",
        ]);

        assert_eq!(cmd.output.to_str().unwrap(), "state.jsonl");
        assert_eq!(cmd.header.unwrap().to_str().unwrap(), "header.rlp");
    }

    #[test]
    fn export_state_v1_includes_accounts_and_storage() {
        let chain_spec = test_chain_spec();
        let factory = create_test_provider_factory_with_chain_spec(chain_spec);
        init_genesis_with_settings(&factory, StorageSettings::v1()).unwrap();

        let dump = export_dump(&factory).unwrap();
        let lines: Vec<_> = String::from_utf8(dump).unwrap().lines().map(str::to_owned).collect();

        assert_eq!(lines.len(), 3);

        let account_lines: Vec<serde_json::Value> =
            lines[1..].iter().map(|line| serde_json::from_str(line).unwrap()).collect();
        assert!(account_lines.iter().any(|line| {
            line.get("address") ==
                Some(&serde_json::Value::String(Address::with_last_byte(1).to_string()))
        }));
        assert!(account_lines.iter().any(|line| {
            line.get("address") ==
                Some(&serde_json::Value::String(Address::with_last_byte(2).to_string())) &&
                line.get("storage").is_some()
        }));
    }

    #[test]
    fn export_state_v2_uses_history_indices() {
        let chain_spec = test_chain_spec();
        let factory = create_test_provider_factory_with_chain_spec(chain_spec);
        init_genesis_with_settings(&factory, StorageSettings::v2()).unwrap();

        let dump = export_dump(&factory).unwrap();
        let lines: Vec<_> = String::from_utf8(dump).unwrap().lines().map(str::to_owned).collect();

        assert_eq!(lines.len(), 3);

        let root_line: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert!(root_line.get("root").is_some());

        let account_lines: Vec<serde_json::Value> =
            lines[1..].iter().map(|line| serde_json::from_str(line).unwrap()).collect();
        assert!(account_lines.iter().any(|line| {
            line.get("address") ==
                Some(&serde_json::Value::String(Address::with_last_byte(1).to_string()))
        }));
        assert!(account_lines.iter().any(|line| {
            line.get("address") ==
                Some(&serde_json::Value::String(Address::with_last_byte(2).to_string()))
        }));
    }

    #[test]
    fn export_state_v2_rejects_pruned_history() {
        let chain_spec = test_chain_spec();
        let factory = create_test_provider_factory_with_chain_spec(chain_spec);
        init_genesis_with_settings(&factory, StorageSettings::v2()).unwrap();

        let provider_rw = factory.database_provider_rw().unwrap();
        provider_rw
            .save_prune_checkpoint(
                PruneSegment::AccountHistory,
                PruneCheckpoint {
                    block_number: Some(0),
                    tx_number: None,
                    prune_mode: PruneMode::Full,
                },
            )
            .unwrap();
        provider_rw.commit().unwrap();

        let err = export_dump(&factory).unwrap_err();
        assert!(err.to_string().contains("pruned AccountHistory history"));
    }

    #[test]
    fn export_state_v2_rejects_missing_all_live_storage_history_for_account() {
        let chain_spec = test_chain_spec_two_storage_accounts();
        let factory = create_test_provider_factory_with_chain_spec(chain_spec);
        init_genesis_with_settings(&factory, StorageSettings::v2()).unwrap();

        let address = Address::with_last_byte(3);
        factory
            .rocksdb_provider()
            .delete::<StoragesHistory>(StorageShardedKey::last(address, B256::with_last_byte(0x01)))
            .unwrap();

        let err = export_dump(&factory).unwrap_err();
        assert!(err.to_string().contains("missing history entries"));
    }

    #[test]
    fn export_state_v2_rejects_missing_live_storage_history() {
        let chain_spec = test_chain_spec_two_storage_slots();
        let factory = create_test_provider_factory_with_chain_spec(chain_spec);
        init_genesis_with_settings(&factory, StorageSettings::v2()).unwrap();

        let address = Address::with_last_byte(2);
        let removed_slot = B256::with_last_byte(0x02);
        factory
            .rocksdb_provider()
            .delete::<StoragesHistory>(StorageShardedKey::last(address, removed_slot))
            .unwrap();

        let err = export_dump(&factory).unwrap_err();
        assert!(err.to_string().contains("missing history entry"));
    }

    #[test]
    fn exported_header_sidecar_is_readable() {
        let chain_spec = test_chain_spec();
        let factory = create_test_provider_factory_with_chain_spec(chain_spec);
        init_genesis_with_settings(&factory, StorageSettings::v1()).unwrap();

        let provider = factory.provider().unwrap();
        let block_number = provider.last_block_number().unwrap();
        let header = provider.header_by_number(block_number).unwrap().unwrap();

        let file = NamedTempFile::new().unwrap();
        reth_fs_util::write(file.path(), alloy_rlp::encode(&header)).unwrap();

        let decoded: Header = read_header_from_file(file.path()).unwrap();
        assert_eq!(decoded.hash_slow(), header.hash_slow());
    }

    fn export_dump(factory: &ProviderFactory<MockNodeTypesWithDB>) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        let mut sink = JsonlStateDumpWriter::new(&mut output);
        factory.dump_state(&mut sink)?;
        Ok(output)
    }

    fn test_chain_spec() -> Arc<ChainSpec> {
        let code = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0x55]);
        let storage_key = B256::with_last_byte(0x01);
        let storage_value = B256::with_last_byte(0x02);

        Arc::new(ChainSpec {
            chain: Chain::from_id(1),
            genesis: Genesis {
                alloc: BTreeMap::from([
                    (
                        Address::with_last_byte(1),
                        GenesisAccount::default().with_balance(U256::from(100_u64)),
                    ),
                    (
                        Address::with_last_byte(2),
                        GenesisAccount::default()
                            .with_balance(U256::from(200_u64))
                            .with_nonce(Some(7))
                            .with_code(Some(code))
                            .with_storage(Some(BTreeMap::from([(storage_key, storage_value)]))),
                    ),
                ]),
                ..Default::default()
            },
            hardforks: Default::default(),
            paris_block_and_final_difficulty: None,
            deposit_contract: None,
            ..Default::default()
        })
    }

    fn test_chain_spec_two_storage_slots() -> Arc<ChainSpec> {
        let code = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0x55]);
        let storage_key_1 = B256::with_last_byte(0x01);
        let storage_value_1 = B256::with_last_byte(0x02);
        let storage_key_2 = B256::with_last_byte(0x02);
        let storage_value_2 = B256::with_last_byte(0x03);

        Arc::new(ChainSpec {
            chain: Chain::from_id(1),
            genesis: Genesis {
                alloc: BTreeMap::from([
                    (
                        Address::with_last_byte(1),
                        GenesisAccount::default().with_balance(U256::from(100_u64)),
                    ),
                    (
                        Address::with_last_byte(2),
                        GenesisAccount::default()
                            .with_balance(U256::from(200_u64))
                            .with_nonce(Some(7))
                            .with_code(Some(code))
                            .with_storage(Some(BTreeMap::from([
                                (storage_key_1, storage_value_1),
                                (storage_key_2, storage_value_2),
                            ]))),
                    ),
                ]),
                ..Default::default()
            },
            hardforks: Default::default(),
            paris_block_and_final_difficulty: None,
            deposit_contract: None,
            ..Default::default()
        })
    }

    fn test_chain_spec_two_storage_accounts() -> Arc<ChainSpec> {
        let code = Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0x55]);
        let storage_key = B256::with_last_byte(0x01);

        Arc::new(ChainSpec {
            chain: Chain::from_id(1),
            genesis: Genesis {
                alloc: BTreeMap::from([
                    (
                        Address::with_last_byte(1),
                        GenesisAccount::default().with_balance(U256::from(100_u64)),
                    ),
                    (
                        Address::with_last_byte(2),
                        GenesisAccount::default().with_balance(U256::from(200_u64)).with_storage(
                            Some(BTreeMap::from([(storage_key, B256::with_last_byte(0x02))])),
                        ),
                    ),
                    (
                        Address::with_last_byte(3),
                        GenesisAccount::default()
                            .with_balance(U256::from(300_u64))
                            .with_nonce(Some(7))
                            .with_code(Some(code))
                            .with_storage(Some(BTreeMap::from([(
                                storage_key,
                                B256::with_last_byte(0x03),
                            )]))),
                    ),
                ]),
                ..Default::default()
            },
            hardforks: Default::default(),
            paris_block_and_final_difficulty: None,
            deposit_contract: None,
            ..Default::default()
        })
    }
}
