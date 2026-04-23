//! Contains RPC handler implementations specific to state.

use crate::EthApi;
use alloy_primitives::B256;
use reth_rpc_convert::RpcConvert;
use reth_rpc_eth_api::{
    helpers::{EthState, LoadPendingBlock, LoadState, SpawnBlocking},
    FromEthApiError, RpcNodeCore,
};
use reth_rpc_eth_types::EthApiError;
use reth_storage_api::{StateProviderBox, StateProviderFactory};

impl<N, Rpc> EthState for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert<Primitives = N::Primitives, Error = EthApiError>,
    Self: LoadPendingBlock,
{
    fn max_proof_window(&self) -> u64 {
        self.inner.eth_proof_window()
    }
}

impl<N, Rpc> LoadState for EthApi<N, Rpc>
where
    N: RpcNodeCore,
    Rpc: RpcConvert<Primitives = N::Primitives>,
    Self: LoadPendingBlock,
{
    fn state_at_hash(&self, block_hash: B256) -> Result<StateProviderBox, Self::Error> {
        self.provider()
            .history_by_block_hash(block_hash)
            .map(|state| self.inner.wrap_state(state))
            .map_err(Self::Error::from_eth_err)
    }

    #[allow(clippy::manual_async_fn)]
    fn state_at_block_id(
        &self,
        at: alloy_rpc_types_eth::BlockId,
    ) -> impl Future<Output = Result<StateProviderBox, Self::Error>> + Send
    where
        Self: SpawnBlocking,
    {
        async move {
            if at.is_pending() &&
                let Ok(Some(state)) = self.local_pending_state().await
            {
                return Ok(self.inner.wrap_state(state))
            }

            self.provider()
                .state_by_block_id(at)
                .map(|state| self.inner.wrap_state(state))
                .map_err(Self::Error::from_eth_err)
        }
    }

    fn latest_state(&self) -> Result<StateProviderBox, Self::Error> {
        self.provider()
            .latest()
            .map(|state| self.inner.wrap_state(state))
            .map_err(Self::Error::from_eth_err)
    }
}

#[cfg(test)]
mod tests {
    use crate::eth::helpers::types::EthRpcConverter;

    use super::*;
    use alloy_primitives::{
        map::{AddressMap, B256Map},
        Address, StorageKey, StorageValue, U256,
    };
    use alloy_rpc_types_eth::BlockId;
    use reth_chainspec::ChainSpec;
    use reth_evm_ethereum::EthEvmConfig;
    use reth_network_api::noop::NoopNetwork;
    use reth_provider::{
        test_utils::{ExtendedAccount, MockEthProvider, NoopProvider},
        ChainSpecProvider,
    };
    use reth_rpc_eth_api::{helpers::EthState, node::RpcNodeCoreAdapter};
    use reth_transaction_pool::test_utils::{testing_pool, TestPool};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    fn noop_eth_api() -> EthApi<
        RpcNodeCoreAdapter<NoopProvider, TestPool, NoopNetwork, EthEvmConfig>,
        EthRpcConverter<ChainSpec>,
    > {
        let provider = NoopProvider::default();
        let pool = testing_pool();
        let evm_config = EthEvmConfig::mainnet();

        EthApi::builder(provider, pool, NoopNetwork::default(), evm_config).build()
    }

    fn mock_eth_api(
        accounts: AddressMap<ExtendedAccount>,
    ) -> EthApi<
        RpcNodeCoreAdapter<MockEthProvider, TestPool, NoopNetwork, EthEvmConfig>,
        EthRpcConverter<ChainSpec>,
    > {
        let pool = testing_pool();
        let mock_provider = MockEthProvider::default();

        let evm_config = EthEvmConfig::new(mock_provider.chain_spec());
        mock_provider.extend_accounts(accounts);

        EthApi::builder(mock_provider, pool, NoopNetwork::default(), evm_config).build()
    }

    #[tokio::test]
    async fn test_storage() {
        // === Noop ===
        let eth_api = noop_eth_api();
        let address = Address::random();
        let storage = eth_api.storage_at(address, U256::ZERO.into(), None).await.unwrap();
        assert_eq!(storage, U256::ZERO.to_be_bytes());

        // === Mock ===
        let storage_value = StorageValue::from(1337);
        let storage_key = StorageKey::random();
        let storage: B256Map<_> = core::iter::once((storage_key, storage_value)).collect();

        let accounts = AddressMap::from_iter([(
            address,
            ExtendedAccount::new(0, U256::ZERO).extend_storage(storage),
        )]);
        let eth_api = mock_eth_api(accounts);

        let storage_key: U256 = storage_key.into();
        let storage = eth_api.storage_at(address, storage_key.into(), None).await.unwrap();
        assert_eq!(storage, storage_value.to_be_bytes());
    }

    #[tokio::test]
    async fn test_get_account_missing() {
        let eth_api = noop_eth_api();
        let address = Address::random();
        let account = eth_api.get_account(address, Default::default()).await.unwrap();
        assert!(account.is_none());
    }

    #[tokio::test]
    async fn test_interceptor_wraps_latest_state() {
        let calls = Arc::new(AtomicUsize::new(0));
        let interceptor_calls = calls.clone();

        let eth_api = EthApi::builder(
            NoopProvider::default(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::mainnet(),
        )
        .interceptor(move |state| {
            interceptor_calls.fetch_add(1, Ordering::SeqCst);
            state
        })
        .build();

        let _ = eth_api.latest_state().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_interceptor_wraps_state_at_block_id() {
        let calls = Arc::new(AtomicUsize::new(0));
        let interceptor_calls = calls.clone();

        let eth_api = EthApi::builder(
            NoopProvider::default(),
            testing_pool(),
            NoopNetwork::default(),
            EthEvmConfig::mainnet(),
        )
        .interceptor(move |state| {
            interceptor_calls.fetch_add(1, Ordering::SeqCst);
            state
        })
        .build();

        let _ = eth_api.state_at_block_id(BlockId::latest()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
