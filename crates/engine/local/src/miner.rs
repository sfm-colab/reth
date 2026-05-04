//! Contains the implementation of the mining mode for the local engine.

use alloy_primitives::{TxHash, B256};
use alloy_rpc_types_engine::{ForkchoiceState, ForkchoiceUpdated, PayloadStatus};
use futures_util::{stream::Fuse, Stream, StreamExt};
use reth_engine_primitives::{
    BeaconForkChoiceUpdateError, BeaconOnNewPayloadError, ConsensusEngineHandle,
};
use reth_payload_builder::PayloadBuilderHandle;
use reth_payload_primitives::{
    BuiltPayload, PayloadAttributesBuilder, PayloadBuilderError, PayloadKind, PayloadTypes,
};
use reth_primitives_traits::{HeaderTy, SealedHeader, SealedHeaderFor};
use reth_storage_api::BlockReader;
use reth_transaction_pool::TransactionPool;
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Interval,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::error;

/// Handle used to control a running [`LocalMiner`].
pub struct LocalMinerHandle<H> {
    tx: mpsc::UnboundedSender<LocalMinerCommand<H>>,
}

impl<H> Clone for LocalMinerHandle<H> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl<H> fmt::Debug for LocalMinerHandle<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMinerHandle").finish_non_exhaustive()
    }
}

impl<H> LocalMinerHandle<H> {
    /// Creates a miner handle and the matching control receiver.
    pub fn new() -> (Self, LocalMinerControl<H>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, LocalMinerControl { rx })
    }

    /// Resets the miner's local forkchoice head.
    ///
    /// This is intended for local-dev flows that externally rewind the canonical chain, such as
    /// Anvil snapshot reverts. The returned future resolves after the running miner has applied
    /// the new head and sent a forkchoice update to the engine.
    pub async fn reset_head(&self, header: SealedHeader<H>) -> Result<(), LocalMinerError> {
        let (response, result) = oneshot::channel();
        self.tx
            .send(LocalMinerCommand::ResetHead { header, response })
            .map_err(|_| LocalMinerError::TaskUnavailable)?;

        result.await.map_err(|_| LocalMinerError::ResponseDropped("reset"))?
    }

    /// Mines one block immediately and updates forkchoice to the mined block.
    ///
    /// This bypasses the configured mining mode trigger and waits until the local miner has built
    /// and canonicalized the new block, returning its sealed header.
    pub async fn mine_one(&self) -> Result<SealedHeader<H>, LocalMinerError> {
        let (response, result) = oneshot::channel();
        self.tx
            .send(LocalMinerCommand::MineOne { response })
            .map_err(|_| LocalMinerError::TaskUnavailable)?;

        result.await.map_err(|_| LocalMinerError::ResponseDropped("mine"))?
    }
}

/// Errors returned when controlling a running [`LocalMiner`].
#[derive(Debug, thiserror::Error)]
pub enum LocalMinerError {
    /// The local miner task is no longer running.
    #[error("local miner task is not running")]
    TaskUnavailable,
    /// The local miner task dropped the request response channel.
    #[error("local miner task dropped the {0} response")]
    ResponseDropped(&'static str),
    /// The engine rejected or failed a forkchoice update.
    #[error(transparent)]
    ForkchoiceUpdate(#[from] BeaconForkChoiceUpdateError),
    /// The engine rejected or failed a new payload.
    #[error(transparent)]
    NewPayload(#[from] BeaconOnNewPayloadError),
    /// The payload builder failed to produce a payload.
    #[error(transparent)]
    PayloadBuilder(#[from] PayloadBuilderError),
    /// The forkchoice update response was not valid.
    #[error("invalid forkchoice update {state:?}: {response:?}")]
    InvalidForkchoiceUpdate {
        /// Requested forkchoice state.
        state: ForkchoiceState,
        /// Engine response.
        response: ForkchoiceUpdated,
    },
    /// The forkchoice update response did not include a payload id.
    #[error("missing payload id in forkchoice update response")]
    MissingPayloadId,
    /// The new payload response was not valid.
    #[error("invalid payload: {0:?}")]
    InvalidPayload(PayloadStatus),
}

/// Receiver side for [`LocalMinerHandle`].
pub struct LocalMinerControl<H> {
    rx: mpsc::UnboundedReceiver<LocalMinerCommand<H>>,
}

impl<H> fmt::Debug for LocalMinerControl<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMinerControl").finish_non_exhaustive()
    }
}

enum LocalMinerCommand<H> {
    ResetHead { header: SealedHeader<H>, response: oneshot::Sender<Result<(), LocalMinerError>> },
    MineOne { response: oneshot::Sender<Result<SealedHeader<H>, LocalMinerError>> },
}

/// A mining mode for the local dev engine.
pub enum MiningMode<Pool: TransactionPool + Unpin> {
    /// In this mode a block is built as soon as
    /// a valid transaction reaches the pool.
    /// If `max_transactions` is set, a block is built when that many transactions have
    /// accumulated.
    Instant {
        /// The transaction pool.
        pool: Pool,
        /// Stream of transaction notifications.
        rx: Fuse<ReceiverStream<TxHash>>,
        /// Maximum number of transactions to accumulate before mining a block.
        /// If None, mine immediately when any transaction arrives.
        max_transactions: Option<usize>,
        /// Counter for accumulated transactions (only used when `max_transactions` is set).
        accumulated: usize,
    },
    /// In this mode a block is built at a fixed interval.
    Interval(Interval),
    /// In this mode a block is built when the trigger stream yields a value.
    ///
    /// This is a general-purpose trigger that can be fired on demand, for example via a channel
    /// or any other [`Stream`] implementation.
    Trigger(Pin<Box<dyn Stream<Item = ()> + Send + Sync>>),
}

impl<Pool: TransactionPool + Unpin> fmt::Debug for MiningMode<Pool> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Instant { max_transactions, accumulated, .. } => f
                .debug_struct("Instant")
                .field("max_transactions", max_transactions)
                .field("accumulated", accumulated)
                .finish(),
            Self::Interval(interval) => f.debug_tuple("Interval").field(interval).finish(),
            Self::Trigger(_) => f.debug_tuple("Trigger").finish(),
        }
    }
}

impl<Pool: TransactionPool + Unpin> MiningMode<Pool> {
    /// Constructor for a [`MiningMode::Instant`]
    pub fn instant(pool: Pool, max_transactions: Option<usize>) -> Self {
        let rx = pool.pending_transactions_listener();
        Self::Instant { pool, rx: ReceiverStream::new(rx).fuse(), max_transactions, accumulated: 0 }
    }

    /// Constructor for a [`MiningMode::Interval`]
    pub fn interval(duration: Duration) -> Self {
        let start = tokio::time::Instant::now() + duration;
        Self::Interval(tokio::time::interval_at(start, duration))
    }

    /// Constructor for a [`MiningMode::Trigger`]
    ///
    /// Accepts any stream that yields `()` values, each of which triggers a new block to be
    /// mined. This can be backed by a channel, a custom stream, or any other async source.
    pub fn trigger(trigger: impl Stream<Item = ()> + Send + Sync + 'static) -> Self {
        Self::Trigger(Box::pin(trigger))
    }
}

impl<Pool: TransactionPool + Unpin> Future for MiningMode<Pool> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this {
            Self::Instant { pool, rx, max_transactions, accumulated } => {
                // Poll for new transaction notifications
                while let Poll::Ready(Some(_)) = rx.poll_next_unpin(cx) {
                    if pool.pending_and_queued_txn_count().0 == 0 {
                        continue;
                    }
                    if let Some(max_tx) = max_transactions {
                        *accumulated += 1;
                        // If we've reached the max transactions threshold, mine a block
                        if *accumulated >= *max_tx {
                            *accumulated = 0; // Reset counter for next block
                            return Poll::Ready(());
                        }
                    } else {
                        // If no max_transactions is set, mine immediately
                        return Poll::Ready(());
                    }
                }
                Poll::Pending
            }
            Self::Interval(interval) => {
                if interval.poll_tick(cx).is_ready() {
                    return Poll::Ready(())
                }
                Poll::Pending
            }
            Self::Trigger(trigger) => {
                if trigger.poll_next_unpin(cx).is_ready() {
                    return Poll::Ready(())
                }
                Poll::Pending
            }
        }
    }
}

/// Local miner advancing the chain
#[derive(Debug)]
pub struct LocalMiner<T: PayloadTypes, B, Pool: TransactionPool + Unpin> {
    /// The payload attribute builder for the engine
    payload_attributes_builder: B,
    /// Sender for events to engine.
    to_engine: ConsensusEngineHandle<T>,
    /// The mining mode for the engine
    mode: MiningMode<Pool>,
    /// The payload builder for the engine
    payload_builder: PayloadBuilderHandle<T>,
    /// Latest block in the chain so far.
    last_header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    /// Stores latest mined blocks.
    last_block_hashes: VecDeque<B256>,
    /// Optional sleep duration between initiating payload building and resolving.
    ///
    /// When set, the miner sleeps after `fork_choice_updated` before calling
    /// `resolve_kind`, giving the payload job time for multiple rebuild attempts.
    payload_wait_time: Option<Duration>,
    /// Optional local control channel.
    control: Option<LocalMinerControl<HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>>,
}

impl<T, B, Pool> LocalMiner<T, B, Pool>
where
    T: PayloadTypes,
    B: PayloadAttributesBuilder<
        T::PayloadAttributes,
        HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>,
    >,
    Pool: TransactionPool + Unpin,
{
    /// Spawns a new [`LocalMiner`] with the given parameters.
    pub fn new(
        provider: impl BlockReader<Header = HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>,
        payload_attributes_builder: B,
        to_engine: ConsensusEngineHandle<T>,
        mode: MiningMode<Pool>,
        payload_builder: PayloadBuilderHandle<T>,
    ) -> Self {
        let last_header =
            provider.sealed_header(provider.best_block_number().unwrap()).unwrap().unwrap();

        Self {
            payload_attributes_builder,
            to_engine,
            mode,
            payload_builder,
            last_block_hashes: VecDeque::from([last_header.hash()]),
            last_header,
            payload_wait_time: None,
            control: None,
        }
    }

    /// Sets the payload wait time, if any.
    pub const fn with_payload_wait_time_opt(mut self, wait_time: Option<Duration>) -> Self {
        self.payload_wait_time = wait_time;
        self
    }

    /// Sets a control receiver for this local miner.
    pub fn with_control(
        mut self,
        control: LocalMinerControl<HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>,
    ) -> Self {
        self.control = Some(control);
        self
    }

    /// Runs the [`LocalMiner`] in a loop, polling the miner and building payloads.
    pub async fn run(mut self) {
        let mut fcu_interval = tokio::time::interval(Duration::from_secs(1));
        let mut control = self.control.take();

        loop {
            if let Some(control_rx) = control.as_mut() {
                tokio::select! {
                    command = control_rx.rx.recv() => {
                        if let Some(command) = command {
                            self.handle_control_command(command).await;
                        } else {
                            control = None;
                        }
                    }
                    // Wait for the interval or the pool to receive a transaction
                    _ = &mut self.mode => {
                        if let Err(e) = self.advance().await {
                            error!(target: "engine::local", "Error advancing the chain: {:?}", e);
                        }
                    }
                    // send FCU once in a while
                    _ = fcu_interval.tick() => {
                        if let Err(e) = self.update_forkchoice_state().await {
                            error!(target: "engine::local", "Error updating fork choice: {:?}", e);
                        }
                    }
                }
            } else {
                tokio::select! {
                    // Wait for the interval or the pool to receive a transaction
                    _ = &mut self.mode => {
                        if let Err(e) = self.advance().await {
                            error!(target: "engine::local", "Error advancing the chain: {:?}", e);
                        }
                    }
                    // send FCU once in a while
                    _ = fcu_interval.tick() => {
                        if let Err(e) = self.update_forkchoice_state().await {
                            error!(target: "engine::local", "Error updating fork choice: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    async fn handle_control_command(
        &mut self,
        command: LocalMinerCommand<HeaderTy<<T::BuiltPayload as BuiltPayload>::Primitives>>,
    ) {
        match command {
            LocalMinerCommand::ResetHead { header, response } => {
                let result = self.reset_head(header).await;
                let _ = response.send(result);
            }
            LocalMinerCommand::MineOne { response } => {
                let result = self.mine_one().await;
                let _ = response.send(result);
            }
        }
    }

    async fn reset_head(
        &mut self,
        header: SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>,
    ) -> Result<(), LocalMinerError> {
        self.last_block_hashes = VecDeque::from([header.hash()]);
        self.last_header = header;
        self.update_forkchoice_state().await
    }

    async fn mine_one(
        &mut self,
    ) -> Result<SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>, LocalMinerError>
    {
        let header = self.advance().await?;
        self.update_forkchoice_state().await?;
        Ok(header)
    }

    /// Returns current forkchoice state.
    fn forkchoice_state(&self) -> ForkchoiceState {
        ForkchoiceState {
            head_block_hash: *self.last_block_hashes.back().expect("at least 1 block exists"),
            safe_block_hash: *self
                .last_block_hashes
                .get(self.last_block_hashes.len().saturating_sub(32))
                .expect("at least 1 block exists"),
            finalized_block_hash: *self
                .last_block_hashes
                .get(self.last_block_hashes.len().saturating_sub(64))
                .expect("at least 1 block exists"),
        }
    }

    /// Sends a FCU to the engine.
    async fn update_forkchoice_state(&self) -> Result<(), LocalMinerError> {
        let state = self.forkchoice_state();
        let res = self.to_engine.fork_choice_updated(state, None).await?;

        if !res.is_valid() {
            return Err(LocalMinerError::InvalidForkchoiceUpdate { state, response: res })
        }

        Ok(())
    }

    /// Generates payload attributes for a new block, passes them to FCU and inserts built payload
    /// through newPayload.
    async fn advance(
        &mut self,
    ) -> Result<SealedHeaderFor<<T::BuiltPayload as BuiltPayload>::Primitives>, LocalMinerError>
    {
        let state = self.forkchoice_state();
        let res = self
            .to_engine
            .fork_choice_updated(
                state,
                Some(self.payload_attributes_builder.build(&self.last_header)),
            )
            .await?;

        if !res.is_valid() {
            return Err(LocalMinerError::InvalidForkchoiceUpdate { state, response: res })
        }

        let payload_id = res.payload_id.ok_or(LocalMinerError::MissingPayloadId)?;

        if let Some(wait_time) = self.payload_wait_time {
            tokio::time::sleep(wait_time).await;
        }

        let payload = match self
            .payload_builder
            .resolve_kind(payload_id, PayloadKind::WaitForPending)
            .await
        {
            Some(Ok(payload)) => payload,
            Some(Err(error)) => return Err(error.into()),
            None => return Err(PayloadBuilderError::MissingPayload.into()),
        };

        let header = payload.block().sealed_header().clone();
        let payload = T::block_to_payload(payload.block().clone());
        let res = self.to_engine.new_payload(payload).await?;

        if !res.is_valid() {
            return Err(LocalMinerError::InvalidPayload(res))
        }

        self.last_block_hashes.push_back(header.hash());
        self.last_header = header.clone();
        // ensure we keep at most 64 blocks
        if self.last_block_hashes.len() > 64 {
            self.last_block_hashes.pop_front();
        }

        Ok(header)
    }
}
