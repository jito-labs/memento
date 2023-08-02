use crate::{config::BackfillConfig, metrics::BackfillMetrics};
use jito_net::slack::SlackClient;
use jito_protos::geyser::{
    account::Msg, geyser_service_client::GeyserServiceClient,
    replay_service_client::ReplayServiceClient, ReplayAccount, ReplayRequest, ReplayResponse,
    SnapshotAccount,
};
use jito_replayer_db::{
    account::{AccountModel, AccountsTable},
    progress::ProgressTable,
};
use jito_workers::helpers::get_snapshots;
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashSet,
    ops::Mul,
    string::FromUtf8Error,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    io,
    sync::{mpsc::UnboundedReceiver, Mutex},
    time::sleep,
};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tonic::{transport::Channel, Code};
use tracing::{instrument, warn};

#[derive(Debug, Error)]
pub enum BackfillError {
    #[error("GrpcConnection: {0}")]
    GrpcConnection(#[from] tonic::transport::Error),

    #[error("Grpc: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("Io: {0}")]
    Io(#[from] io::Error),

    #[error("SerdeYaml: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),

    #[error("Utf8: {0}")]
    Utf8(#[from] FromUtf8Error),

    #[error("BackfillStopped: {0}")]
    BackfillStopped(String),

    #[error("ClickhouseError: {0}")]
    ClickhouseError(#[from] clickhouse_rs::errors::Error),
}

#[instrument(
    skip(
        slot_receiver,
        progress_table,
        accounts_table,
        slack_client,
        accounts,
        service_addr
    ),
    err
)]
pub async fn stream_geyser_to_postgres(
    slot_receiver: Arc<Mutex<UnboundedReceiver<u64>>>,
    progress_table: ProgressTable,
    accounts_table: AccountsTable,
    slack_client: SlackClient,
    accounts: HashSet<Pubkey>,
    service_addr: String,
    thread_id: usize,
) -> Result<(), BackfillError> {
    let mut replay_client = ReplayServiceClient::connect(service_addr).await?;

    loop {
        let slot = slot_receiver.lock().await.recv().await;
        if slot.is_none() {
            slack_client
                .post_slack_message(&format!("backfill finished thread_id: {}", thread_id))
                .await;
            return Ok(());
        }
        let slot = slot.unwrap();

        match start_slot_replay(
            &mut replay_client,
            slot,
            &slack_client,
            &accounts_table,
            &accounts,
            thread_id,
        )
        .await
        {
            Ok(_) => {
                if let Err(e) = progress_table.insert_completed_slot(slot).await {
                    slack_client
                        .post_slack_message(&format!(
                            "backfill error marking completed thread_id: {} slot: {} error: {:?}",
                            thread_id, slot, e
                        ))
                        .await;
                }
            }
            Err(e) => {
                slack_client
                    .post_slack_message(&format!(
                        "backfill error thread_id: {} slot: {} error: {:?}",
                        thread_id, slot, e
                    ))
                    .await;
            }
        }
    }
}

#[instrument(skip(replay_client, slack_client, accounts_table, programs))]
async fn start_slot_replay(
    replay_client: &mut ReplayServiceClient<Channel>,
    snapshot_slot: u64,
    slack_client: &SlackClient,
    accounts_table: &AccountsTable,
    programs: &HashSet<Pubkey>,
    thread_id: usize,
) -> Result<(), BackfillError> {
    const BATCH_SIZE: usize = 1_000_000;

    let replay_response = try_start_replay(replay_client, snapshot_slot).await;

    // give the geyser service time to startup
    sleep(Duration::from_secs(1)).await;

    let mut geyser_client =
        GeyserServiceClient::connect(format!("http://{}", replay_response.addr)).await?;
    let mut stream = geyser_client.subscribe_accounts(()).await?.into_inner();

    slack_client
        .post_slack_message(&format!(
            "backfill starting thread_id: {} slot: {}",
            thread_id, snapshot_slot
        ))
        .await;

    let start = Instant::now();
    let mut metrics = BackfillMetrics::new(thread_id, snapshot_slot, slack_client.clone());
    let mut buffered_batch = Vec::with_capacity(BATCH_SIZE);

    loop {
        match stream.message().await {
            Ok(Some(message)) => {
                metrics.increment_num_messages_received(1);

                match message.msg {
                    None => {}
                    Some(Msg::Snap(SnapshotAccount {
                        slot,
                        pubkey,
                        lamports,
                        owner,
                        is_executable,
                        rent_epoch,
                        data,
                        seq,
                        is_startup: _,
                        tx_signature: _,
                    })) => {
                        let owner_pubkey = Pubkey::try_from(owner.as_slice()).unwrap();
                        let is_poi = programs.contains(&owner_pubkey);
                        if is_poi {
                            metrics.increment_num_messages_buffered(1);

                            // note: we save the snapshot_slot here as a separate field because during snapshot loading
                            // it will send the slot the account was last modified, not the slot of the snapshot you're loading from.
                            buffered_batch.push(AccountModel {
                                slot: slot as i64,
                                snapshot_slot: snapshot_slot as i64,
                                pubkey: Pubkey::try_from(pubkey.as_slice()).unwrap().to_string(),
                                lamports: lamports as i64,
                                owner: Pubkey::try_from(owner.as_slice()).unwrap().to_string(),
                                is_executable,
                                rent_epoch: rent_epoch as i64,
                                data,
                                seq: seq as i64,
                            });

                            if buffered_batch.len() == buffered_batch.capacity() {
                                let retry_strategy =
                                    ExponentialBackoff::from_millis(1_000).map(jitter).take(5);
                                Retry::spawn(retry_strategy, || {
                                    accounts_table.insert_accounts(&buffered_batch)
                                })
                                .await?;
                                buffered_batch.clear();
                                metrics.maybe_report().await;
                            }
                        }
                    }
                    Some(Msg::Replay(ReplayAccount {})) => {
                        // the worker only supports snapshot loading, so it sends an empty replay account to signal when its done
                        break;
                    }
                }
            }
            Ok(None) => {
                return Err(BackfillError::BackfillStopped(format!(
                    "backfill stream closed unexpectedly thread_id: {} slot: {}",
                    thread_id, snapshot_slot
                )));
            }
            Err(e) => {
                return Err(BackfillError::BackfillStopped(format!(
                    "backfill stream closed unexpectedly thread_id: {} slot: {} error: {:?}",
                    thread_id, snapshot_slot, e
                )));
            }
        }
    }

    accounts_table.insert_accounts(&buffered_batch).await?;
    buffered_batch.clear();
    metrics.maybe_report().await;

    slack_client
        .post_slack_message(&format!(
            "backfill completed thread_id: {} slot: {} elapsed: {:?}",
            thread_id,
            snapshot_slot,
            start.elapsed()
        ))
        .await;

    Ok(())
}

/// There are a max number of workspaces (jobs) that the worker can run.
/// This will continually retry to start a job until it suceeeds.
#[instrument(skip(replay_client))]
async fn try_start_replay(
    replay_client: &mut ReplayServiceClient<Channel>,
    slot: u64,
) -> ReplayResponse {
    let mut backoff_secs = 1;
    loop {
        match replay_client.start_replay(ReplayRequest { slot }).await {
            Ok(response) => return response.into_inner(),
            Err(e) => {
                if e.code() != Code::ResourceExhausted {
                    warn!("error waiting to start replay: {:?}", e);
                }
                sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = backoff_secs.mul(2).clamp(0, 120);
            }
        }
    }
}

#[instrument(skip_all)]
pub async fn find_slots_to_backfill(
    replay_client: &mut ReplayServiceClient<Channel>,
    backfill_config: &BackfillConfig,
    progress_table: &ProgressTable,
) -> Vec<u64> {
    let already_completed_slots: Vec<_> = progress_table.get_completed_slots().await.unwrap();
    let snapshot_slots_available: Vec<_> = get_snapshots(replay_client)
        .await
        .unwrap()
        .into_iter()
        .map(|snap| snap.slot)
        .collect();
    backfill_config
        .slots
        .iter()
        .filter(|slot| snapshot_slots_available.contains(slot))
        .filter(|slot| !already_completed_slots.contains(slot))
        .copied()
        .collect()
}
