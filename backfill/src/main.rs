mod backfill;
mod config;
mod metrics;

use crate::{
    backfill::{find_slots_to_backfill, stream_geyser_to_postgres},
    config::BackfillConfig,
};
use clap::Parser;
use futures_util::future::join_all;
use jito_net::slack::SlackClient;
use jito_protos::geyser::replay_service_client::ReplayServiceClient;
use jito_replayer_db::{account::AccountsTable, progress::ProgressTable};
use solana_sdk::pubkey::Pubkey;
use std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{mpsc::unbounded_channel, Mutex};
use tracing::{info, instrument};

#[derive(Parser, Debug)]
struct Args {
    /// Geyser address to connect to
    #[arg(long, env, default_value_t = String::from("http://0.0.0.0:7892"))]
    service_addr: String,

    /// Path to backfill configuration
    #[arg(long, env)]
    config_file_path: PathBuf,

    /// Clickhouse URL
    #[arg(long, env)]
    clickhouse_url: String,

    /// Slack webhook URL to post updates to channel on
    #[arg(long, env)]
    slack_url: Option<String>,
}

#[tokio::main]
#[instrument]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Args = Args::parse();
    info!("starting with args: {:?}", args);

    let backfill_config = BackfillConfig::read_backfill_config(&args.config_file_path)
        .await
        .expect("read config");

    let pool = clickhouse_rs::Pool::new(args.clickhouse_url);
    let slack_client = SlackClient::new(args.slack_url, Duration::from_secs(1));

    let accounts: HashSet<Pubkey> = backfill_config.programs.iter().copied().collect();

    let mut replay_client = ReplayServiceClient::connect(args.service_addr.clone())
        .await
        .unwrap();

    let progress_table = ProgressTable::new(pool.clone());
    let accounts_table = AccountsTable::new(pool.clone());

    progress_table.migrate().await.unwrap();
    accounts_table.migrate().await.unwrap();

    let mut backfill_slots =
        find_slots_to_backfill(&mut replay_client, &backfill_config, &progress_table).await;
    backfill_slots.sort_by_key(|slot| *slot);

    let (slot_sender, slot_receiver) = unbounded_channel();
    let slot_receiver = Arc::new(Mutex::new(slot_receiver));

    // start with most recent and work backwards
    for s in backfill_slots.into_iter().rev() {
        slot_sender.send(s).unwrap();
    }
    drop(slot_sender);

    let futs = (0..backfill_config.max_workers).map(|i| {
        let slot_receiver = slot_receiver.clone();
        let progress_table = progress_table.clone();
        let accounts_table = accounts_table.clone();

        let slack_client = slack_client.clone();
        let service_addr = args.service_addr.clone();
        let accounts = accounts.clone();

        tokio::spawn(stream_geyser_to_postgres(
            slot_receiver,
            progress_table,
            accounts_table,
            slack_client,
            accounts,
            service_addr,
            i,
        ))
    });
    let results = join_all(futs).await;
    info!("backfill results: {:?}", results);
}
