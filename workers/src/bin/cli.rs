use chrono::{Days, NaiveDateTime, NaiveTime};
use clap::{Parser, Subcommand};
use futures_util::future::join_all;
use jito_protos::geyser::{
    geyser_service_client::GeyserServiceClient, replay_service_client::ReplayServiceClient,
    Account, GetReplayStateRequest, ReplayRequest,
};
use jito_workers::helpers::get_snapshots;
use solana_storage_bigtable::LedgerStorage;
use std::time::Duration;
use tonic::{transport::Channel, Streaming};
use tracing::{error, info, instrument};

#[derive(Clone, Debug, Subcommand)]
enum Mode {
    /// Start loading and streaming a snapshot
    StartSnapshotStream { slot: u64 },

    /// Gets all snapshots
    GetSnapshots,

    /// Returns the state of the replayer module
    GetState,

    /// Prints out information surrounding the snapshot times using an RPC server
    GetSnapshotTimes {
        /// minimum slot to return context on
        min_slot: Option<u64>,
    },
    /// Finds snapshots given a duration between snapshot
    FindDailySnapshots {
        /// minimum slot to return context on
        min_slot: Option<u64>,
        /// UTC time in military time (5pm -> 17)
        military_time_hour: u32,
    },
}

#[derive(Parser, Debug)]
struct Args {
    /// Geyser address to connect to
    #[arg(long, env, default_value_t = String::from("http://0.0.0.0:7892"))]
    service_addr: String,

    /// Mode to use
    #[command(subcommand)]
    subcommand: Mode,
}

#[tokio::main]
#[instrument]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Args = Args::parse();
    info!("starting with args: {:?}", args);

    let mut replay_client = ReplayServiceClient::connect(args.service_addr)
        .await
        .expect("connects to replay client");

    match args.subcommand {
        Mode::StartSnapshotStream { slot } => {
            command_start_or_subscribe(&mut replay_client, slot).await;
        }
        Mode::GetSnapshots => {
            command_get_snapshots(&mut replay_client).await;
        }
        Mode::GetState => {
            command_get_state(&mut replay_client).await;
        }
        Mode::GetSnapshotTimes { min_slot } => {
            command_get_snapshot_times(&mut replay_client, min_slot).await;
        }
        Mode::FindDailySnapshots {
            min_slot,
            military_time_hour,
        } => {
            command_find_daily_snapshots(&mut replay_client, min_slot, military_time_hour).await;
        }
    }
}

/// Expects GOOGLE_APPLICATION_CREDENTIALS env var to be set so it can read from bigtable
#[instrument]
async fn command_find_daily_snapshots(
    replay_client: &mut ReplayServiceClient<Channel>,
    min_slot: Option<u64>,
    military_time_hour: u32,
) {
    let snapshots = get_snapshots(replay_client).await.unwrap();

    let min_slot = min_slot.unwrap_or_default();
    let slots: Vec<_> = snapshots
        .iter()
        .map(|snap| snap.slot)
        .filter(|slot| *slot >= min_slot)
        .collect();
    info!(
        "fetching information on {} snapshots, this might take awhile...",
        slots.len()
    );

    let mut slots_times = find_block_times(&slots).await;
    slots_times.sort_by_key(|(_, block_time)| *block_time);

    let first_date_time = NaiveDateTime::new(
        slots_times.first().unwrap().1.date(),
        NaiveTime::from_hms_opt(military_time_hour, 0, 0).unwrap(),
    );
    let last_date_time = NaiveDateTime::new(
        slots_times.last().unwrap().1.date(),
        NaiveTime::from_hms_opt(military_time_hour, 0, 0).unwrap(),
    );
    let date_range: Vec<_> = (0..)
        .map(|days| first_date_time.checked_add_days(Days::new(days)).unwrap())
        .take_while(|&date| date <= last_date_time)
        .collect();

    for date in &date_range {
        let mut closest_block_time = None;
        let mut closest_duration = None;
        let mut closest_slot = None;

        for (slot, block_time) in &slots_times {
            let duration = date.signed_duration_since(*block_time).num_seconds().abs();

            // If the current block_time is closer than the previous closest one (or if there isn't a closest one yet)
            if closest_duration.is_none() || duration < closest_duration.unwrap() {
                closest_block_time = Some(block_time);
                closest_duration = Some(duration);
                closest_slot = Some(slot);
            }
        }

        let closest_block_time = closest_block_time.unwrap();
        let closest_duration = closest_duration.unwrap();

        // At this point, for the given date, closest_block_time is the block_time closest to that date.
        // You can now work with these values as needed.
        println!(
            "slot: {:?} date: {}, closest block time: {:?}, time diff: {:.2} minutes",
            closest_slot,
            date,
            closest_block_time,
            closest_duration as f32 / 60.0
        );
    }
}

#[instrument(skip_all)]
async fn command_get_snapshots(replay_client: &mut ReplayServiceClient<Channel>) {
    let snapshots = get_snapshots(replay_client).await.unwrap();
    for snapshot in snapshots {
        info!("slot: {}, file: {}", snapshot.slot, snapshot.filename);
    }
}

#[instrument(skip_all)]
async fn command_get_state(replay_client: &mut ReplayServiceClient<Channel>) {
    let states = replay_client
        .get_replay_state(GetReplayStateRequest {})
        .await
        .unwrap()
        .into_inner();
    for context in states.states {
        info!("replay_context: {:?}", context);
    }
}

#[instrument(skip_all)]
async fn print_geyser_stream(mut stream: Streaming<Account>) {
    info!("streaming accounts...");
    let mut count = 0;
    loop {
        match stream.message().await {
            Ok(Some(_account)) => {
                info!("num accounts: {:?}", count);
                count += 1;
            }
            Ok(None) => {
                info!("received none, exiting");
                return;
            }
            Err(e) => {
                error!("error receiving message: {:?}", e);
            }
        }
    }
}

#[instrument(skip(replay_client))]
async fn command_start_or_subscribe(replay_client: &mut ReplayServiceClient<Channel>, slot: u64) {
    match replay_client.start_replay(ReplayRequest { slot }).await {
        Ok(response) => {
            let addr = format!("http://{}", response.into_inner().addr);
            info!("subscribing to geyser client at {:?}", addr);
            let mut geyser_client = GeyserServiceClient::connect(addr).await.unwrap();
            let subscribe_response = geyser_client
                .subscribe_accounts(())
                .await
                .unwrap()
                .into_inner();

            print_geyser_stream(subscribe_response).await;
        }
        Err(status) => {
            error!("error starting replay: {:?}", status);
        }
    }
}

/// Expects GOOGLE_APPLICATION_CREDENTIALS env var to be set so it can read from bigtable
#[instrument(skip(replay_client))]
async fn command_get_snapshot_times(
    replay_client: &mut ReplayServiceClient<Channel>,
    min_slot: Option<u64>,
) {
    let snapshots = get_snapshots(replay_client).await.unwrap();

    let min_slot = min_slot.unwrap_or_default();
    let slots: Vec<_> = snapshots
        .iter()
        .map(|snap| snap.slot)
        .filter(|slot| *slot >= min_slot)
        .collect();
    info!(
        "fetching information on {} snapshots, this might take awhile...",
        slots.len()
    );
    let slots_times = find_block_times(&slots).await;
    for (slot, block_time) in slots_times {
        info!("slot: {:?}, time: {:?}", slot, block_time);
    }
}

#[instrument]
async fn find_block_times(slots: &[u64]) -> Vec<(u64, NaiveDateTime)> {
    let futs = slots.chunks(100).map(|chunk| {
        let chunk = chunk.to_vec();
        tokio::spawn(async move {
            let ledger_tool = LedgerStorage::new(true, Some(Duration::from_secs(120)), None)
                .await
                .expect("connects to bigtable");
            let slots_blocks = ledger_tool
                .get_confirmed_blocks_with_data(&chunk)
                .await
                .unwrap();
            slots_blocks
                .into_iter()
                .map(|(s, b)| {
                    let datetime =
                        NaiveDateTime::from_timestamp_opt(b.block_time.unwrap_or_default(), 0)
                            .unwrap();
                    (s, datetime)
                })
                .collect::<Vec<(u64, NaiveDateTime)>>()
        })
    });
    join_all(futs)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .flatten()
        .collect()
}
