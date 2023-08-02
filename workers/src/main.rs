use clap::Parser;
use cloud_storage::Object;
use futures_util::future::join_all;
use jito_gcs::bucket::{get_all_objects, get_snapshots};
use jito_net::slack::SlackClient;
use jito_protos::geyser::replay_service_server::ReplayServiceServer;
use jito_workers::replay_request::ReplayServiceImpl;
use std::{net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};
use tonic::transport::Server;
use tracing::{error, info};

#[derive(Parser, Debug)]
struct Args {
    /// Geyser address to bind to
    #[arg(long, env, default_value_t = SocketAddr::from_str("0.0.0.0:7892").unwrap())]
    geyser_addr: SocketAddr,

    /// Ledger path, should be very big. Main workdir.
    /// The snapshots are downloaded here.
    #[clap(long, env, default_value = "/tmp/ledger")]
    ledger_path: PathBuf,

    /// buckets to query which contain snapshots in the typical snapshot file naming scheme.
    /// Solana Labs has buckets you can ask for the link to
    #[clap(long, env, required = true)]
    buckets: Vec<String>,

    /// RPC url used to fetch genesis files from
    #[clap(long, env, default_value = "http://mainnet.rpc.jito.wtf")]
    rpc: String,

    /// Starting port for geyser servers to spawn.
    /// The server will start here and work its way up trying to find new port to bind to
    #[clap(long, env, default_value = "9009")]
    starting_geyser_port: u16,

    /// Maximum number of concurrent replays happening.
    /// A good rule of thumb is ~30GB of RAM per snapshot being loaded, however this may change over time.
    #[clap(long, env, default_value = "4")]
    max_workspaces: usize,

    /// Slack URL to post updates on
    #[clap(long, env)]
    slack_url: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Args = Args::parse();
    info!("starting with args: {:?}", args);

    let slack_client = SlackClient::new(args.slack_url, Duration::from_secs(1));

    info!("reading from buckets: {:?}", args.buckets);
    let get_object_tasks = args
        .buckets
        .iter()
        .map(|bucket| get_all_objects(bucket, Some("**/snapshot-*")));
    let objects: Vec<Object> = join_all(get_object_tasks)
        .await
        .into_iter()
        .filter_map(|o| match o {
            Ok(objects) => Some(objects),
            Err(e) => {
                error!("error getting objects: {:?}", e);
                None
            }
        })
        .flatten()
        .filter_map(|o| o.items)
        .flatten()
        .collect();

    let mut snapshots = get_snapshots(&objects);
    snapshots.sort_by_key(|s| s.slot);

    let addr = args.geyser_addr;
    Server::builder()
        .add_service(ReplayServiceServer::new(ReplayServiceImpl::new(
            args.ledger_path,
            snapshots,
            args.rpc,
            args.starting_geyser_port,
            args.max_workspaces,
            slack_client,
        )))
        .serve(addr)
        .await
        .expect("serves ok");
}
