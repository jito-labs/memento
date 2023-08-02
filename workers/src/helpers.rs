use jito_protos::geyser::{replay_service_client::ReplayServiceClient, Snapshot};
use tonic::{transport::Channel, Status};
use tracing::instrument;

#[instrument]
pub async fn get_snapshots(
    replay_client: &mut ReplayServiceClient<Channel>,
) -> Result<Vec<Snapshot>, Status> {
    let mut stream = replay_client.get_snapshots(()).await?.into_inner();
    let mut snapshots = Vec::new();

    while let Some(snapshot) = stream.message().await? {
        snapshots.push(snapshot);
    }
    snapshots.sort_by_key(|s| s.slot);
    Ok(snapshots)
}
