use cloud_storage::Object;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr;
use thiserror::Error;
use tracing::{debug, info, instrument, warn};

#[derive(Debug, Error)]
enum ErrorFoo {
    #[error("message")]
    GcloudError(#[from] reqwest::Error),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ObjectResponse {
    kind: String,
    next_page_token: Option<String>,
    items: Vec<Object>,
}

#[derive(Debug, Default)]
pub struct Snapshot {
    name: String,
    url: String,
    size: u64,
    slot: u64,
    hash: String,
}

#[derive(Debug, Default)]
pub struct Bounds {
    name: String,
    url: String,
    size: u64,
    start_slot: u64,
    stop_slot: u64,
}

#[derive(Debug, Default)]
pub struct LedgerSnapshot {
    name: String,
    epoch: u64,
    url: String,
    size: u64,
    bounds: Bounds,
}

#[instrument]
async fn list_objects(
    bucket_name: &str,
    next_page_token: &str,
) -> Result<ObjectResponse, reqwest::Error> {
    let mut url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o",
        bucket_name,
    );
    if !next_page_token.is_empty() {
        url.extend(format!("/?pageToken={}", next_page_token).chars());
    }
    info!("url={:?}", url);
    let response = reqwest::get(url).await?;
    response.json().await
}

#[instrument]
async fn get_all_objects(bucket_name: &str) -> Result<Vec<ObjectResponse>, reqwest::Error> {
    let mut next_page_token = "".to_string();
    let mut all_objects = vec![];

    loop {
        let objects = list_objects(bucket_name, &next_page_token).await?;

        let done = true; //objects.next_page_token.is_none();
        next_page_token = objects.next_page_token.clone().unwrap_or_default();
        info!(
            "done: {}, items: {} next_page_token: {}",
            done,
            objects.items.len(),
            next_page_token
        );
        all_objects.push(objects);

        if done {
            break;
        }
    }
    Ok(all_objects)
}

fn search_objects_by_filename<'a>(
    objects: &'a [Object],
    re: &'a Regex,
) -> Vec<(&'a Object, regex::Captures<'a>)> {
    objects
        .iter()
        .filter_map(|object| {
            let filename = &object.name;

            match re.captures(filename) {
                Some(caps) => Some((object, caps)),
                None => None,
            }
        })
        .collect()
}

fn get_snapshots(objects: &[Object]) -> Vec<Snapshot> {
    let snapshot_regex = Regex::new(r"snapshot-(\d+)-(\w+)\.tar\.(zst|bz2)").unwrap();
    search_objects_by_filename(&objects, &snapshot_regex)
        .into_iter()
        .map(|(s, captures)| {
            let slot = captures.get(1).unwrap().as_str().parse().unwrap();
            let hash = captures.get(2).unwrap().as_str().to_string();
            let file_type = captures.get(3).unwrap().as_str();

            Snapshot {
                name: s.name.clone(),
                url: s.media_link.clone(),
                size: s.size,
                slot,
                hash,
            }
        })
        .collect()
}

fn get_ledger_snapshots(objects: &[Object]) -> Vec<LedgerSnapshot> {
    let rocksdb_regex = Regex::new(r"(\d+)/rocksdb\.tar\.bz2").unwrap();
    let bounds_regex = Regex::new(r"(\d+)/bounds\.txt").unwrap();

    // need rocksdb for everything
    let mut snapshots: HashMap<u64, LedgerSnapshot> =
        search_objects_by_filename(&objects, &rocksdb_regex)
            .into_iter()
            .map(|(s, captures)| {
                let epoch: u64 = captures.get(1).unwrap().as_str().parse().unwrap();
                (
                    epoch,
                    LedgerSnapshot {
                        name: s.name.clone(),
                        epoch,
                        url: s.media_link.clone(),
                        size: s.size,
                        bounds: Bounds::default(),
                    },
                )
            })
            .collect();

    search_objects_by_filename(&objects, &bounds_regex)
        .into_iter()
        .for_each(|(s, captures)| {
            let epoch: u64 = captures.get(1).unwrap().as_str().parse().unwrap();
            if let Some(snapshot) = snapshots.get_mut(&epoch) {
                snapshot.bounds = Bounds {
                    name: s.name.to_string(),
                    url: s.media_link.to_string(),
                    size: s.size,
                    start_slot: 0,
                    stop_slot: 0,
                };
            }
        });

    snapshots.drain().map(|(_, s)| s).collect()
}

#[tokio::main]
async fn main() -> Result<(), ErrorFoo> {
    tracing_subscriber::fmt::init();

    let objects: Vec<_> = get_all_objects("mainnet-beta-ledger-us-ny5")
        .await?
        .into_iter()
        .flat_map(|o| o.items)
        .collect();

    let snapshots = get_snapshots(&objects);
    let ledger_snapshots = get_ledger_snapshots(&objects);

    for l in ledger_snapshots {
        println!("{:?}", l);
    }

    Ok(())
}
