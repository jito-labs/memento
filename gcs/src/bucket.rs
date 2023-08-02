use cloud_storage::Object;
use futures_util::future::join_all;
use jito_net::http::download_file;
use regex::{Captures, Regex};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
use tokio::spawn;
use tracing::{debug, info, instrument};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectResponse {
    pub kind: String,
    pub next_page_token: Option<String>,
    pub items: Option<Vec<Object>>,
}

#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    pub name: String,
    pub filename: String,
    pub url: String,
    pub size: u64,
    pub slot: u64,
    pub hash: String,
}

#[derive(Clone, Debug, Default)]
pub struct Bounds {
    pub name: String,
    pub url: String,
    pub size: u64,
    pub start_slot: u64,
    pub end_slot: u64,
}

#[derive(Clone, Debug, Default)]
pub struct LedgerSnapshot {
    pub name: String,
    pub epoch: u64,
    pub url: String,
    pub size: u64,
    pub bounds: Bounds,
    pub version: String,
}

#[instrument(err)]
pub async fn list_objects(
    bucket_name: &str,
    next_page_token: &str,
    glob: Option<&str>,
) -> Result<ObjectResponse, reqwest::Error> {
    let base_url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/",
        bucket_name,
    );

    let mut url = Url::parse(&base_url).expect("Failed to parse base URL");

    let mut query_pairs = Vec::new();

    if !next_page_token.is_empty() {
        query_pairs.push(("pageToken", next_page_token));
    }

    if let Some(glob) = glob {
        query_pairs.push(("matchGlob", glob));
    }

    if !query_pairs.is_empty() {
        url.query_pairs_mut().extend_pairs(query_pairs.into_iter());
    }

    debug!("requesting: {:?}", url);

    let response = reqwest::get(url.as_str()).await?;
    response.json().await
}

#[instrument]
pub async fn get_all_objects(
    bucket_name: &str,
    glob: Option<&str>,
) -> Result<Vec<ObjectResponse>, reqwest::Error> {
    let mut next_page_token = "".to_string();
    let mut all_objects = vec![];

    loop {
        let objects = list_objects(bucket_name, &next_page_token, glob).await?;

        let done = objects.next_page_token.is_none();
        next_page_token = objects.next_page_token.clone().unwrap_or_default();

        let num_items = objects.items.as_ref().unwrap_or(&vec![]).len();
        info!("read items: {} done: {}", num_items, done);
        all_objects.push(objects);

        if done {
            break;
        }
    }
    Ok(all_objects)
}

#[instrument]
pub fn search_objects_by_filename<'a>(
    objects: &'a [Object],
    re: &'a Regex,
) -> Vec<(&'a Object, regex::Captures<'a>)> {
    objects
        .iter()
        .filter_map(|object| re.captures(&object.name).map(|caps| (object, caps)))
        .collect()
}

pub fn get_snapshots(objects: &[Object]) -> Vec<Snapshot> {
    let snapshot_regex = Regex::new(r"snapshot-(\d+)-(\w+)\.tar\.(zst|bz2)").unwrap();
    search_objects_by_filename(objects, &snapshot_regex)
        .into_iter()
        .map(|(s, captures)| {
            let slot = captures.get(1).unwrap().as_str().parse().unwrap();
            let hash = captures.get(2).unwrap().as_str().to_string();
            let filename = s.name.split('/').last().unwrap().to_string();
            // let file_type = captures.get(3).unwrap().as_str();

            Snapshot {
                name: s.name.clone(),
                filename,
                url: s.media_link.clone(),
                size: s.size,
                slot,
                hash,
            }
        })
        .collect()
}

async fn get_epoch_bounds(bounds_files: &[(&Object, Captures<'_>)]) -> Vec<(u64, Bounds)> {
    let bounds_text_regex = Regex::new(r"Ledger has data for \d+ slots (\d+) to (\d+)").unwrap();

    let bounds_tasks = bounds_files.iter().map(|(s, captures)| {
        let bounds_text_regex = bounds_text_regex.clone();
        let epoch: u64 = captures.get(1).unwrap().as_str().parse().unwrap();

        let url = s.media_link.clone();
        let name = s.name.to_string();
        let size = s.size;

        spawn(async move {
            let downloaded_contents = download_file(Url::from_str(&url).unwrap()).await.ok()?;
            let downloaded_string = String::from_utf8(downloaded_contents).ok()?;

            let bounds_txt_captures = bounds_text_regex.captures(&downloaded_string)?;
            let start_slot: u64 = bounds_txt_captures.get(1)?.as_str().parse().ok()?;
            let end_slot: u64 = bounds_txt_captures.get(2)?.as_str().parse().ok()?;

            Some((
                epoch,
                Bounds {
                    name,
                    url,
                    size,
                    start_slot,
                    end_slot,
                },
            ))
        })
    });

    join_all(bounds_tasks)
        .await
        .into_iter()
        .filter_map(|r| r.ok()?)
        .collect()
}

async fn get_epochs_versions(versions_file: &[(&Object, Captures<'_>)]) -> Vec<(u64, String)> {
    let bounds_tasks = versions_file.iter().map(|(s, captures)| {
        let epoch: u64 = captures.get(1).unwrap().as_str().parse().unwrap();
        let url = s.media_link.clone();
        spawn(async move {
            let downloaded_contents = download_file(Url::from_str(&url).unwrap()).await.ok()?;
            let downloaded_string = String::from_utf8(downloaded_contents).ok()?;
            Some((epoch, downloaded_string))
        })
    });

    join_all(bounds_tasks)
        .await
        .into_iter()
        .filter_map(|r| r.ok()?)
        .collect()
}

pub async fn get_ledger_snapshots(objects: &[Object]) -> Vec<LedgerSnapshot> {
    let rocksdb_file_regex = Regex::new(r"(\d+)/rocksdb\.tar\.bz2").unwrap();
    let bounds_file_regex = Regex::new(r"(\d+)/bounds\.txt").unwrap();
    let version_file_regex = Regex::new(r"(\d+)/version\.txt").unwrap();

    // need rocksdb for everything
    let mut snapshots: HashMap<u64, LedgerSnapshot> =
        search_objects_by_filename(objects, &rocksdb_file_regex)
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
                        version: String::default(),
                    },
                )
            })
            .collect();

    // grab the bounds.txt files, download the contents, and attempt to parse them
    let epochs_bounds =
        get_epoch_bounds(&search_objects_by_filename(objects, &bounds_file_regex)).await;
    for (epoch, bounds) in epochs_bounds {
        if let Some(ledger) = snapshots.get_mut(&epoch) {
            ledger.bounds = bounds;
        }
    }

    let epochs_version =
        get_epochs_versions(&search_objects_by_filename(objects, &version_file_regex)).await;
    for (epoch, version) in epochs_version {
        if let Some(ledger) = snapshots.get_mut(&epoch) {
            ledger.version = version;
        }
    }

    snapshots.drain().map(|(_, s)| s).collect()
}
