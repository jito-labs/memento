use futures_util::StreamExt;
use reqwest::Url;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use thiserror::Error;
use tokio::{
    fs::{remove_file, File},
    io,
    io::AsyncWriteExt,
};
use tracing::{info, instrument, warn};

#[derive(Debug, Error)]
pub enum Error {
    #[error("reqwest: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("IO: {0}")]
    IOError(#[from] io::Error),
    #[error("Exited")]
    Exited,
}

/// Download and save large files to disk
pub async fn download_and_save_file(
    url: Url,
    path: &str,
    exit: Arc<AtomicBool>,
) -> Result<(), Error> {
    info!("GET {}, saving to {}", url, path);

    let response = reqwest::get(url).await?;

    let content_length = response.content_length().unwrap();
    info!("content_length: {:?}", content_length);

    let mut out = File::create(&path).await?;

    let mut stream = response.bytes_stream();
    while let Some(Ok(item)) = stream.next().await {
        if exit.load(Ordering::Relaxed) {
            if let Err(e) = remove_file(path).await {
                warn!("error removing file: {:?}", e);
            }
            return Err(Error::Exited);
        }
        out.write_all(&item).await?;
    }

    info!("flushing file...");
    let _ = out.flush().await;
    info!("Saved file to {}", path);

    Ok(())
}

/// Download smaller files
#[instrument]
pub async fn download_file(url: Url) -> Result<Vec<u8>, Error> {
    let response = reqwest::get(url).await?;
    let mut stream = response.bytes_stream();

    let mut bytes = vec![];
    while let Some(Ok(item)) = stream.next().await {
        bytes.extend(item);
    }
    Ok(bytes)
}
