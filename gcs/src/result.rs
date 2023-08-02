use thiserror::Error;

#[derive(Debug, Error)]
pub enum GcsError {
    #[error("message")]
    GcloudError(#[from] reqwest::Error),
}
