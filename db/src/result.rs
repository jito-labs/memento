use std::result;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("ClickhouseError: {0}")]
    ClickhouseError(#[from] clickhouse_rs::errors::Error),
}

pub type Result<T> = result::Result<T, DatabaseError>;
