use serde::{Deserialize, Deserializer};
use solana_sdk::pubkey::Pubkey;
use std::{path::PathBuf, str::FromStr, string::FromUtf8Error};
use thiserror::Error;
use tokio::{fs, io};

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Io: {0}")]
    Io(#[from] io::Error),

    #[error("SerdeYaml: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),

    #[error("Utf8: {0}")]
    Utf8(#[from] FromUtf8Error),
}

#[derive(Debug, Deserialize)]
pub struct BackfillConfig {
    pub max_workers: usize,
    #[serde(deserialize_with = "BackfillConfig::deserialize_pubkey")]
    pub programs: Vec<Pubkey>,
    pub slots: Vec<u64>,
}

impl BackfillConfig {
    fn deserialize_pubkey<'de, D>(deserializer: D) -> Result<Vec<Pubkey>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let keys: Vec<String> = Vec::deserialize(deserializer)?;
        keys.into_iter()
            .map(|pubkey| Pubkey::from_str(&pubkey).map_err(serde::de::Error::custom))
            .collect()
    }

    pub async fn read_backfill_config(path: &PathBuf) -> Result<BackfillConfig, ConfigError> {
        let contents = String::from_utf8(fs::read(path).await?)?;
        Ok(serde_yaml::from_str(&contents)?)
    }
}
