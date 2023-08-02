use clickhouse_rs::{errors::Result, row, Block, Pool};
use futures_util::StreamExt;
use tracing::instrument;

/// The progress table keeps track of completed snapshots
#[derive(Clone)]
pub struct ProgressTable {
    pool: Pool,
}

impl ProgressTable {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    #[instrument(skip(self))]
    pub async fn migrate(&self) -> Result<()> {
        let mut client = self.pool.get_handle().await?;
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS progress
                    (
                        slot          Int64
                    ) ENGINE = MergeTree()
                    ORDER BY slot
                    PRIMARY KEY slot;
                    ",
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn insert_completed_slot(&self, slot: u64) -> Result<()> {
        let mut client = self.pool.get_handle().await?;

        let mut block = Block::new();
        block.push(row! {slot: slot as i64})?;
        client.insert("progress", block).await?;
        Ok(())
    }

    #[instrument(skip_all, ret, err)]
    pub async fn get_completed_slots(&self) -> Result<Vec<u64>> {
        let mut client = self.pool.get_handle().await?;
        let mut stream = client.query("SELECT * FROM progress").stream();

        let mut slots = vec![];
        while let Some(row) = stream.next().await {
            let row = row?;
            let slot: i64 = row.get("slot")?;
            slots.push(slot as u64);
        }
        Ok(slots)
    }
}
