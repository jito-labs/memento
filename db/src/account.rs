use clickhouse_rs::{errors::Result, row, Block, Pool};
use futures_util::StreamExt;
use tracing::instrument;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountModel {
    pub slot: i64,
    pub snapshot_slot: i64,
    pub pubkey: String,
    pub lamports: i64,
    pub owner: String,
    pub is_executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
    pub seq: i64,
}

#[derive(Clone)]
pub struct AccountsTable {
    pool: Pool,
}

impl AccountsTable {
    pub fn new(pool: Pool) -> AccountsTable {
        AccountsTable { pool }
    }

    #[instrument(skip(self), err)]
    pub async fn migrate(&self) -> Result<()> {
        let mut client = self.pool.get_handle().await?;
        // TODO (LB): https://stackoverflow.com/questions/53442559/how-to-avoid-duplicates-in-clickhouse-table
        // This table keeps duplicates, which is fine for now, but we should fix this eventually
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS accounts
            (
            slot          Int64, -- slot the account was last modified
            snapshot_slot Int64, -- snapshot slot the account was loaded from
            pubkey        String,
            lamports      Int64,
            owner         String,
            is_executable UInt8,
            rent_epoch    Int64,
            data          String,
            seq           Int64
            ) ENGINE = MergeTree()
            ORDER BY (snapshot_slot, pubkey)
            PRIMARY KEY (snapshot_slot, pubkey);
            ",
            )
            .await?;
        client.execute("ALTER TABLE accounts ADD INDEX IF NOT EXISTS account_pubkey_index pubkey TYPE set(0);").await?;
        client
            .execute(
                "ALTER TABLE accounts ADD INDEX IF NOT EXISTS account_slot_index slot TYPE set(0);",
            )
            .await?;
        client.execute("ALTER TABLE accounts ADD INDEX IF NOT EXISTS account_owner_index owner TYPE set(0);").await?;
        Ok(())
    }

    /// Clickhouse reccomends bulk inserting data for performance reasons
    /// https://clickhouse.com/docs/en/optimize/bulk-inserts
    #[instrument(skip_all, err)]
    pub async fn insert_accounts(&self, accounts: &[AccountModel]) -> Result<()> {
        let mut client = self.pool.get_handle().await?;

        let mut block = Block::new();
        for account in accounts {
            block.push(row! {slot: account.slot, snapshot_slot: account.snapshot_slot, pubkey: account.pubkey.to_string(), lamports: account.lamports, owner: account.owner.to_string(), is_executable: account.is_executable as u8, rent_epoch: account.rent_epoch, data: account.data.clone(), seq: account.seq})?;
        }

        client.insert("accounts", block).await?;
        Ok(())
    }

    /// Note: don't call this with token program, you will probably OOM
    #[instrument(skip(self), err)]
    pub async fn read_accounts(&self, owner: &String) -> Result<Vec<AccountModel>> {
        let mut client = self.pool.get_handle().await?;

        let mut stream = client
            .query(format!("SELECT * FROM accounts where owner = '{}'", owner))
            .stream();

        let mut accounts = vec![];
        while let Some(row) = stream.next().await {
            let row = row?;
            accounts.push(AccountModel {
                slot: row.get("slot")?,
                snapshot_slot: row.get("snapshot_slot")?,
                pubkey: row.get("pubkey")?,
                lamports: row.get("lamports")?,
                owner: row.get("owner")?,
                is_executable: row.get::<u8, _>("is_executable")? != 0,
                rent_epoch: row.get("rent_epoch")?,
                data: row.get("data")?,
                seq: row.get("seq")?,
            });
        }
        Ok(accounts)
    }

    #[instrument(skip(self), err)]
    pub async fn delete_slot(&self, slot: i64) -> Result<()> {
        let mut client = self.pool.get_handle().await?;

        client
            .execute(format!(
                "DELETE FROM accounts where snapshot_slot = {}",
                slot
            ))
            .await
    }
}
