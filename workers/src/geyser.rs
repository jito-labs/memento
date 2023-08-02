use jito_protos::geyser::{
    account::Msg, geyser_service_server::GeyserService, Account, ReplayAccount, SnapshotAccount,
};
use solana_ledger::{
    bank_forks_utils,
    blockstore::Blockstore,
    blockstore_options::{AccessType, BlockstoreOptions, LedgerColumnOptions, ShredStorageType},
    blockstore_processor::ProcessOptions,
};
use solana_runtime::{
    account_storage::meta::StoredAccountMeta,
    accounts_update_notifier_interface::AccountsUpdateNotifierInterface,
    hardened_unpack::unpack_genesis_archive,
    snapshot_config::SnapshotConfig,
    snapshot_utils::{ArchiveFormat, SnapshotVersion},
};
use solana_sdk::{
    account::{AccountSharedData, ReadableAccount},
    clock::Slot,
    genesis_config::{GenesisConfig, DEFAULT_GENESIS_ARCHIVE},
    pubkey::Pubkey,
    transaction::SanitizedTransaction,
};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, RwLock},
};
use tokio::sync::{
    mpsc::{channel, Sender},
    Mutex,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{error, info, instrument, warn};

#[derive(Debug, Default)]
struct Inner {
    account_subscriptions: Vec<Sender<Result<Account, Status>>>,
}

#[derive(Clone, Debug)]
pub struct GeyserGrpcPlugin {
    inner: Arc<Mutex<Inner>>,
    workdir: PathBuf,
    slot: u64,
}

impl GeyserGrpcPlugin {
    pub fn new(workdir: PathBuf, slot: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
            workdir,
            slot,
        }
    }

    pub fn workdir(&self) -> PathBuf {
        self.workdir.clone()
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }
}

#[tonic::async_trait]
impl GeyserService for GeyserGrpcPlugin {
    type SubscribeAccountsStream = ReceiverStream<Result<Account, Status>>;

    #[instrument]
    async fn subscribe_accounts(
        &self,
        _request: Request<()>,
    ) -> Result<Response<Self::SubscribeAccountsStream>, Status> {
        let mut inner = self.inner.lock().await;
        let (sender, receiver) = channel(100);
        inner.account_subscriptions.push(sender);
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

impl AccountsUpdateNotifierInterface for GeyserGrpcPlugin {
    fn notify_account_update(
        &self,
        _slot: Slot,
        _account: &AccountSharedData,
        _txn: &Option<&SanitizedTransaction>,
        _pubkey: &Pubkey,
        _write_version: u64,
    ) {
        let mut inner = self.inner.blocking_lock();
        // TODO (LB): implement this when live
        let account = Account {
            msg: Some(Msg::Replay(ReplayAccount {})),
        };
        inner
            .account_subscriptions
            .retain(|sub| sub.blocking_send(Ok(account.clone())).is_ok());
    }

    /// Note: the slot notified here may not be equal to the snapshot slot
    /// See AccountsDb::notify_account_restore_from_snapshot
    fn notify_account_restore_from_snapshot(&self, slot: Slot, account: &StoredAccountMeta) {
        let account_proto = Account {
            msg: Some(Msg::Snap(SnapshotAccount {
                slot,
                pubkey: account.pubkey().to_bytes().to_vec(),
                lamports: account.lamports(),
                owner: account.owner().to_bytes().to_vec(),
                is_executable: account.executable(),
                rent_epoch: account.rent_epoch(),
                data: account.data().to_vec(),
                seq: account.write_version(),
                is_startup: true,
                tx_signature: None,
            })),
        };
        let mut inner = self.inner.blocking_lock();
        inner
            .account_subscriptions
            .retain(|sub| sub.blocking_send(Ok(account_proto.clone())).is_ok());
    }

    fn notify_end_of_restore_from_snapshot(&self) {
        let mut inner = self.inner.blocking_lock();
        let account = Account {
            msg: Some(Msg::Replay(ReplayAccount {})),
        };

        // Right now we'll send an empty replay account since replay isn't live
        inner
            .account_subscriptions
            .retain(|sub| sub.blocking_send(Ok(account.clone())).is_ok());
    }
}

pub fn start_snapshot_loading(plugin: GeyserGrpcPlugin, exit: Arc<AtomicBool>) {
    let workdir = plugin.workdir();

    info!("workdir: {:?}", workdir);
    info!("loading genesis config...");
    let genesis_config = GenesisConfig::load(&workdir).unwrap_or_else(|load_err| {
        let genesis_package = workdir.join(DEFAULT_GENESIS_ARCHIVE);
        unpack_genesis_archive(&genesis_package, &workdir, 10 * 1024 * 1024).unwrap_or_else(
            |unpack_err| {
                warn!(
                    "Failed to open ledger genesis_config at {:?}: {}, {}",
                    workdir, load_err, unpack_err,
                );
            },
        );

        // loading must succeed at this moment
        GenesisConfig::load(&workdir).unwrap()
    });

    info!("opening blockstore in workdir: {:?}", workdir);
    let blockstore = Blockstore::open_with_options(
        &workdir,
        BlockstoreOptions {
            access_type: AccessType::Primary,
            recovery_mode: None,
            enforce_ulimit_nofile: true,
            column_options: LedgerColumnOptions {
                shred_storage_type: ShredStorageType::RocksLevel,
                ..LedgerColumnOptions::default()
            },
        },
    );
    if let Err(e) = blockstore {
        error!("error loading blockstore: {:?}", e);
        return;
    }
    let blockstore = Arc::new(blockstore.unwrap());

    let accounts_update_notifier = Arc::new(RwLock::new(plugin));

    let account_paths = vec![blockstore.ledger_path().join("accounts.ledger-tool")];
    let bank_snapshots_dir = blockstore.ledger_path().join("snapshot.ledger-tool");

    let (_bank_forks, _leader_schedule_cache, _starting_snapshot_hashes, ..) =
        bank_forks_utils::load_bank_forks(
            &genesis_config,
            blockstore.as_ref(),
            account_paths,
            None,
            Some(&SnapshotConfig {
                full_snapshot_archives_dir: blockstore.ledger_path().clone(),
                incremental_snapshot_archives_dir: blockstore.ledger_path().clone(),
                bank_snapshots_dir,
                archive_format: ArchiveFormat::TarZstd,
                snapshot_version: SnapshotVersion::default(),
                ..SnapshotConfig::default()
            }),
            &ProcessOptions {
                accounts_db_skip_shrink: true,
                ..ProcessOptions::default()
            },
            None,
            None,
            Some(accounts_update_notifier),
            &exit,
        );
}
