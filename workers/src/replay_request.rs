use crate::geyser::{start_snapshot_loading, GeyserGrpcPlugin};
use jito_gcs::bucket::Snapshot;
use jito_net::{http::download_and_save_file, slack::SlackClient};
use jito_protos::{
    geyser,
    geyser::{
        geyser_service_server::GeyserServiceServer, replay_service_server::ReplayService,
        GetReplayStateRequest, GetReplayStateResponse, ReplayContext, ReplayRequest,
        ReplayResponse,
    },
};
use reqwest::Url;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};
use tokio::{
    fs::{create_dir_all, read_dir, remove_dir_all},
    net::TcpListener,
    spawn,
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot, Mutex,
    },
    task::{spawn_blocking, JoinHandle},
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{
    codegen::http,
    transport::{Error, Server},
    Request, Response, Status,
};
use tracing::{debug, error, info, instrument, warn};

#[derive(Debug, Clone)]
pub enum ReplayError {
    Download(String),
}

/// Keeps track of state and currently running workspaces
#[derive(Debug)]
struct ReplayState {
    workspaces: HashMap<u64, Workspace>,
    max_workspaces: usize,
    count: u64,
}

impl ReplayState {
    pub fn new(max_workspaces: usize) -> Self {
        Self {
            workspaces: HashMap::with_capacity(max_workspaces),
            max_workspaces,
            count: 0,
        }
    }
}

#[derive(Debug)]
struct Workspace {
    slot: u64,
    shutdown_tx: oneshot::Sender<()>,
    download_and_replay_handle: JoinHandle<Result<ReplayBackgroundThreadContext, http::Error>>,
    addr: SocketAddr,
}

#[derive(Clone, Debug)]
pub struct ReplayServiceImpl {
    inner: Arc<Mutex<ReplayState>>,
    snapshots: Vec<Snapshot>,
    event_sender: Sender<Event>,
}

#[derive(Debug)]
pub enum Event {
    ReplayStart {
        request: ReplayRequest,
        response_sender: Sender<Result<ReplayResponse, Status>>,
    },
    Stopped {
        workspace_id: u64,
        error: Option<ReplayError>,
    },
}

#[derive(Debug)]
#[allow(unused)]
struct ReplayBackgroundThreadContext {
    handle: JoinHandle<Result<(), Error>>,
    snapshot_load_thread: Option<thread::JoinHandle<()>>,
}

impl ReplayServiceImpl {
    pub fn new(
        workdir: PathBuf,
        snapshots: Vec<Snapshot>,
        rpc: String,
        starting_geyser_port: u16,
        max_workspaces: usize,
        slack_client: SlackClient,
    ) -> Self {
        let inner = Arc::new(Mutex::new(ReplayState::new(max_workspaces)));

        let (event_sender, receiver) = channel(10);
        spawn(Self::start_request_handler(
            event_sender.clone(),
            receiver,
            inner.clone(),
            workdir,
            snapshots.clone(),
            rpc,
            starting_geyser_port,
            slack_client,
        ));

        Self {
            inner,
            snapshots,
            event_sender,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_request_handler(
        event_sender: Sender<Event>,
        mut receiver: Receiver<Event>,
        inner_state: Arc<Mutex<ReplayState>>,
        workdir: PathBuf,
        snapshots: Vec<Snapshot>,
        rpc: String,
        starting_geyser_port: u16,
        slack_client: SlackClient,
    ) {
        while let Some(event) = receiver.recv().await {
            info!("received event: {:?}", event);
            match event {
                Event::ReplayStart {
                    request,
                    response_sender,
                } => {
                    let result = Self::handle_replay_start(
                        &request,
                        &event_sender,
                        &inner_state,
                        &workdir,
                        &snapshots,
                        &rpc,
                        &starting_geyser_port,
                        &slack_client,
                    )
                    .await;
                    if response_sender.send(result).await.is_err() {
                        warn!("error sending back result");
                    }
                }
                Event::Stopped {
                    workspace_id,
                    error,
                } => {
                    info!(
                        "stopped for workspace_id: {} error: {:?}",
                        workspace_id, error
                    );
                    let mut state = inner_state.lock().await;
                    if let Some(Workspace {
                        slot,
                        shutdown_tx,
                        download_and_replay_handle,
                        addr: _,
                    }) = state.workspaces.remove(&workspace_id)
                    {
                        slack_client
                            .post_slack_message(&format!(
                                "backfill done slot: {} error: {:?}",
                                slot, error
                            ))
                            .await;

                        info!("shutting down workspace: {} slot: {}", workspace_id, slot);
                        if shutdown_tx.send(()).is_err() {
                            warn!("error sending shutdown signal");
                        }

                        info!("trying to join download_and_replay_handle...");
                        let response = download_and_replay_handle.await;
                        info!("joined download_and_replay_handle, result: {:?}", response);
                        match response {
                            Ok(Ok(ReplayBackgroundThreadContext {
                                handle,
                                snapshot_load_thread,
                            })) => {
                                info!("joining background thread");
                                let join_res = handle.await;
                                info!("join_res: {:?}", join_res);
                                if let Some(snapshot_load_thread) = snapshot_load_thread {
                                    info!("joining snapshot thread, this might take awhile...");
                                    let snapshot_join =
                                        spawn_blocking(move || snapshot_load_thread.join()).await;
                                    info!("snapshot_join: {:?}", snapshot_join);
                                }
                            }
                            Ok(Err(e)) => {
                                error!("http error: {:?}", e);
                            }
                            Err(e) => {
                                error!("join error: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(event_sender, snapshots))]
    async fn handle_replay_start(
        request: &ReplayRequest,
        event_sender: &Sender<Event>,
        inner_state: &Arc<Mutex<ReplayState>>,
        workdir: &PathBuf,
        snapshots: &[Snapshot],
        rpc: &String,
        starting_geyser_port: &u16,
        slack_client: &SlackClient,
    ) -> Result<ReplayResponse, Status> {
        let mut inner = inner_state.lock().await;
        if inner.workspaces.len() >= inner.max_workspaces {
            return Err(Status::resource_exhausted("workspaces already in use"));
        }

        let snapshot = snapshots
            .iter()
            .find(|s| s.slot == request.slot)
            .ok_or_else(|| Status::invalid_argument("snapshot doesnt exist"))?;

        if inner.workspaces.values().any(|w| w.slot == snapshot.slot) {
            return Err(Status::resource_exhausted("already replaying snapshot"));
        }

        // Prepare filesystem

        debug!("creating work directory");
        create_dir_all(&workdir).await.map_err(|e| {
            warn!("error creating workdir: {:?}", e);
            Status::internal("cant create workdir")
        })?;

        debug!("removing old downloads");
        Self::remove_old_downloads(&inner, &snapshot.slot, workdir).await?;

        debug!("creating snapshot directory");
        let mut snapshot_folder = workdir.clone();
        snapshot_folder.push(format!("{}/", request.slot));
        create_dir_all(&snapshot_folder).await.map_err(|e| {
            warn!("error creating snapshot workdir: {:?}", e);
            Status::internal("cant create snapshot workdir")
        })?;

        // Download genesis from an RPC server and start to configure the larger download

        let exit = Arc::new(AtomicBool::new(false));

        info!("downloading genesis");
        let url = Url::from_str(&format!("{}/genesis.tar.bz2", rpc)).unwrap();
        download_and_save_file(
            url,
            &format!("{}/genesis.tar.bz2", snapshot_folder.to_str().unwrap()),
            exit.clone(),
        )
        .await
        .map_err(|e| {
            warn!("error downloading genesis: {:?}", e);
            Status::internal("error downloading genesis file")
        })?;

        info!("finding geyser port to bind to");
        let mut port = *starting_geyser_port;
        let (addr, listener) = loop {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
            if let Ok(listener) = TcpListener::bind(addr).await {
                break (addr, listener);
            } else {
                port += 1;
            }
        };
        info!("got socket at: {:?}", addr);

        // spawn off the more involved and time sensitive tasks
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        slack_client
            .post_slack_message(&format!(
                "backfill downloading snapshot slot: {}",
                request.slot
            ))
            .await;

        inner.count += 1;
        let download_and_replay_handle = Self::spawn_download_and_replay_task(
            snapshot_folder,
            snapshot,
            shutdown_rx,
            listener,
            &exit,
            event_sender,
            inner.count,
            slack_client,
        );
        let count = inner.count;
        inner.workspaces.insert(
            count,
            Workspace {
                slot: snapshot.slot,
                shutdown_tx,
                download_and_replay_handle,
                addr,
            },
        );

        Ok(ReplayResponse {
            addr: addr.to_string(),
            workspace_id: inner.count,
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument]
    fn spawn_download_and_replay_task(
        snapshot_folder: PathBuf,
        snapshot: &Snapshot,
        shutdown_rx: oneshot::Receiver<()>,
        listener: TcpListener,
        exit: &Arc<AtomicBool>,
        event_sender: &Sender<Event>,
        workspace_id: u64,
        slack_client: &SlackClient,
    ) -> JoinHandle<Result<ReplayBackgroundThreadContext, http::Error>> {
        let geyser_plugin = GeyserGrpcPlugin::new(snapshot_folder.clone(), snapshot.slot);
        let exit = exit.clone();
        let snapshot = snapshot.clone();
        let event_sender = event_sender.clone();
        let slack_client = slack_client.clone();

        spawn(async move {
            let server_handle = {
                let exit = exit.clone();
                let event_sender = event_sender.clone();
                spawn(
                    Server::builder()
                        .add_service(GeyserServiceServer::new(geyser_plugin.clone()))
                        .serve_with_incoming_shutdown(
                            TcpListenerStream::new(listener),
                            async move {
                                info!("waiting for shutdown");
                                let _ = shutdown_rx.await;
                                exit.store(true, Ordering::Relaxed);
                                let _ = event_sender
                                    .send(Event::Stopped {
                                        workspace_id,
                                        error: None,
                                    })
                                    .await;
                                warn!("shutting down server");
                            },
                        ),
                )
            };

            let snapshot_filepath = snapshot_folder.join(snapshot.filename);

            // TODO (LB): check here to see if already downloaded
            info!("downloading snapshot to {:?}", snapshot_filepath);
            if let Err(e) = download_and_save_file(
                Url::from_str(&snapshot.url).unwrap(),
                snapshot_filepath.as_os_str().to_str().unwrap(),
                exit.clone(),
            )
            .await
            {
                error!("sending done event to main thread. download error: {:?}", e);
                let _ = event_sender
                    .send(Event::Stopped {
                        workspace_id,
                        error: Some(ReplayError::Download(e.to_string())),
                    })
                    .await;
                // TODO (LB): return an error here with server_handle to shut it down?
                return Ok(ReplayBackgroundThreadContext {
                    handle: server_handle,
                    snapshot_load_thread: None,
                });
            }

            let snapshot_load_thread = {
                let exit = exit.clone();
                let event_sender = event_sender.clone();
                let slot = snapshot.slot;

                slack_client
                    .post_slack_message(&format!("backfill geyser stream starting slot: {}", slot))
                    .await;

                thread::spawn(move || {
                    info!("starting snapshot loader");
                    start_snapshot_loading(geyser_plugin, exit);

                    info!("done loading snapshot, sending stopped event to main thread");
                    let _ = event_sender.blocking_send(Event::Stopped {
                        workspace_id,
                        error: None,
                    });
                })
            };

            Ok(ReplayBackgroundThreadContext {
                handle: server_handle,
                snapshot_load_thread: Some(snapshot_load_thread),
            })
        })
    }

    #[instrument(skip(state))]
    async fn remove_old_downloads(
        state: &ReplayState,
        slot: &u64,
        workdir: &PathBuf,
    ) -> Result<(), Status> {
        // find the slots being actively downloaded right now
        let active_downloading_slots: Vec<u64> =
            state.workspaces.values().map(|w| w.slot).collect();
        info!("actively downloading slots: {:?}", active_downloading_slots);

        // delete all the numerical directories not being actively downloaded
        let mut entries = read_dir(workdir).await.map_err(|e| {
            warn!("error reading workdir: {:?}", e);
            Status::internal("cant read work directory")
        })?;
        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            warn!("error getting next_entry in workdir: {:?}", e);
            Status::internal("error iterating over entries")
        })? {
            let metadata = entry.metadata().await.map_err(|e| {
                warn!("error reading metadata: {:?}", e);
                Status::internal("error reading metadata")
            })?;
            if metadata.is_dir() {
                let dir_name = entry.file_name();
                let dir_name_str = dir_name.to_str().unwrap();
                if let Ok(directory_slot) = dir_name_str.parse::<u64>() {
                    if !active_downloading_slots.contains(&directory_slot)
                        && directory_slot != *slot
                    {
                        warn!("deleting directory: {:?}", entry.path());
                        remove_dir_all(&entry.path()).await.map_err(|e| {
                            error!("error deleting directory: {:?}", e);
                            Status::internal("error deleting directory")
                        })?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl ReplayService for ReplayServiceImpl {
    #[instrument]
    async fn start_replay(
        &self,
        request: Request<ReplayRequest>,
    ) -> Result<Response<ReplayResponse>, Status> {
        let (response_sender, mut response_receiver) = channel(1);

        self.event_sender
            .send(Event::ReplayStart {
                response_sender,
                request: request.into_inner(),
            })
            .await
            .map_err(|_| Status::internal("error sending replay request"))?;
        if let Some(response) = response_receiver.recv().await {
            let res = response?;
            Ok(Response::new(res))
        } else {
            Err(Status::internal("error receiving response"))
        }
    }

    type GetSnapshotsStream = ReceiverStream<Result<geyser::Snapshot, Status>>;

    /// Request the snapshots available
    #[instrument]
    async fn get_snapshots(
        &self,
        _request: Request<()>,
    ) -> Result<Response<Self::GetSnapshotsStream>, Status> {
        let (sender, receiver) = channel(100);
        let snapshots = self.snapshots.clone();
        spawn(async move {
            for s in snapshots {
                if let Err(e) = sender
                    .send(Ok(geyser::Snapshot {
                        name: s.name,
                        filename: s.filename,
                        url: s.url,
                        size: s.size,
                        slot: s.slot,
                        hash: s.hash,
                    }))
                    .await
                {
                    warn!("error sending snapshot: {:?}", e);
                    return Err(e);
                }
            }
            Ok(())
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    #[instrument]
    async fn get_replay_state(
        &self,
        _request: Request<GetReplayStateRequest>,
    ) -> Result<Response<geyser::GetReplayStateResponse>, Status> {
        let inner = self.inner.lock().await;
        let states = inner
            .workspaces
            .iter()
            .map(
                |(workspace_id, Workspace { slot, addr, .. })| ReplayContext {
                    workspace_id: *workspace_id,
                    addr: addr.to_string(),
                    slot: *slot,
                },
            )
            .collect();
        Ok(Response::new(GetReplayStateResponse { states }))
    }
}
