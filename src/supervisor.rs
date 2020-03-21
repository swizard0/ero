use futures::{
    Future,
    SinkExt,
    StreamExt,
    FutureExt,
    channel::{
        mpsc,
        oneshot,
    },
    select,
    pin_mut,
};

use tokio::runtime::Handle;

use log::{
    warn,
    debug,
    error,
};

use super::Terminate;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SupervisorLoopGone;

pub struct Supervisor {
    runtime_handle: Handle,
    notify_tx: mpsc::Sender<SupervisorNotify>,
    shutdown_rx: oneshot::Receiver<Terminate<()>>,
}

impl Supervisor {
    pub fn new() -> Supervisor {
        Supervisor::with_runtime_handle(Handle::current())
    }

    pub fn with_runtime_handle(runtime_handle: Handle) -> Supervisor {
        let (notify_tx, notify_rx) = mpsc::channel(0);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        runtime_handle.spawn(supervisor_loop(notify_rx, shutdown_tx));

        Supervisor { runtime_handle, notify_tx, shutdown_rx, }
    }

    pub fn spawn_link<F>(&self, future: F) where F: Future<Output = ()> + Send + 'static {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let mut child_notify_tx = self.notify_tx.clone();
        let future = async move {
            let fused_future = future.fuse();
            let mut fused_shutdown_rx = shutdown_rx.fuse();
            pin_mut!(fused_future);

            match child_notify_tx.send(SupervisorNotify::Spawned(shutdown_tx)).await {
                Ok(()) =>
                    select! {
                        () = fused_future => {
                            debug!("a process was terminated");
                            child_notify_tx.send(SupervisorNotify::Exited).await.ok();
                        },
                        result = fused_shutdown_rx =>
                            match result {
                                Ok(Terminate(())) =>
                                    debug!("a process was terminated (shutted down)"),
                                Err(oneshot::Canceled) =>
                                    debug!("a process was terminated (shutdown channel supervisor endpoint dropped)"),
                            },
                    },
                Err(_send_error) =>
                    warn!("supervisor is gone before child is actually spawned"),
            }
        };
        self.runtime_handle.spawn(future);
    }

    pub async fn shutdown_on_idle(self) -> Result<(), SupervisorLoopGone> {
        match self.shutdown_rx.await {
            Ok(Terminate(())) =>
                Ok(()),
            Err(oneshot::Canceled) => {
                error!("supervisor loop dropped unexpectedly its shutdown channel endpoint");
                Err(SupervisorLoopGone)
            }
        }
    }
}

enum SupervisorNotify {
    Spawned(oneshot::Sender<Terminate<()>>),
    Exited,

}

async fn supervisor_loop(mut notify_rx: mpsc::Receiver<SupervisorNotify>, shutdown_tx: oneshot::Sender<Terminate<()>>) {
    debug!("supervisor loop started");

    let mut children_txs = Vec::new();
    while let Some(event) = notify_rx.next().await {
        match event {
            SupervisorNotify::Spawned(notify_tx) => {
                debug!("a process was spawned");
                children_txs.push(notify_tx);
            },
            SupervisorNotify::Exited => {
                debug!("a supervised process was terminated");
                break;
            },
        }
    }

    debug!("supervisor spawn notify endpoint disconnected: terminating {} linked processes", children_txs.len());
    for child_tx in children_txs {
        if let Err(..) = child_tx.send(Terminate(())) {
            debug!("supervised child is gone while performing shutdown");
        }
    }
    if let Err(..) = shutdown_tx.send(Terminate(())) {
        debug!("supervisor shutdown channel endpoint dropped");
    }
}
