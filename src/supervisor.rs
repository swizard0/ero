use std::collections::HashMap;

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
};

use super::Terminate;

pub struct Init {
    notify_rx: mpsc::Receiver<Event>,
}

pub async fn run(init: Init) {
    supervisor_loop(init.notify_rx).await
}

pub struct Supervisor {
    current_pid: ProcessId,
    runtime_handle: Handle,
    notify_tx: mpsc::Sender<Event>,
}

impl Supervisor {
    pub fn new() -> (Supervisor, Init) {
        Supervisor::with_runtime_handle(Handle::current())
    }

    pub fn with_runtime_handle(runtime_handle: Handle) -> (Supervisor, Init) {
        let (notify_tx, notify_rx) = mpsc::channel(0);

        (
            Supervisor {
                current_pid: ProcessId::default(),
                runtime_handle,
                notify_tx,
            },
            Init { notify_rx, },
        )
    }

    pub fn spawn_link_permanent<F>(&mut self, future: F) where F: Future<Output = ()> + Send + 'static {
        self.spawn_link(future, |pid| Event::PermanentProcessExited { pid, })
    }

    pub fn spawn_link_temporary<F>(&mut self, future: F) where F: Future<Output = ()> + Send + 'static {
        self.spawn_link(future, |pid| Event::TemporaryProcessExited { pid, })
    }

    fn spawn_link<F, E>(
        &mut self,
        future: F,
        report_exit: E,
    )
    where F: Future<Output = ()> + Send + 'static,
          E: FnOnce(ProcessId) -> Event + Send + 'static,
    {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let pid = self.current_pid.incf();
        let mut child_notify_tx = self.notify_tx.clone();

        let future = async move {
            let fused_future = future.fuse();
            let mut fused_shutdown_rx = shutdown_rx.fuse();
            pin_mut!(fused_future);

            match child_notify_tx.send(Event::ProcessSpawned { pid, shutdown_tx, }).await {
                Ok(()) => {
                    select! {
                        () = fused_future =>
                            debug!("a process was terminated"),
                        result = fused_shutdown_rx =>
                            match result {
                                Ok(Terminate(())) =>
                                    debug!("a process was terminated (shutted down)"),
                                Err(oneshot::Canceled) =>
                                    debug!("a process was terminated (shutdown channel supervisor endpoint dropped)"),
                            },
                    };
                    child_notify_tx
                        .send(report_exit(pid))
                        .await
                        .ok();
                },
                Err(_send_error) =>
                    warn!("supervisor is gone before child is actually spawned"),
            }
        };
        self.runtime_handle.spawn(future);
    }

}

#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, Debug)]
struct ProcessId {
    pid: usize,
}

impl ProcessId {
    fn incf(&mut self) -> ProcessId {
        let old = self.clone();
        self.pid += 1;
        old
    }
}

enum Event {
    ProcessSpawned { pid: ProcessId, shutdown_tx: oneshot::Sender<Terminate<()>>, },
    PermanentProcessExited { pid: ProcessId, },
    TemporaryProcessExited { pid: ProcessId, },
}

async fn supervisor_loop(mut notify_rx: mpsc::Receiver<Event>) {
    debug!("supervisor loop started");

    let mut children_txs = HashMap::new();

    while let Some(event) = notify_rx.next().await {
        match event {
            Event::ProcessSpawned { pid, shutdown_tx, } => {
                debug!("a supervised process {:?} was spawned", pid);
                children_txs.insert(pid, shutdown_tx);
            },
            Event::PermanentProcessExited { pid, } => {
                debug!("a permanent supervised process {:?} was terminated", pid);
                if children_txs.remove(&pid).is_none() {
                    warn!("something wrong: terminated permanent process {:?} was not supervised", pid);
                } else {
                    break;
                }
            },
            Event::TemporaryProcessExited { pid, } => {
                debug!("a temporary supervised process {:?} was terminated", pid);
                if children_txs.remove(&pid).is_none() {
                    warn!("something wrong: terminated temporary process {:?} was not supervised", pid);
                }
            },
        }
    }

    debug!("supervisor shutdown: terminating {} linked processes", children_txs.len());

    for (pid, shutdown_tx) in children_txs {
        if let Err(..) = shutdown_tx.send(Terminate(())) {
            debug!("supervised process {:?} is gone while performing shutdown", pid);
        }
    }
}
