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

pub struct SupervisorGenServer {
    sup_tx: mpsc::Sender<Command>,
    sup_rx: mpsc::Receiver<Command>,
    runtime_handle: Handle,
}

pub struct SupervisorPid {
    sup_tx: mpsc::Sender<Command>,
    runtime_handle: Handle,
}

impl SupervisorGenServer {
    pub fn new() -> SupervisorGenServer {
        SupervisorGenServer::with_runtime_handle(Handle::current())
    }

    pub fn with_runtime_handle(runtime_handle: Handle) -> SupervisorGenServer {
        let (sup_tx, sup_rx) = mpsc::channel(0);
        SupervisorGenServer {
            sup_tx,
            sup_rx,
            runtime_handle,
        }
    }

    pub fn pid(&self) -> SupervisorPid {
        SupervisorPid {
            sup_tx: self.sup_tx.clone(),
            runtime_handle: self.runtime_handle.clone(),
        }
    }

    pub async fn run(self) {
        supervisor_loop(self.sup_rx).await
    }
}

impl SupervisorPid {
    pub fn spawn_link_permanent<F>(&mut self, future: F) where F: Future<Output = ()> + Send + 'static {
        self.spawn_link(future, |pid| Command::PermanentProcessExited { pid, })
    }

    pub fn spawn_link_temporary<F>(&mut self, future: F) where F: Future<Output = ()> + Send + 'static {
        self.spawn_link(future, |pid| Command::TemporaryProcessExited { pid, })
    }

    pub fn child_supevisor(&self) -> SupervisorGenServer {
        SupervisorGenServer::with_runtime_handle(self.runtime_handle.clone())
    }

    fn spawn_link<F, E>(
        &mut self,
        future: F,
        report_exit: E,
    )
    where F: Future<Output = ()> + Send + 'static,
          E: FnOnce(ProcessId) -> Command + Send + 'static,
    {
        let child_sup_tx = self.sup_tx.clone();
        self.runtime_handle.spawn(async {
            let _ = run_child(child_sup_tx, future, report_exit).await;
        });
    }
}

async fn run_child<F, E>(
    mut child_sup_tx: mpsc::Sender<Command>,
    future: F,
    report_exit: E,
)
    -> Result<(), ()>
where F: Future<Output = ()> + Send + 'static,
      E: FnOnce(ProcessId) -> Command + Send + 'static,
{
    let (init_tx, init_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    child_sup_tx.send(Command::ProcessSpawned { init_tx, shutdown_tx, }).await
        .map_err(|_send_error| warn!("supervisor is gone before child is actually spawned"))?;

    let pid = init_rx.await
        .map_err(|_send_error| warn!("supervisor is gone before child pid received"))?;

    let fused_future = future.fuse();
    let mut fused_shutdown_rx = shutdown_rx.fuse();

    pin_mut!(fused_future);
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

    child_sup_tx.send(report_exit(pid)).await.ok();

    Ok(())
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, Debug)]
struct ProcessId {
    pid: usize,
}

enum Command {
    ProcessSpawned { init_tx: oneshot::Sender<ProcessId>, shutdown_tx: oneshot::Sender<Terminate<()>>, },
    PermanentProcessExited { pid: ProcessId, },
    TemporaryProcessExited { pid: ProcessId, },
}

async fn supervisor_loop(mut notify_rx: mpsc::Receiver<Command>) {
    debug!("supervisor loop started");

    let mut children_txs: Vec<Option<oneshot::Sender<Terminate<()>>>> = Vec::new();
    let mut free_cells = Vec::new();

    while let Some(command) = notify_rx.next().await {
        match command {
            Command::ProcessSpawned { init_tx, shutdown_tx, } => {
                let index = match free_cells.pop() {
                    None => {
                        let index = children_txs.len();
                        children_txs.push(Some(shutdown_tx));
                        index
                    },
                    Some(index) => {
                        let prev_value = std::mem::replace(
                            &mut children_txs[index],
                            Some(shutdown_tx),
                        );
                        assert!(prev_value.is_none());
                        index
                    },
                };
                let pid = ProcessId { pid: index, };
                if let Err(_send_error) = init_tx.send(pid) {
                    warn!("supervised task has gone before PID is sent");
                }
                debug!("a supervised process {:?} was spawned", pid);
            },
            Command::PermanentProcessExited { pid, } => {
                debug!("a permanent supervised process {:?} was terminated", pid);
                let index = pid.pid;
                children_txs[index].take().unwrap();
                free_cells.push(index);
                break;
            },
            Command::TemporaryProcessExited { pid, } => {
                debug!("a temporary supervised process {:?} was terminated", pid);
                let index = pid.pid;
                children_txs[index].take().unwrap();
                free_cells.push(index);
            },
        }
    }

    debug!("supervisor shutdown: terminating {} linked processes", children_txs.len());

    for (index, maybe_shutdown_tx) in children_txs.into_iter().enumerate() {
        if let Some(shutdown_tx) = maybe_shutdown_tx {
            let pid = ProcessId { pid: index, };
            if let Err(..) = shutdown_tx.send(Terminate(())) {
                debug!("supervised process {:?} is gone while performing shutdown", pid);
            }
        }
    }
}
