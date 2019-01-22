use futures::{
    Sink,
    Future,
    Stream,
    sync::{
        mpsc,
        oneshot,
    },
    future::Either,
};

use log::{
    warn,
    debug,
    error,
};

struct Shutdown;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SupervisorLoopGone;

pub struct Supervisor {
    executor: tokio::runtime::TaskExecutor,
    notify_tx: mpsc::Sender<SupervisorNotify>,
    shutdown_rx: oneshot::Receiver<Shutdown>,
}

enum SupervisorNotify {
    Spawned(oneshot::Sender<Shutdown>),
    Exited,
}

impl Supervisor {
    pub fn new(executor: &tokio::runtime::TaskExecutor) -> Supervisor {
        let (notify_tx, notify_rx) = mpsc::channel(0);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        executor.spawn(supervisor_loop(notify_rx, shutdown_tx));

        Supervisor {
            executor: executor.clone(),
            notify_tx,
            shutdown_rx,
        }
    }

    pub fn executor(&self) -> &tokio::runtime::TaskExecutor {
        &self.executor
    }

    pub fn spawn_link<F>(&self, restartable_future: F) where F: Future<Item = (), Error = ()> + Send + 'static {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let future = self.notify_tx
            .clone()
            .send(SupervisorNotify::Spawned(shutdown_tx))
            .map_err(|_send_error| {
                warn!("supervisor is gone before task is actually spawned");
            })
            .and_then(move |notify_tx| {
                restartable_future
                    .select2(shutdown_rx)
                    .then(|result| {
                        match result {
                            Ok(Either::A(((), _shutdown_rx))) => {
                                debug!("a process was terminated (normally)");
                                Ok(())
                            },
                            Ok(Either::B((Shutdown, _task_future))) => {
                                debug!("a process was terminated (shutted down)");
                                Ok(())
                            },
                            Err(Either::A(((), _shutdown_rx))) => {
                                error!("a process was terminated (task error)");
                                Err(())
                            },
                            Err(Either::B((oneshot::Canceled, _task_future))) => {
                                debug!("a process was terminated (shutdown channel supervisor endpoint dropped)");
                                Err(())
                            },
                        }
                    })
                    .then(move |upper_result| {
                        notify_tx.send(SupervisorNotify::Exited)
                            .then(move |_send_result| upper_result)
                    })
            });
        self.executor.spawn(future);
    }

    pub fn shutdown_on_idle(self, runtime: &mut tokio::runtime::Runtime) -> Result<(), SupervisorLoopGone> {
        let shutdown_future = self.shutdown_rx
            .then(|shutdown_result| {
                match shutdown_result {
                    Ok(Shutdown) =>
                        Ok(()),
                    Err(oneshot::Canceled) => {
                        error!("supervisor loop dropped its shutdown channel endpoint unexpectedly");
                        Err(SupervisorLoopGone)
                    }
                }
            });
        runtime.block_on(shutdown_future)
    }
}

fn supervisor_loop(
    notify_rx: mpsc::Receiver<SupervisorNotify>,
    shutdown_tx: oneshot::Sender<Shutdown>,
)
    -> impl Future<Item = (), Error = ()>
{
    debug!("supervisor loop started");

    enum Event {
        SpawnNotify(oneshot::Sender<Shutdown>),
        ExitNotify,
        Disconnected,
    }

    notify_rx
        .then(|event| {
            match event {
                Ok(SupervisorNotify::Spawned(notify)) =>
                    Ok(Event::SpawnNotify(notify)),
                Ok(SupervisorNotify::Exited) =>
                    Ok(Event::ExitNotify),
                Err(()) =>
                    Ok(Event::Disconnected)
            }
        })
        .fold(Vec::new(), |mut linked, event| {
            match event {
                Event::SpawnNotify(notify) => {
                    debug!("a process was spawned");
                    linked.push(notify);
                    Ok(linked)
                },
                Event::ExitNotify | Event::Disconnected =>
                    Err(linked),
            }
        })
        .then(|result| {
            let linked = match result {
                Ok(linked) => linked,
                Err(linked) => linked,
            };
            debug!("supervisor spawn notify endpoint disconnected: terminating {} linked processes", linked.len());

            for linked_tx in linked {
                if let Err(..) = linked_tx.send(Shutdown) {
                    debug!("supervised task is gone while performing shutdown");
                }
            }

            if let Err(..) = shutdown_tx.send(Shutdown) {
                debug!("supervisor shutdown channel endpoint dropped");
            }

            Ok(())
        })
}
