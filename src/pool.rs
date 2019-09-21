use futures::{
    future::{
        result,
        loop_fn,
        Loop,
        Future,
        Either,
        IntoFuture,
    },
    sync::{
        mpsc,
        oneshot,
    },
    Sink,
};

use log::{
    warn,
    error,
};

use super::blend::{
    Gone,
    Blender,
    ErrorEvent,
    DecomposeZip,
};

pub fn run_master<N, M>(
    name: N,
    tasks_rx: mpsc::Receiver<M>,
    slaves_rx: mpsc::Receiver<oneshot::Sender<M>>,
)
    -> impl Future<Item = (), Error = ()>
where N: AsRef<str>
{
    enum Source<A, B> {
        Task(A),
        Slave(B),
    }

    struct State<N, M> {
        pending: Vec<M>,
        slaves: Vec<oneshot::Sender<M>>,
        name: N,
    }

    let blender = Blender::new()
        .add(tasks_rx)
        .add(slaves_rx)
        .finish_sources()
        .fold(Source::Slave, Source::Slave)
        .fold(Source::Task, Source::Task)
        .finish();

    let state = State {
        pending: Vec::new(),
        slaves: Vec::new(),
        name,
    };

    loop_fn((blender, state), |(blender, mut state)| {
        blender
            .then(move |result| match result {
                Ok((Source::Task(task), blender)) => {
                    match state.slaves.pop() {
                        Some(slave_tx) => {
                            slave_tx.send(task)
                                .map_err(|_task| {
                                    warn!("slave disconnect #0 detected in master {}: shutting down", state.name.as_ref());
                                })
                                .map(|()| Loop::Continue((blender, state)))
                        },
                        None => {
                            state.pending.push(task);
                            Ok(Loop::Continue((blender, state)))
                        },
                    }
                },
                Ok((Source::Slave(slave_online_tx), blender)) => {
                    match state.pending.pop() {
                        Some(task) => {
                            slave_online_tx.send(task)
                                .map_err(|_task| {
                                    warn!("slave disconnect #1 detected in master {}: shutting down", state.name.as_ref());
                                })
                                .map(|()| Loop::Continue((blender, state)))
                        },
                        None => {
                            state.slaves.push(slave_online_tx);
                            Ok(Loop::Continue((blender, state)))
                        },
                    }
                },
                Err(Source::Task(ErrorEvent::Depleted {
                    decomposed: DecomposeZip { left_dir: (_slaves_rx, ()), myself: Gone, right_rev: (), },
                })) => {
                    error!("unexpected tasks stream termination in master {}: shutting down", state.name.as_ref());
                    Err(())
                },
                Err(Source::Slave(ErrorEvent::Depleted {
                    decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_tasks_rx, ()), },
                })) => {
                    error!("unexpected slaves stream termination in master {}: shutting down", state.name.as_ref());
                    Err(())
                },
                Err(Source::Task(ErrorEvent::Error {
                    error: (),
                    decomposed: DecomposeZip { left_dir: (_slaves_rx, ()), myself: Gone, right_rev: (), },
                })) => {
                    error!("unexpected tasks stream drop in master {}: shutting down", state.name.as_ref());
                    Err(())
                },
                Err(Source::Slave(ErrorEvent::Error {
                    error: (),
                    decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_tasks_rx, ()), },
                })) => {
                    error!("unexpected slaves stream drop in master {}: shutting down", state.name.as_ref());
                    Err(())
                },
            })
    })
}

pub fn run_slave<N, M, S, H, F>(
    name: N,
    init_state: S,
    slaves_tx: mpsc::Sender<oneshot::Sender<M>>,
    handler: H,
)
    -> impl Future<Item = (), Error = ()>
where N: AsRef<str>,
      H: FnMut(M, S) -> F,
      F: IntoFuture<Item = S, Error = ()>,
{
    loop_fn((name, slaves_tx, init_state, handler), |(name, slaves_tx, state, mut handler)| {
        let (task_tx, task_rx) = oneshot::channel();
        slaves_tx
            .send(task_tx)
            .then(move |send_result| match send_result {
                Ok(slaves_tx) => {
                    let future = task_rx
                        .then(move |recv_result| match recv_result {
                            Ok(task) => {
                                let future = handler(task, state)
                                    .into_future()
                                    .map(move |state| {
                                        Loop::Continue((name, slaves_tx, state, handler))
                                    });
                                Either::A(future)
                            },
                            Err(oneshot::Canceled) => {
                                warn!("broken message channel in slave {}", name.as_ref());
                                Either::B(result(Err(())))
                            },
                        });
                    Either::A(future)
                },
                Err(_send_error) => {
                    warn!("unexpected master stream disconnect in slave {}, terminating", name.as_ref());
                    Either::B(result(Err(())))
                },
            })
    })
}
