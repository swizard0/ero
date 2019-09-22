use std::sync::Arc;

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

use super::{
    supervisor,
    restart,
    blend::{
        Gone,
        Blender,
        Decompose,
        ErrorEvent,
        DecomposeZip,
    },
    Params,
    ErrorSeverity,
};

// pub fn run_pool<MN, I, SN, S, M, H, F>(
//     supervisor: &supervisor::Supervisor,
//     master_name: MN,
//     slaves_iter: I,
//     handler: H,
// )
//     -> mpsc::Sender<M>
// where MN: AsRef<str> + Send + 'static,
//       SN: AsRef<str> + Send + 'static,
//       I: IntoIterator<Item = (SN, S)>,
//       S: Send + 'static,
//       M: Send + 'static,
//       H: Fn(M, S) -> F + Send + Sync + 'static,
//       F: IntoFuture<Item = S, Error = ()> + Send + 'static,
//       F::Future: Send,
// {
//     let (tasks_tx, tasks_rx) = mpsc::channel(1);
//     let (slaves_tx, slaves_rx) = mpsc::channel(1);

//     supervisor.spawn_link(run_master(master_name, tasks_rx, slaves_rx));

//     let shared_handler = Arc::new(handler);
//     for (slave_name, slave_state) in slaves_iter {
//         let handler = shared_handler.clone();
//         supervisor.spawn_link(
//             run_slave(
//                 slave_name,
//                 slave_state,
//                 slaves_tx.clone(),
//                 move |task, state| handler(task, state),
//             )
//         );
//     }

//     tasks_tx
// }

pub fn run_master<N, MT, ST, M>(
    params: Params<N>,
    tasks_rx: mpsc::Receiver<MT>,
    slaves_rx: mpsc::Receiver<oneshot::Sender<ST>>,
    converter: M,
)
    -> impl Future<Item = (), Error = ()>
where N: AsRef<str> + Clone,
      M: Fn(MT) -> ST,
{
    struct RestartableState<MT, ST, M> {
        tasks_rx: mpsc::Receiver<MT>,
        slaves_rx: mpsc::Receiver<oneshot::Sender<ST>>,
        converter: M,
    }

    restart::restartable(
        params,
        RestartableState {
            tasks_rx,
            slaves_rx,
            converter,
        },
        |RestartableState { tasks_rx, slaves_rx, converter, }| {
            enum Source<A, B> {
                Task(A),
                Slave(B),
            }

            struct State<ST, M> {
                pending: Vec<ST>,
                slaves: Vec<oneshot::Sender<ST>>,
                converter: M,
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
                converter,
            };

            loop_fn((blender, state), |(blender, mut state)| {
                blender
                    .then(move |blend_result| match blend_result {
                        Ok((Source::Task(master_task), blender)) => {
                            let task = (state.converter)(master_task);
                            match state.slaves.pop() {
                                Some(slave_tx) =>
                                    match slave_tx.send(task) {
                                        Ok(()) =>
                                            Ok(Loop::Continue((blender, state))),
                                        Err(_task) => {
                                            let (tasks_rx, (slaves_rx, ())) = blender.decompose();
                                            Err(ErrorSeverity::Recoverable {
                                                state: RestartableState {
                                                    tasks_rx,
                                                    slaves_rx,
                                                    converter: state.converter,
                                                },
                                            })
                                        }
                                    },
                                None => {
                                    state.pending.push(task);
                                    Ok(Loop::Continue((blender, state)))
                                },
                            }
                        },
                        Ok((Source::Slave(slave_online_tx), blender)) => {
                            match state.pending.pop() {
                                Some(task) =>
                                    match slave_online_tx.send(task) {
                                        Ok(()) =>
                                            Ok(Loop::Continue((blender, state))),
                                        Err(_task) => {
                                            let (tasks_rx, (slaves_rx, ())) = blender.decompose();
                                            Err(ErrorSeverity::Recoverable {
                                                state: RestartableState {
                                                    tasks_rx,
                                                    slaves_rx,
                                                    converter: state.converter,
                                                },
                                            })
                                        }
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
                            error!("unexpected tasks stream termination in master: shutting down");
                            Err(ErrorSeverity::Fatal(()))
                        },
                        Err(Source::Slave(ErrorEvent::Depleted {
                            decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_tasks_rx, ()), },
                        })) => {
                            error!("unexpected slaves stream termination in master: shutting down");
                            Err(ErrorSeverity::Fatal(()))
                        },
                        Err(Source::Task(ErrorEvent::Error {
                            error: (),
                            decomposed: DecomposeZip { left_dir: (_slaves_rx, ()), myself: Gone, right_rev: (), },
                        })) => {
                            error!("unexpected tasks stream drop in master: shutting down");
                            Err(ErrorSeverity::Fatal(()))
                        },
                        Err(Source::Slave(ErrorEvent::Error {
                            error: (),
                            decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_tasks_rx, ()), },
                        })) => {
                            error!("unexpected slaves stream drop in master: shutting down");
                            Err(ErrorSeverity::Fatal(()))
                        },
                    })
            })
        })
        .map_err(|_restartable_error| ())
}

// pub fn run_slave<N, M, S, H, F>(
//     name: N,
//     init_state: S,
//     slaves_tx: mpsc::Sender<oneshot::Sender<M>>,
//     handler: H,
// )
//     -> impl Future<Item = (), Error = ()>
// where N: AsRef<str>,
//       H: Fn(M, S) -> F,
//       F: IntoFuture<Item = S, Error = ()>,
// {
//     loop_fn((name, slaves_tx, init_state, handler), |(name, slaves_tx, state, handler)| {
//         let (task_tx, task_rx) = oneshot::channel();
//         slaves_tx
//             .send(task_tx)
//             .then(move |send_result| match send_result {
//                 Ok(slaves_tx) => {
//                     let future = task_rx
//                         .then(move |recv_result| match recv_result {
//                             Ok(task) => {
//                                 let future = handler(task, state)
//                                     .into_future()
//                                     .map(move |state| {
//                                         Loop::Continue((name, slaves_tx, state, handler))
//                                     });
//                                 Either::A(future)
//                             },
//                             Err(oneshot::Canceled) => {
//                                 warn!("broken message channel in slave {}", name.as_ref());
//                                 Either::B(result(Err(())))
//                             },
//                         });
//                     Either::A(future)
//                 },
//                 Err(_send_error) => {
//                     warn!("unexpected master stream disconnect in slave {}, terminating", name.as_ref());
//                     Either::B(result(Err(())))
//                 },
//             })
//     })
// }
