use std::{
    sync::Arc,
    fmt::Debug,
};

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
    info,
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

pub fn spawn_pool<MN, SN, B, FB, EB, S, H, FH, EH, M, MT, ST, I>(
    supervisor: &supervisor::Supervisor,
    master_params: Params<MN>,
    master_converter: M,
    slaves_bootstrap: B,
    slaves_handler: H,
    slaves_iter: I,
)
    -> mpsc::Sender<MT>
where MN: AsRef<str> + Send + 'static,
      SN: AsRef<str> + Send + 'static,
      M: Fn(MT) -> ST + Send + 'static,
      MT: Send + 'static,
      ST: Send + 'static,
      B: Fn(S) -> FB + Sync + Send + 'static,
      FB: IntoFuture<Item = S, Error = ErrorSeverity<S, EB>> + 'static,
      FB::Future: Send,
      EB: Debug + Send + 'static,
      S: Send + 'static,
      H: Fn(ST, S) -> FH + Sync + Send + 'static,
      FH: IntoFuture<Item = S, Error = ErrorSeverity<S, EH>> + 'static,
      FH::Future: Send,
      EH: Debug + Send + 'static,
      I: IntoIterator<Item = (Params<SN>, S)>,
{
    let (tasks_tx, tasks_rx) = mpsc::channel(1);
    let (slaves_tx, slaves_rx) = mpsc::channel(1);

    supervisor.spawn_link(
        run_master(
            master_params,
            tasks_rx,
            slaves_rx,
            master_converter,
        ).map_err(|error| match error {
            MasterError::TasksChannelDropped =>
                error!("tasks channel dropped in master"),
            MasterError::TasksChannelDepleted =>
                info!("tasks channel depleted in master"),
            MasterError::SlavesChannelDropped =>
                error!("slaves channel dropped in master"),
            MasterError::SlavesChannelDepleted =>
                info!("slaves channel depleted in master"),
            MasterError::CrashForced =>
                info!("crash forced after error in master"),
        })
    );

    let shared_bootstrap = Arc::new(slaves_bootstrap);
    let shared_handler = Arc::new(slaves_handler);

    for (params, init_state) in slaves_iter {
        let bootstrap = shared_bootstrap.clone();
        let handler = shared_handler.clone();
        supervisor.spawn_link(
            run_slave(
                params,
                init_state,
                move |state| bootstrap(state),
                slaves_tx.clone(),
                move |task, state| handler(task, state),
            ).map_err(|error| match error {
                SlaveError::Bootstrap(error) =>
                    error!("state bootstrap error in slave: {:?}", error),
                SlaveError::Handler(error) =>
                    error!("task handler error in slave: {:?}", error),
                SlaveError::MasterChannelDropped =>
                    error!("master channel dropped in slave"),
                SlaveError::TasksChannelDropped =>
                    error!("tasks channel dropped in slave"),
                SlaveError::CrashForced =>
                    info!("crash forced after error in slave"),
            }),
        );
    }

    tasks_tx
}

#[derive(Debug)]
pub enum MasterError {
    TasksChannelDropped,
    TasksChannelDepleted,
    SlavesChannelDropped,
    SlavesChannelDepleted,
    CrashForced,
}

pub fn run_master<N, MT, ST, M>(
    params: Params<N>,
    tasks_rx: mpsc::Receiver<MT>,
    slaves_rx: mpsc::Receiver<oneshot::Sender<ST>>,
    converter: M,
)
    -> impl Future<Item = (), Error = MasterError>
where N: AsRef<str>,
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
                        })) =>
                            Err(ErrorSeverity::Fatal(MasterError::TasksChannelDepleted)),
                        Err(Source::Slave(ErrorEvent::Depleted {
                            decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_tasks_rx, ()), },
                        })) =>
                            Err(ErrorSeverity::Fatal(MasterError::SlavesChannelDepleted)),
                        Err(Source::Task(ErrorEvent::Error {
                            error: (),
                            decomposed: DecomposeZip { left_dir: (_slaves_rx, ()), myself: Gone, right_rev: (), },
                        })) =>
                            Err(ErrorSeverity::Fatal(MasterError::TasksChannelDropped)),
                        Err(Source::Slave(ErrorEvent::Error {
                            error: (),
                            decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_tasks_rx, ()), },
                        })) =>
                            Err(ErrorSeverity::Fatal(MasterError::SlavesChannelDropped)),
                    })
            })
        })
        .map_err(|restartable_error| match restartable_error {
            restart::RestartableError::Fatal(error) =>
                error,
            restart::RestartableError::RestartCrashForced =>
                MasterError::CrashForced,
        })
}

#[derive(Debug)]
pub enum SlaveError<EB, EH> {
    Bootstrap(EB),
    Handler(EH),
    MasterChannelDropped,
    TasksChannelDropped,
    CrashForced,
}

pub fn run_slave<N, B, EB, FB, S, T, H, EH, FH>(
    params: Params<N>,
    init_state: S,
    bootstrap: B,
    slaves_tx: mpsc::Sender<oneshot::Sender<T>>,
    handler: H,
)
    -> impl Future<Item = (), Error = SlaveError<EB, EH>>
where N: AsRef<str>,
      B: Fn(S) -> FB,
      FB: IntoFuture<Item = S, Error = ErrorSeverity<S, EB>>,
      H: Fn(T, S) -> FH,
      FH: IntoFuture<Item = S, Error = ErrorSeverity<S, EH>>,
{
    struct State<S, B, T, H> {
        state: S,
        bootstrap: B,
        slaves_tx: mpsc::Sender<oneshot::Sender<T>>,
        handler: H,
    }

    restart::restartable(
        params,
        State {
            state: init_state,
            bootstrap,
            slaves_tx,
            handler,
        },
        |State { state: init_state, bootstrap, slaves_tx, handler }| {
            bootstrap(init_state)
                .into_future()
                .then(move |bootstrap_result| match bootstrap_result {
                    Ok(state) => {
                        let future = loop_fn(State {state, bootstrap, slaves_tx, handler, }, |State { state, bootstrap, slaves_tx, handler, }| {
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
                                                        .then(move |handler_result| match handler_result {
                                                            Ok(state) =>
                                                                Ok(Loop::Continue(State { state, bootstrap, slaves_tx, handler, })),
                                                            Err(ErrorSeverity::Fatal(error)) =>
                                                                Err(ErrorSeverity::Fatal(SlaveError::Handler(error))),
                                                            Err(ErrorSeverity::Recoverable { state, }) =>
                                                                Err(ErrorSeverity::Recoverable {
                                                                    state: State { state, bootstrap, slaves_tx, handler, },
                                                                }),
                                                        });
                                                    Either::A(future)
                                                },
                                                Err(oneshot::Canceled) =>
                                                    Either::B(result(Err(ErrorSeverity::Fatal(SlaveError::TasksChannelDropped)))),
                                            });
                                        Either::A(future)
                                    },
                                    Err(_send_error) =>
                                        Either::B(result(Err(ErrorSeverity::Fatal(SlaveError::MasterChannelDropped)))),
                                })
                        });
                        Either::A(future)
                    },
                    Err(ErrorSeverity::Fatal(error)) =>
                        Either::B(result(Err(ErrorSeverity::Fatal(SlaveError::Bootstrap(error))))),
                    Err(ErrorSeverity::Recoverable { state, }) =>
                        Either::B(result(Err(ErrorSeverity::Recoverable {
                            state: State { state, bootstrap, slaves_tx, handler, },
                        }))),
                })
        })
        .map_err(|restartable_error| match restartable_error {
            restart::RestartableError::Fatal(error) =>
                error,
            restart::RestartableError::RestartCrashForced =>
                SlaveError::CrashForced,
        })
}
