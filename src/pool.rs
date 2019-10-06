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

pub fn spawn_pool<MN, SN, MB, FMB, EMB, BMS, MS, C, FC, EC, SB, FSB, ESB, BSS, SS, H, FH, EH, MT, ST, I>(
    supervisor: &supervisor::Supervisor,
    master_params: Params<MN>,
    master_init_state: BMS,
    master_bootstrap: MB,
    master_converter: C,
    slaves_bootstrap: SB,
    slaves_handler: H,
    slaves_iter: I,
)
    -> mpsc::Sender<MT>
where MN: AsRef<str> + Send + 'static,
      SN: AsRef<str> + Send + 'static,
      MB: Fn(BMS) -> FMB + Send + 'static,
      FMB: IntoFuture<Item = MS, Error = ErrorSeverity<BMS, EMB>> + 'static,
      FMB::Future: Send,
      EMB: Debug + Send + 'static,
      BMS: Send + 'static,
      MS: Send + 'static,
      C: Fn(MT, MS) -> FC + Send + 'static,
      FC: IntoFuture<Item = (ST, MS), Error = ErrorSeverity<BMS, EC>> + 'static,
      FC::Future: Send,
      EC: Debug + Send + 'static,
      MT: Send + 'static,
      ST: Send + 'static,
      SB: Fn(BSS) -> FSB + Sync + Send + 'static,
      FSB: IntoFuture<Item = SS, Error = ErrorSeverity<BSS, ESB>> + 'static,
      FSB::Future: Send,
      ESB: Debug + Send + 'static,
      SS: Send + 'static,
      BSS: Send + 'static,
      H: Fn(ST, SS) -> FH + Sync + Send + 'static,
      FH: IntoFuture<Item = SS, Error = ErrorSeverity<BSS, EH>> + 'static,
      FH::Future: Send,
      EH: Debug + Send + 'static,
      I: IntoIterator<Item = (Params<SN>, BSS)>,
{
    let (tasks_tx, tasks_rx) = mpsc::channel(1);
    let (slaves_tx, slaves_rx) = mpsc::channel(1);

    supervisor.spawn_link(
        run_master(
            master_params,
            master_init_state,
            master_bootstrap,
            tasks_rx,
            slaves_rx,
            master_converter,
        ).map_err(|error| match error {
            MasterError::Bootstrap(error) =>
                error!("state bootstrap error in master: {:?}", error),
            MasterError::Converter(error) =>
                error!("tasks converter error in master: {:?}", error),
            MasterError::TasksChannelDropped =>
                error!("tasks channel dropped in master"),
            MasterError::TasksChannelDepleted =>
                info!("tasks channel depleted in master"),
            MasterError::SlavesChannelDropped =>
                error!("slaves channel dropped in master"),
            MasterError::SlavesChannelDepleted =>
                info!("slaves channel depleted in master"),
            MasterError::SlaveTaskChannelDropped =>
                error!("slave task channel dropped in master"),
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
pub enum MasterError<EB, EC> {
    Bootstrap(EB),
    Converter(EC),
    TasksChannelDropped,
    TasksChannelDepleted,
    SlavesChannelDropped,
    SlavesChannelDepleted,
    SlaveTaskChannelDropped,
    CrashForced,
}

pub fn run_master<N, B, FB, EB, BS, C, FC, EC, S, MT, ST>(
    params: Params<N>,
    init_state: BS,
    bootstrap: B,
    tasks_rx: mpsc::Receiver<MT>,
    slaves_rx: mpsc::Receiver<oneshot::Sender<ST>>,
    converter: C,
)
    -> impl Future<Item = (), Error = MasterError<EB, EC>>
where N: AsRef<str>,
      BS: Send + 'static,
      B: Fn(BS) -> FB + Send + 'static,
      FB: IntoFuture<Item = S, Error = ErrorSeverity<BS, EB>>,
      FB::Future: Send + 'static,
      EB: Send + 'static,
      C: Fn(MT, S) -> FC + Send + 'static,
      FC: IntoFuture<Item = (ST, S), Error = ErrorSeverity<BS, EC>>,
      FC::Future: Send + 'static,
      EC: Send + 'static,
      S: Send + 'static,
      MT: Send + 'static,
      ST: Send + 'static,
{
    struct RestartableState<BS, B, MT, ST, C> {
        state: BS,
        bootstrap: B,
        tasks_rx: mpsc::Receiver<MT>,
        slaves_rx: mpsc::Receiver<oneshot::Sender<ST>>,
        converter: C,
    }

    restart::restartable(
        params,
        RestartableState {
            state: init_state,
            bootstrap,
            tasks_rx,
            slaves_rx,
            converter,
        },
        |RestartableState { state: init_state, bootstrap, tasks_rx, slaves_rx, converter, }| {
            let bootstrapped: Box<dyn Future<Item = _, Error = _> + Send + 'static> =
                Box::new(
                    bootstrap(init_state)
                        .into_future()
                );
            bootstrapped
                .then(move |bootstrap_result| match bootstrap_result {
                    Ok(state) => {
                        enum Source<A, B> {
                            Task(A),
                            Slave(B),
                        }

                        struct State<S, B, ST, C> {
                            state: S,
                            bootstrap: B,
                            pending: Vec<ST>,
                            slaves: Vec<oneshot::Sender<ST>>,
                            converter: C,
                        }

                        let blender = Blender::new()
                            .add(tasks_rx)
                            .add(slaves_rx)
                            .finish_sources()
                            .fold(Source::Slave, Source::Slave)
                            .fold(Source::Task, Source::Task)
                            .finish();

                        let state = State {
                            state,
                            bootstrap,
                            pending: Vec::new(),
                            slaves: Vec::new(),
                            converter,
                        };

                        let future = loop_fn((blender, state), |(blender, mut state)| {
                            blender
                                .then(move |blend_result| match blend_result {
                                    Ok((Source::Task(master_task), blender)) => {
                                        let State { state, bootstrap, mut pending, mut slaves, converter, } = state;
                                        let future = converter(master_task, state)
                                            .into_future()
                                            .then(move |converter_result| match converter_result {
                                                Ok((task, state)) => match slaves.pop() {
                                                    Some(slave_tx) =>
                                                        match slave_tx.send(task) {
                                                            Ok(()) => {
                                                                let state = State { state, bootstrap, pending, slaves, converter, };
                                                                Ok(Loop::Continue((blender, state)))
                                                            },
                                                            Err(_task) =>
                                                                Err(ErrorSeverity::Fatal(MasterError::SlaveTaskChannelDropped)),
                                                        },
                                                    None => {
                                                        pending.push(task);
                                                        let state = State { state, bootstrap, pending, slaves, converter, };
                                                        Ok(Loop::Continue((blender, state)))
                                                    },
                                                },
                                                Err(ErrorSeverity::Fatal(error)) =>
                                                    Err(ErrorSeverity::Fatal(MasterError::Converter(error))),
                                                Err(ErrorSeverity::Recoverable { state, }) => {
                                                    let (tasks_rx, (slaves_rx, ())) = blender.decompose();
                                                    Err(ErrorSeverity::Recoverable {
                                                        state: RestartableState {
                                                            state, bootstrap, tasks_rx, slaves_rx, converter,
                                                        },
                                                    })
                                                },
                                            });
                                        Either::A(future)
                                    },
                                    Ok((Source::Slave(slave_online_tx), blender)) => {
                                        let future_result = match state.pending.pop() {
                                            Some(task) =>
                                                match slave_online_tx.send(task) {
                                                    Ok(()) =>
                                                        Ok(Loop::Continue((blender, state))),
                                                    Err(_task) =>
                                                        Err(ErrorSeverity::Fatal(MasterError::SlaveTaskChannelDropped)),
                                                },
                                            None => {
                                                state.slaves.push(slave_online_tx);
                                                Ok(Loop::Continue((blender, state)))
                                            },
                                        };
                                        Either::B(result(future_result))
                                    },
                                    Err(Source::Task(ErrorEvent::Depleted {
                                        decomposed: DecomposeZip { left_dir: (_slaves_rx, ()), myself: Gone, right_rev: (), },
                                    })) =>
                                        Either::B(result(Err(ErrorSeverity::Fatal(MasterError::TasksChannelDepleted)))),
                                    Err(Source::Slave(ErrorEvent::Depleted {
                                        decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_tasks_rx, ()), },
                                    })) =>
                                        Either::B(result(Err(ErrorSeverity::Fatal(MasterError::SlavesChannelDepleted)))),
                                    Err(Source::Task(ErrorEvent::Error {
                                        error: (),
                                        decomposed: DecomposeZip { left_dir: (_slaves_rx, ()), myself: Gone, right_rev: (), },
                                    })) =>
                                        Either::B(result(Err(ErrorSeverity::Fatal(MasterError::TasksChannelDropped)))),
                                    Err(Source::Slave(ErrorEvent::Error {
                                        error: (),
                                        decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_tasks_rx, ()), },
                                    })) =>
                                        Either::B(result(Err(ErrorSeverity::Fatal(MasterError::SlavesChannelDropped)))),
                                })
                        });
                        Box::new(future) as Box<dyn Future<Item = _, Error = _> + Send + 'static>
                    },
                    Err(ErrorSeverity::Fatal(error)) =>
                        Box::new(result(Err(ErrorSeverity::Fatal(MasterError::Bootstrap(error))))),
                    Err(ErrorSeverity::Recoverable { state, }) =>
                        Box::new(result(Err(ErrorSeverity::Recoverable {
                            state: RestartableState { state, bootstrap, tasks_rx, slaves_rx, converter, },
                        }))),
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

pub fn run_slave<N, B, EB, FB, BS, S, T, H, EH, FH>(
    params: Params<N>,
    init_state: BS,
    bootstrap: B,
    slaves_tx: mpsc::Sender<oneshot::Sender<T>>,
    handler: H,
)
    -> impl Future<Item = (), Error = SlaveError<EB, EH>>
where N: AsRef<str>,
      BS: Send + 'static,
      B: Fn(BS) -> FB + Send + 'static,
      FB: IntoFuture<Item = S, Error = ErrorSeverity<BS, EB>>,
      FB::Future: Send + 'static,
      EB: Send + 'static,
      H: Fn(T, S) -> FH + Send + 'static,
      FH: IntoFuture<Item = S, Error = ErrorSeverity<BS, EH>>,
      FH::Future: Send + 'static,
      EH: Send + 'static,
      S: Send + 'static,
      T: Send + 'static,
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
            let bootstrapped: Box<dyn Future<Item = _, Error = _> + Send + 'static> =
                Box::new(
                    bootstrap(init_state)
                        .into_future()
                );
            bootstrapped
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
                        Box::new(future) as Box<dyn Future<Item = _, Error = _> + Send + 'static>
                    },
                    Err(ErrorSeverity::Fatal(error)) =>
                        Box::new(result(Err(ErrorSeverity::Fatal(SlaveError::Bootstrap(error))))),
                    Err(ErrorSeverity::Recoverable { state, }) =>
                        Box::new(result(Err(ErrorSeverity::Recoverable {
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
