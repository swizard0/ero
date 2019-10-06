use std::{
    mem,
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
    Poll,
    Async,
    AsyncSink,
};

use log::{
    debug,
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
      B: Fn(BS) -> FB,
      FB: IntoFuture<Item = S, Error = ErrorSeverity<BS, EB>>,
      C: Fn(MT, S) -> FC,
      FC: IntoFuture<Item = (ST, S), Error = ErrorSeverity<BS, EC>>,
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
            bootstrap(init_state)
                .into_future()
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
                        Either::A(future)
                    },
                    Err(ErrorSeverity::Fatal(error)) =>
                        Either::B(result(Err(ErrorSeverity::Fatal(MasterError::Bootstrap(error))))),
                    Err(ErrorSeverity::Recoverable { state, }) =>
                        Either::B(result(Err(ErrorSeverity::Recoverable {
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
      B: Fn(BS) -> FB,
      FB: IntoFuture<Item = S, Error = ErrorSeverity<BS, EB>>,
      H: Fn(T, S) -> FH,
      FH: IntoFuture<Item = S, Error = ErrorSeverity<BS, EH>>,
{
    restart::restartable(
        params,
        SlaveState { init_state, bootstrap, slaves_tx, handler, },
        |state| SlaveFuture {
            mode: SlaveMode::Bootstrap {
                ctx: SlaveContext {
                    bootstrap: state.bootstrap,
                    handler: state.handler,
                    slaves_tx: state.slaves_tx,
                },
                init_state: state.init_state,
            },
        })
        .map_err(|restartable_error| match restartable_error {
            restart::RestartableError::Fatal(error) =>
                error,
            restart::RestartableError::RestartCrashForced =>
                SlaveError::CrashForced,
        })
}

struct SlaveState<S, B, T, H> {
    init_state: S,
    bootstrap: B,
    slaves_tx: mpsc::Sender<oneshot::Sender<T>>,
    handler: H,
}

struct SlaveFuture<BS, S, B, FFB, T, H, FFH> {
    mode: SlaveMode<BS, S, B, FFB, T, H, FFH>,
}

struct SlaveContext<B, H, T> {
    bootstrap: B,
    handler: H,
    slaves_tx: mpsc::Sender<oneshot::Sender<T>>,
}

enum SlaveMode<BS, S, B, FFB, T, H, FFH> {
    Invalid,
    Bootstrap { ctx: SlaveContext<B, H, T>, init_state: BS, },
    BootstrapWait { ctx: SlaveContext<B, H, T>, future: FFB, },
    SlaveStartSend {
        ctx: SlaveContext<B, H, T>,
        state: S,
        task_tx: oneshot::Sender<T>,
        task_rx: oneshot::Receiver<T>,
    },
    SlavePollSend {
        ctx: SlaveContext<B, H, T>,
        state: S,
        task_rx: oneshot::Receiver<T>,
    },
    TaskRecv {
        ctx: SlaveContext<B, H, T>,
        state: S,
        task_rx: oneshot::Receiver<T>,
    },
    HandlerWait { ctx: SlaveContext<B, H, T>, future: FFH, },
}

impl<BS, S, B, FB, EB, T, H, FH, EH> Future for SlaveFuture<BS, S, B, FB::Future, T, H, FH::Future>
where B: Fn(BS) -> FB,
      FB: IntoFuture<Item = S, Error = ErrorSeverity<BS, EB>>,
      H: Fn(T, S) -> FH,
      FH: IntoFuture<Item = S, Error = ErrorSeverity<BS, EH>>,
{
    type Item = ();
    type Error = ErrorSeverity<SlaveState<BS, B, T, H>, SlaveError<EB, EH>>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match mem::replace(&mut self.mode, SlaveMode::Invalid) {
                SlaveMode::Invalid =>
                    panic!("cannot poll SlaveFuture twice"),

                SlaveMode::Bootstrap { ctx, init_state, } => {
                    debug!("SlaveMode::Bootstrap");
                    let future = (ctx.bootstrap)(init_state)
                        .into_future();
                    self.mode = SlaveMode::BootstrapWait { ctx, future, };
                },

                SlaveMode::BootstrapWait { ctx, mut future, } => {
                    debug!("SlaveMode::BootstrapWait");
                    match future.poll() {
                        Ok(Async::NotReady) => {
                            self.mode = SlaveMode::BootstrapWait { ctx, future, };
                            return Ok(Async::NotReady);
                        },
                        Ok(Async::Ready(state)) => {
                            let (task_tx, task_rx) = oneshot::channel();
                            self.mode = SlaveMode::SlaveStartSend { ctx, state, task_tx, task_rx, };
                        },
                        Err(ErrorSeverity::Recoverable { state, }) =>
                            return Err(ErrorSeverity::Recoverable {
                                state: SlaveState {
                                    init_state: state,
                                    bootstrap: ctx.bootstrap,
                                    slaves_tx: ctx.slaves_tx,
                                    handler: ctx.handler,
                                },
                            }),
                        Err(ErrorSeverity::Fatal(error)) =>
                            return Err(ErrorSeverity::Fatal(SlaveError::Bootstrap(error))),
                    }
                },

                SlaveMode::SlaveStartSend { mut ctx, state, task_tx, task_rx, } => {
                    debug!("SlaveMode::SlaveStartSend");
                    match ctx.slaves_tx.start_send(task_tx) {
                        Ok(AsyncSink::NotReady(task_tx)) => {
                            self.mode = SlaveMode::SlaveStartSend { ctx, state, task_tx, task_rx, };
                            return Ok(Async::NotReady);
                        },
                        Ok(AsyncSink::Ready) =>
                            self.mode = SlaveMode::SlavePollSend { ctx, state, task_rx, },
                        Err(_send_error) =>
                            return Err(ErrorSeverity::Fatal(SlaveError::MasterChannelDropped)),
                    }
                },

                SlaveMode::SlavePollSend { mut ctx, state, task_rx, } => {
                    debug!("SlaveMode::SlavePollSend");
                    match ctx.slaves_tx.poll_complete() {
                        Ok(Async::NotReady) => {
                            self.mode = SlaveMode::SlavePollSend { ctx, state, task_rx, };
                            return Ok(Async::NotReady);
                        },
                        Ok(Async::Ready(())) =>
                            self.mode = SlaveMode::TaskRecv { ctx, state, task_rx, },
                        Err(_poll_error) =>
                            return Err(ErrorSeverity::Fatal(SlaveError::MasterChannelDropped)),
                    }
                },

                SlaveMode::TaskRecv { ctx, state, mut task_rx, } => {
                    debug!("SlaveMode::TaskRecv");
                    match task_rx.poll() {
                        Ok(Async::NotReady) => {
                            self.mode = SlaveMode::TaskRecv { ctx, state, task_rx, };
                            return Ok(Async::NotReady);
                        },
                        Ok(Async::Ready(task)) => {
                            let future = (ctx.handler)(task, state)
                                .into_future();
                            self.mode = SlaveMode::HandlerWait { ctx, future, };
                        },
                        Err(oneshot::Canceled) =>
                            return Err(ErrorSeverity::Fatal(SlaveError::TasksChannelDropped)),
                    }
                },

                SlaveMode::HandlerWait { ctx, mut future, } => {
                    debug!("SlaveMode::HandlerWait");
                    match future.poll() {
                        Ok(Async::NotReady) => {
                            self.mode = SlaveMode::HandlerWait { ctx, future, };
                            return Ok(Async::NotReady);
                        },
                        Ok(Async::Ready(state)) => {
                            let (task_tx, task_rx) = oneshot::channel();
                            self.mode = SlaveMode::SlaveStartSend { ctx, state, task_tx, task_rx, };
                        },
                        Err(ErrorSeverity::Recoverable { state, }) =>
                            return Err(ErrorSeverity::Recoverable {
                                state: SlaveState {
                                    init_state: state,
                                    bootstrap: ctx.bootstrap,
                                    slaves_tx: ctx.slaves_tx,
                                    handler: ctx.handler,
                                },
                            }),
                        Err(ErrorSeverity::Fatal(error)) =>
                            return Err(ErrorSeverity::Fatal(SlaveError::Handler(error))),
                    }
                },
            }
        }
    }
}
