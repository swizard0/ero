use std::{
    time::{
        Instant,
    },
};

use futures::{
    future::{
        loop_fn,
        result,
        Either,
    },
    Sink,
    stream,
    Future,
    Stream,
    IntoFuture,
    sync::{
        mpsc,
        oneshot,
    },
};

use log::{
    debug,
    info,
    warn,
    error,
};

use tokio::timer::Delay;

use super::{
    Loop,
    ErrorSeverity,
    RestartStrategy,
};

struct AquireReq<R> {
    reply_tx: oneshot::Sender<R>,
}

enum ReleaseReq<R> {
    Reimburse(R),
    ResourceLost,
    ResourceFault,
}

struct Shutdown;

pub struct Lode<R> {
    aquire_tx: mpsc::Sender<AquireReq<R>>,
    release_tx: mpsc::UnboundedSender<ReleaseReq<R>>,
    shutdown_tx: oneshot::Sender<Shutdown>,
}

pub enum Resource<P, Q> {
    Available(P),
    OutOfStock(Q),
}

pub struct Params<N> {
    name: N,
    restart_strategy: RestartStrategy,
}

pub fn spawn<FNI, FI, FNA, FA, FNR, FR, FNC, FC, N, S, R, P, Q>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
    init_state: S,
    init_fn: FNI,
    aquire_fn: FNA,
    release_fn: FNR,
    close_fn: FNC,
)
    -> Lode<R>
where FNI: FnMut(S) -> FI + Send + 'static,
      FI: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
      FI::Future: Send,
      FNA: FnMut(P) -> FA + Send + 'static,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>> + 'static,
      FA::Future: Send,
      FNR: FnMut(Resource<P, Q>, Option<R>) -> FR + Send + 'static,
      FR: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
      FR::Future: Send,
      FNC: FnMut(Resource<P, Q>) -> FC + Send + 'static,
      FC: IntoFuture<Item = S, Error = ()> + Send + 'static,
      FC::Future: Send,
      N: AsRef<str> + Send + 'static,
      S: Send + 'static,
      R: Send + 'static,
      P: Send + 'static,
      Q: Send + 'static,
{
    let (aquire_tx_stream, aquire_rx_stream) = mpsc::channel(0);
    let (release_tx_stream, release_rx_stream) = mpsc::unbounded();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    executor.spawn(
        loop_fn(
            RestartState {
                core: Core {
                    aquire_rx: aquire_rx_stream.into_future(),
                    release_rx: release_rx_stream.into_future(),
                    aquires_count: 0,
                    shutdown_rx,
                    params,
                    aquire_fn,
                    release_fn,
                    close_fn,
                },
                aquire_req_pending: None,
                init_state,
                init_fn,
            },
            restart_loop,
        )
    );

    Lode {
        aquire_tx: aquire_tx_stream,
        release_tx: release_tx_stream,
        shutdown_tx,
    }
}

struct Core<FNA, FNR, FNC, N, R> {
    aquire_rx: stream::StreamFuture<mpsc::Receiver<AquireReq<R>>>,
    release_rx: stream::StreamFuture<mpsc::UnboundedReceiver<ReleaseReq<R>>>,
    shutdown_rx: oneshot::Receiver<Shutdown>,
    params: Params<N>,
    aquire_fn: FNA,
    release_fn: FNR,
    close_fn: FNC,
    aquires_count: usize,
}

struct RestartState<FNI, FNA, FNR, FNC, N, S, R> {
    core: Core<FNA, FNR, FNC, N, R>,
    aquire_req_pending: Option<AquireReq<R>>,
    init_state: S,
    init_fn: FNI,
}

fn restart_loop<FNI, FS, FNA, FA, FNR, FR, FNC, FC, N, S, R, P, Q>(
    restart_state: RestartState<FNI, FNA, FNR, FNC, N, S, R>,
)
    -> impl Future<Item = Loop<(), RestartState<FNI, FNA, FNR, FNC, N, S, R>>, Error = ()>
where FNI: FnMut(S) -> FS + Send + 'static,
      FS: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
      FNA: FnMut(P) -> FA,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>>,
      FNR: FnMut(Resource<P, Q>, Option<R>) -> FR,
      FR: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
      FNC: FnMut(Resource<P, Q>) -> FC,
      FC: IntoFuture<Item = S, Error = ()>,
      N: AsRef<str>,
{
    let RestartState { core, init_state, mut init_fn, mut aquire_req_pending, } = restart_state;
    let future = if let Some(aquire_req) = aquire_req_pending.take() {
        Either::A(result(Ok(AquireWait::Arrived { aquire_req, core, })))
    } else {
        Either::B(wait_for_aquire(core))
    };
    future
        .and_then(move |aquire_wait| {
            match aquire_wait {
                AquireWait::Arrived { aquire_req, mut core, } => {
                    let future = init_fn(init_state)
                        .into_future()
                        .then(move |maybe_lode_state| {
                            match maybe_lode_state {
                                Ok(Resource::Available(state_avail)) => {
                                    let future =
                                        loop_fn(OuterState { core, state_avail, aquire_req_pending: Some(aquire_req), }, outer_loop)
                                        .then(move |inner_loop_result| {
                                            match inner_loop_result {
                                                Ok(BreakOuter::Shutdown) =>
                                                    Either::A(result(Ok(Loop::Break(())))),
                                                Ok(BreakOuter::RequireRestart { core, init_state, aquire_req_pending, }) => {
                                                    let future = proceed_with_restart(RestartState {
                                                        core, init_state, init_fn, aquire_req_pending,
                                                    });
                                                    Either::B(Either::A(future))
                                                },
                                                Err(()) =>
                                                    Either::B(Either::B(result(Err(())))),
                                            }
                                        });
                                    Either::A(Either::A(future))
                                },
                                Ok(Resource::OutOfStock(state_no_left)) => {
                                    warn!("initializing gives no resource in {}", core.params.name.as_ref());
                                    let future = (core.close_fn)(Resource::OutOfStock(state_no_left))
                                        .into_future()
                                        .and_then(move |init_state| {
                                            proceed_with_restart(RestartState {
                                                aquire_req_pending: Some(aquire_req),
                                                core, init_state, init_fn,
                                            })
                                        });
                                    Either::A(Either::B(future))
                                },
                                Err(ErrorSeverity::Recoverable { state: init_state, }) => {
                                    let future = proceed_with_restart(RestartState {
                                        aquire_req_pending: Some(aquire_req),
                                        core, init_state, init_fn,
                                    });
                                    Either::B(Either::A(future))
                                },
                                Err(ErrorSeverity::Fatal(())) => {
                                    error!("{} crashed with fatal error, terminating", core.params.name.as_ref());
                                    Either::B(Either::B(result(Err(()))))
                                },
                            }
                        });
                    Either::A(future)
                },
                AquireWait::Shutdown =>
                    Either::B(result(Ok(Loop::Break(())))),
            }
        })
}

enum AquireWait<FNA, FNR, FNC, N, R> {
    Arrived { aquire_req: AquireReq<R>, core: Core<FNA, FNR, FNC, N, R>, },
    Shutdown,
}

fn wait_for_aquire<FNA, FNR, FNC, N, R>(
    core: Core<FNA, FNR, FNC, N, R>
)
    -> impl Future<Item = AquireWait<FNA, FNR, FNC, N, R>, Error = ()>
{
    let Core { aquire_rx, release_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn, aquires_count, } = core;
    aquire_rx
        .select2(shutdown_rx)
        .then(move |await_result| {
            match await_result {
                Ok(Either::A(((Some(aquire_req), aquire_rx_stream), shutdown_rx))) => {
                    debug!("wait_for_aquire: aquire request");
                    Ok(AquireWait::Arrived {
                        aquire_req,
                        core: Core {
                            aquire_rx: aquire_rx_stream.into_future(),
                            release_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn, aquires_count,
                        },
                    })
                },
                Ok(Either::A(((None, _aquire_rx_stream), _shutdown_rx))) => {
                    debug!("wait_for_aquire: release channel depleted");
                    Ok(AquireWait::Shutdown)
                },
                Ok(Either::B((Shutdown, _aquire_rx))) => {
                    debug!("wait_for_aquire: shutdown request");
                    Ok(AquireWait::Shutdown)
                },
                Err(Either::A((((), _aquire_rx_stream), _shutdown_rx))) => {
                    debug!("wait_for_aquire: aquire channel outer endpoint dropped");
                    Ok(AquireWait::Shutdown)
                },
                Err(Either::B((oneshot::Canceled, _aquire_rx))) => {
                    debug!("wait_for_aquire: shutdown channel outer endpoint dropped");
                    Ok(AquireWait::Shutdown)
                },
            }
        })
}

enum BreakOuter<FNA, FNR, FNC, N, S, R> {
    Shutdown,
    RequireRestart {
        core: Core<FNA, FNR, FNC, N, R>,
        init_state: S,
        aquire_req_pending: Option<AquireReq<R>>,
    },
}

struct OuterState<FNA, FNR, FNC, N, R, P> {
    core: Core<FNA, FNR, FNC, N, R>,
    state_avail: P,
    aquire_req_pending: Option<AquireReq<R>>,
}

fn outer_loop<FNA, FA, FNR, FR, FNC, FC, N, S, R, P, Q>(
    outer_state: OuterState<FNA, FNR, FNC, N, R, P>,
)
    -> impl Future<Item = Loop<BreakOuter<FNA, FNR, FNC, N, S, R>, OuterState<FNA, FNR, FNC, N, R, P>>, Error = ()>
where FNA: FnMut(P) -> FA,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>>,
      FNR: FnMut(Resource<P, Q>, Option<R>) -> FR,
      FR: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
      FNC: FnMut(Resource<P, Q>) -> FC,
      FC: IntoFuture<Item = S, Error = ()>,
      N: AsRef<str>,
{
    let OuterState { core, state_avail, aquire_req_pending, } = outer_state;

    loop_fn(MainState { core, state_avail, aquire_req_pending, }, main_loop)
        .and_then(move |main_loop_result| {
            match main_loop_result {
                BreakMain::Shutdown =>
                    Either::A(result(Ok(Loop::Break(BreakOuter::Shutdown)))),
                BreakMain::RequireRestart { core, init_state, aquire_req_pending, } =>
                    Either::A(result(Ok(Loop::Break(BreakOuter::RequireRestart { core, init_state, aquire_req_pending, })))),
                BreakMain::WaitRelease { core, state_no_left, } => {
                    let future = loop_fn(WaitState { core, state_no_left, }, wait_loop)
                        .map(move |wait_loop_result| {
                            match wait_loop_result {
                                BreakWait::Shutdown =>
                                    Loop::Break(BreakOuter::Shutdown),
                                BreakWait::RequireRestart { core, init_state, } =>
                                    Loop::Break(BreakOuter::RequireRestart { core, init_state, aquire_req_pending: None, }),
                                BreakWait::ProceedMain { core, state_avail, } =>
                                    Loop::Continue(OuterState { core, state_avail, aquire_req_pending: None, }),
                            }
                        });
                    Either::B(future)
                },
            }
        })
}

enum BreakMain<FNA, FNR, FNC, N, S, R, Q> {
    Shutdown,
    RequireRestart {
        core: Core<FNA, FNR, FNC, N, R>,
        init_state: S,
        aquire_req_pending: Option<AquireReq<R>>,
    },
    WaitRelease {
        core: Core<FNA, FNR, FNC, N, R>,
        state_no_left: Q,
    },
}

struct MainState<FNA, FNR, FNC, N, R, P> {
    core: Core<FNA, FNR, FNC, N, R>,
    state_avail: P,
    aquire_req_pending: Option<AquireReq<R>>,
}

fn main_loop<FNA, FA, FNR, FR, FNC, FC, N, S, R, P, Q>(
    main_state: MainState<FNA, FNR, FNC, N, R, P>,
)
    -> impl Future<Item = Loop<BreakMain<FNA, FNR, FNC, N, S, R, Q>, MainState<FNA, FNR, FNC, N, R, P>>, Error = ()>
where FNA: FnMut(P) -> FA,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>>,
      FNR: FnMut(Resource<P, Q>, Option<R>) -> FR,
      FR: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
      FNC: FnMut(Resource<P, Q>) -> FC,
      FC: IntoFuture<Item = S, Error = ()>,
      N: AsRef<str>,
{
    let MainState {
        core: Core { aquire_rx, release_rx, shutdown_rx, params, mut aquire_fn, release_fn, close_fn, aquires_count, },
        state_avail,
        aquire_req_pending,
    } = main_state;

    enum AquireProcess<FNA, FNR, FNC, N, S, R, P, Q> {
        Proceed { core: Core<FNA, FNR, FNC, N, R>, state_avail: P, },
        WaitRelease { core: Core<FNA, FNR, FNC, N, R>, state_no_left: Q, },
        RequireRestart {
            core: Core<FNA, FNR, FNC, N, R>,
            init_state: S,
            aquire_req_pending: Option<AquireReq<R>>,
        }
    }

    let future = if let Some(AquireReq { reply_tx, }) = aquire_req_pending {
        debug!("main_loop: process the aquire request");
        let future = aquire_fn(state_avail)
            .into_future()
            .then(move |aquire_result| {
                match aquire_result {
                    Ok((resource, resource_status)) => {
                        let aquires_count = match reply_tx.send(resource) {
                            Ok(()) =>
                                aquires_count + 1,
                            Err(_resource) => {
                                warn!("receiver has been dropped before resource is aquired");
                                aquires_count
                            },
                        };
                        let core = Core { aquire_rx, release_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn, aquires_count, };
                        match resource_status {
                            Resource::Available(state_avail) =>
                                Ok(AquireProcess::Proceed { core, state_avail, }),
                            Resource::OutOfStock(state_no_left) =>
                                Ok(AquireProcess::WaitRelease { core, state_no_left, }),
                        }

                    },
                    Err(ErrorSeverity::Recoverable { state: init_state, }) => {
                        Ok(AquireProcess::RequireRestart {
                            core: Core {
                                aquire_rx, release_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn, aquires_count,
                            },
                            aquire_req_pending: Some(AquireReq { reply_tx, }),
                            init_state,
                        })
                    },
                    Err(ErrorSeverity::Fatal(())) => {
                        error!("{} crashed with fatal error, terminating", params.name.as_ref());
                        Err(())
                    },
                }
            });
        Either::A(future)
    } else {
        Either::B(result(Ok(AquireProcess::Proceed {
            core: Core {
                aquire_rx, release_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn, aquires_count,
            },
            state_avail,
        })))
    };

    future
        .and_then(|aquire_process| {
            match aquire_process {
                AquireProcess::Proceed { core, state_avail, } => {
                    let Core { aquire_rx, release_rx, shutdown_rx, params, aquire_fn, mut release_fn, mut close_fn, aquires_count, } = core;
                    let future = aquire_rx
                        .select2(release_rx)
                        .select2(shutdown_rx)
                        .then(move |await_result| {
                            match await_result {
                                Ok(Either::A((Either::A(((Some(aquire_req), aquire_rx_stream), release_rx)), shutdown_rx))) => {
                                    debug!("main_loop: aquire request");
                                    Either::B(result(Ok(Loop::Continue(MainState {
                                        core: Core {
                                            aquire_rx: aquire_rx_stream.into_future(),
                                            release_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn, aquires_count,
                                        },
                                        state_avail,
                                        aquire_req_pending: Some(aquire_req),
                                    }))))
                                },
                                Ok(Either::A((Either::A(((None, _aquire_rx_stream), _release_rx)), _shutdown_rx))) => {
                                    debug!("main_loop: aquire channel depleted");
                                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                                },
                                Ok(Either::A((Either::B(((Some(release_req), release_rx_stream), aquire_rx)), shutdown_rx))) => {
                                    debug!("main_loop: release request");
                                    let maybe_resource = match release_req {
                                        ReleaseReq::Reimburse(resource) =>
                                            Some(Some(resource)),
                                        ReleaseReq::ResourceLost =>
                                            Some(None),
                                        ReleaseReq::ResourceFault =>
                                            None,
                                    };
                                    let future = if let Some(released_resource) = maybe_resource {
                                        // resource is actually released
                                        let future = release_fn(Resource::Available(state_avail), released_resource)
                                            .into_future()
                                            .then(move |release_result| {
                                                let core = Core {
                                                    release_rx: release_rx_stream.into_future(),
                                                    aquires_count: if aquires_count > 0 { aquires_count - 1 } else { 0 },
                                                    aquire_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn,
                                                };
                                                match release_result {
                                                    Ok(Resource::Available(state_avail)) =>
                                                        Ok(Loop::Continue(MainState { core, state_avail, aquire_req_pending: None, })),
                                                    Ok(Resource::OutOfStock(state_no_left)) =>
                                                        Ok(Loop::Break(BreakMain::WaitRelease { core, state_no_left, })),
                                                    Err(ErrorSeverity::Recoverable { state: init_state, }) =>
                                                        Ok(Loop::Break(BreakMain::RequireRestart { core, init_state, aquire_req_pending: None, })),
                                                    Err(ErrorSeverity::Fatal(())) => {
                                                        error!("{} crashed with fatal error, terminating", core.params.name.as_ref());
                                                        Err(())
                                                    },
                                                }
                                            });
                                        Either::A(future)
                                    } else {
                                        // something wrong with resource, schedule restart
                                        warn!("resource fault report: performing restart");
                                        let future = close_fn(Resource::Available(state_avail))
                                            .into_future()
                                            .map(move |init_state| {
                                                Loop::Break(BreakMain::RequireRestart {
                                                    core: Core {
                                                        release_rx: release_rx_stream.into_future(),
                                                        aquires_count: if aquires_count > 0 { aquires_count - 1 } else { 0 },
                                                        aquire_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn,
                                                    },
                                                    init_state,
                                                    aquire_req_pending: None,
                                                })
                                            });
                                        Either::B(future)
                                    };
                                    Either::A(future)
                                },
                                Ok(Either::A((Either::B(((None, _release_rx_stream), _aquire_rx)), _shutdown_rx))) => {
                                    debug!("main_loop: release channel depleted");
                                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                                },
                                Ok(Either::B((Shutdown, _aquire_release_rxs))) => {
                                    debug!("main_loop: shutdown request");
                                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                                },
                                Err(Either::A((Either::A((((), _aquire_rx), _release_rx)), _shutdown_rx))) => {
                                    debug!("main_loop: aquire channel outer endpoint dropped");
                                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                                },
                                Err(Either::A((Either::B((((), _release_rx), _aquire_rx)), _shutdown_rx))) => {
                                    debug!("main_loop: release channel outer endpoint dropped");
                                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                                },
                                Err(Either::B((oneshot::Canceled, _aquire_release_rxs))) => {
                                    debug!("main_loop: shutdown channel outer endpoint dropped");
                                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                                },
                            }
                        });
                    Either::A(future)
                },
                AquireProcess::WaitRelease { core, state_no_left, } =>
                    Either::B(result(Ok(Loop::Break(BreakMain::WaitRelease { core, state_no_left, })))),
                AquireProcess::RequireRestart { core, init_state, aquire_req_pending, } =>
                    Either::B(result(Ok(Loop::Break(BreakMain::RequireRestart { core, init_state, aquire_req_pending, })))),
            }
        })
}

enum BreakWait<FNA, FNR, FNC, N, S, R, P> {
    Shutdown,
    RequireRestart {
        core: Core<FNA, FNR, FNC, N, R>,
        init_state: S,
    },
    ProceedMain {
        core: Core<FNA, FNR, FNC, N, R>,
        state_avail: P,
    },
}

struct WaitState<FNA, FNR, FNC, N, R, Q> {
    core: Core<FNA, FNR, FNC, N, R>,
    state_no_left: Q,
}

fn wait_loop<FNA, FA, FNR, FR, FNC, FC, N, S, R, P, Q>(
    wait_state: WaitState<FNA, FNR, FNC, N, R, Q>,
)
    -> impl Future<Item = Loop<BreakWait<FNA, FNR, FNC, N, S, R, P>, WaitState<FNA, FNR, FNC, N, R, Q>>, Error = ()>
where FNA: FnMut(P) -> FA,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>>,
      FNR: FnMut(Resource<P, Q>, Option<R>) -> FR,
      FR: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
      FNC: FnMut(Resource<P, Q>) -> FC,
      FC: IntoFuture<Item = S, Error = ()>,
      N: AsRef<str>,
{
    let WaitState {
        core: Core { aquire_rx, release_rx, shutdown_rx, params, aquire_fn, mut release_fn, mut close_fn, aquires_count, },
        state_no_left,
    } = wait_state;

    release_rx
        .select2(shutdown_rx)
        .then(move |await_result| {
            match await_result {
                Ok(Either::A(((Some(release_req), release_rx_stream), shutdown_rx))) => {
                    let maybe_resource = match release_req {
                        ReleaseReq::Reimburse(resource) =>
                            Some(Some(resource)),
                        ReleaseReq::ResourceLost =>
                            Some(None),
                        ReleaseReq::ResourceFault =>
                            None,
                    };
                    let future = if let Some(released_resource) = maybe_resource {
                        // resource is actually released
                        let future = release_fn(Resource::OutOfStock(state_no_left), released_resource)
                            .into_future()
                            .then(move |release_result| {
                                let mut core = Core {
                                    release_rx: release_rx_stream.into_future(),
                                    aquires_count: if aquires_count > 0 { aquires_count - 1 } else { 0 },
                                    aquire_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn,
                                };
                                match release_result {
                                    Ok(Resource::Available(state_avail)) =>
                                        Either::A(result(Ok(Loop::Break(BreakWait::ProceedMain { core, state_avail, })))),
                                    Ok(Resource::OutOfStock(state_no_left)) =>
                                        if aquires_count > 0 {
                                            Either::A(result(Ok(Loop::Continue(WaitState { core, state_no_left, }))))
                                        } else {
                                            info!("{} runs out of resources, performing restart", core.params.name.as_ref());
                                            let future = (core.close_fn)(Resource::OutOfStock(state_no_left))
                                                .into_future()
                                                .map(move |init_state| {
                                                    Loop::Break(BreakWait::RequireRestart { core, init_state, })
                                                });
                                            Either::B(future)
                                        },
                                    Err(ErrorSeverity::Recoverable { state: init_state, }) =>
                                        Either::A(result(Ok(Loop::Break(BreakWait::RequireRestart { core, init_state, })))),
                                    Err(ErrorSeverity::Fatal(())) => {
                                        error!("{} crashed with fatal error, terminating", core.params.name.as_ref());
                                        Either::A(result(Err(())))
                                    },
                                }
                            });
                        Either::A(future)
                    } else {
                        // something wrong with resource, schedule restart
                        warn!("resource fault report: performing restart");
                        let future = close_fn(Resource::OutOfStock(state_no_left))
                            .into_future()
                            .map(move |init_state| {
                                Loop::Break(BreakWait::RequireRestart {
                                    core: Core {
                                        release_rx: release_rx_stream.into_future(),
                                        aquires_count: if aquires_count > 0 { aquires_count - 1 } else { 0 },
                                        aquire_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn,
                                    },
                                    init_state,
                                })
                            });
                        Either::B(future)
                    };
                    Either::A(future)
                },
                Ok(Either::A(((None, _release_rx_stream), _shutdown_rx))) => {
                    debug!("wait_loop: release channel depleted");
                    Either::B(result(Ok(Loop::Break(BreakWait::Shutdown))))
                },
                Ok(Either::B((Shutdown, _release_rx))) => {
                    debug!("wait_loop: shutdown request");
                    Either::B(result(Ok(Loop::Break(BreakWait::Shutdown))))
                },
                Err(Either::A((((), _release_rx_stream), _shutdown_rx))) => {
                    debug!("wait_loop: release channel outer endpoint dropped");
                    Either::B(result(Ok(Loop::Break(BreakWait::Shutdown))))
                },
                Err(Either::B((oneshot::Canceled, _release_rx))) => {
                    debug!("wait_loop: shutdown channel outer endpoint dropped");
                    Either::B(result(Ok(Loop::Break(BreakWait::Shutdown))))
                },
            }
        })
}

fn proceed_with_restart<FNI, FNA, FNR, FNC, N, S, R>(
    restart_state: RestartState<FNI, FNA, FNR, FNC, N, S, R>,
)
    -> impl Future<Item = Loop<(), RestartState<FNI, FNA, FNR, FNC, N, S, R>>, Error = ()>
where N: AsRef<str>,
{
    match restart_state.core.params.restart_strategy {
        RestartStrategy::RestartImmediately => {
            error!("restarting {} immediately", restart_state.core.params.name.as_ref());
            Either::B(result(Ok(Loop::Continue(restart_state))))
        },
        RestartStrategy::Delay { restart_after, } => {
            info!("restarting {} in {:?}", restart_state.core.params.name.as_ref(), restart_after);
            let future = Delay::new(Instant::now() + restart_after)
                .then(|_delay_result| Ok(Loop::Continue(restart_state)));
            Either::A(future)
        },
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum UsingError<E> {
    ResourceTaskGone,
    Fatal(E),
}

impl<R> Lode<R> {
    pub fn shutdown(self) {
        if let Err(..) = self.shutdown_tx.send(Shutdown) {
            warn!("resource task is gone while performing shutdown");
        }
    }

    pub fn steal_resource(self) -> impl Future<Item = (R, Lode<R>), Error = ()> {
        let Lode { aquire_tx, release_tx, shutdown_tx, } = self;
        let (resource_tx, resource_rx) = oneshot::channel();
        aquire_tx
            .send(AquireReq { reply_tx: resource_tx, })
            .map_err(|_send_error| {
                warn!("resource task is gone while aquiring resource");
            })
            .and_then(move |aquire_tx| {
                resource_rx
                    .map_err(|oneshot::Canceled| {
                        warn!("resouce task is gone while receiving aquired resource");
                    })
                    .and_then(move |resource| {
                        release_tx
                            .send(ReleaseReq::ResourceLost)
                            .map_err(|_send_error| {
                                warn!("resource task is gone while releasing resource");
                            })
                            .map(move |release_tx| {
                                (resource, Lode { aquire_tx, release_tx, shutdown_tx, })
                            })
                    })
            })
    }

    pub fn using_resource<F, T, E, S, FI>(
        self,
        state: S,
        using_fn: F,
    )
        -> impl Future<Item = (T, Lode<R>), Error = UsingError<E>>
    where F: FnMut(R, S) -> FI,
          FI: IntoFuture<Item = (Option<R>, Loop<T, S>), Error = ErrorSeverity<S, E>>
    {
        loop_fn(
            (self, using_fn, state),
            move |(Lode { aquire_tx, release_tx, shutdown_tx, }, mut using_fn, state)| {
                let (resource_tx, resource_rx) = oneshot::channel();
                aquire_tx
                    .send(AquireReq { reply_tx: resource_tx, })
                    .map_err(|_send_error| {
                        warn!("resource task is gone while aquiring resource");
                        UsingError::ResourceTaskGone
                    })
                    .and_then(move |aquire_tx| {
                        resource_rx
                            .map_err(|oneshot::Canceled| {
                                warn!("resouce task is gone while receiving aquired resource");
                                UsingError::ResourceTaskGone
                            })
                            .and_then(move |resource| {
                                using_fn(resource, state)
                                    .into_future()
                                    .then(move |using_result| {
                                        match using_result {
                                            Ok((maybe_resource, loop_action)) => {
                                                release_tx
                                                    .unbounded_send(match maybe_resource {
                                                        None =>
                                                            ReleaseReq::ResourceLost,
                                                        Some(resource) =>
                                                            ReleaseReq::Reimburse(resource),
                                                    })
                                                    .map_err(|_send_error| {
                                                        warn!("resource task is gone while releasing resource");
                                                        UsingError::ResourceTaskGone
                                                    })
                                                    .map(move |()| {
                                                        let lode = Lode { aquire_tx, release_tx, shutdown_tx, };
                                                        match loop_action {
                                                            Loop::Break(item) =>
                                                                Loop::Break((item, lode)),
                                                            Loop::Continue(state) =>
                                                                Loop::Continue((lode, using_fn, state)),
                                                        }
                                                    })
                                            },
                                            Err(error) => {
                                                release_tx
                                                    .unbounded_send(ReleaseReq::ResourceFault)
                                                    .map_err(|_send_error| {
                                                        warn!("resource task is gone while releasing resource");
                                                        UsingError::ResourceTaskGone
                                                    })
                                                    .and_then(move |()| {
                                                        match error {
                                                            ErrorSeverity::Recoverable { state, } =>
                                                                Ok(Loop::Continue((
                                                                    Lode { aquire_tx, release_tx, shutdown_tx, },
                                                                    using_fn,
                                                                    state,
                                                                ))),
                                                            ErrorSeverity::Fatal(fatal_error) =>
                                                                Err(UsingError::Fatal(fatal_error)),
                                                        }
                                                    })
                                            }
                                        }
                                    })
                            })
                    })
            })
    }
}
