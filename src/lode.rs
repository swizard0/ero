use std::{
    time::{
        Instant,
        Duration,
    },
};

use futures::{
    future::{
        Loop,
        loop_fn,
        result,
        Either,
    },
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

use super::ErrorSeverity;

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
    restart_delay: Option<Duration>,
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
      FI: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S>> + 'static,
      FI::Future: Send,
      FNA: FnMut(P) -> FA + Send + 'static,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S>> + 'static,
      FA::Future: Send,
      FNR: FnMut(Resource<P, Q>, Option<R>) -> FR + Send + 'static,
      FR: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S>> + 'static,
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
    init_state: S,
    init_fn: FNI,
}

fn restart_loop<FNI, FS, FNA, FA, FNR, FR, FNC, FC, N, S, R, P, Q>(
    restart_state: RestartState<FNI, FNA, FNR, FNC, N, S, R>,
)
    -> impl Future<Item = Loop<(), RestartState<FNI, FNA, FNR, FNC, N, S, R>>, Error = ()>
where FNI: FnMut(S) -> FS + Send + 'static,
      FS: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S>>,
      FNA: FnMut(P) -> FA,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S>>,
      FNR: FnMut(Resource<P, Q>, Option<R>) -> FR,
      FR: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S>>,
      FNC: FnMut(Resource<P, Q>) -> FC,
      FC: IntoFuture<Item = S, Error = ()>,
      N: AsRef<str>,
{
    let RestartState { mut core, mut init_state, mut init_fn, } = restart_state;
    init_fn(init_state)
        .into_future()
        .then(move |maybe_lode_state| {
            match maybe_lode_state {
                Ok(Resource::Available(state_avail)) => {
                    let future =
                        loop_fn(OuterState { core, state_avail, }, outer_loop)
                        .then(move |inner_loop_result| {
                            match inner_loop_result {
                                Ok(BreakOuter::Shutdown) =>
                                    Either::A(result(Ok(Loop::Break(())))),
                                Ok(BreakOuter::RequireRestart { core, init_state, }) => {
                                    let future = proceed_with_restart(RestartState {
                                        core, init_state, init_fn,
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
                            proceed_with_restart(RestartState { core, init_state, init_fn, })
                        });
                    Either::A(Either::B(future))
                },
                Err(ErrorSeverity::Recoverable { state: init_state, }) => {
                    let future = proceed_with_restart(RestartState { core, init_state, init_fn, });
                    Either::B(Either::A(future))
                },
                Err(ErrorSeverity::Fatal) => {
                    error!("{} crashed with fatal error, terminating", core.params.name.as_ref());
                    Either::B(Either::B(result(Err(()))))
                },
            }
        })
}

enum BreakOuter<FNA, FNR, FNC, N, S, R> {
    Shutdown,
    RequireRestart {
        core: Core<FNA, FNR, FNC, N, R>,
        init_state: S,
    },
}

struct OuterState<FNA, FNR, FNC, N, R, P> {
    core: Core<FNA, FNR, FNC, N, R>,
    state_avail: P,
}

fn outer_loop<FNA, FA, FNR, FR, FNC, FC, N, S, R, P, Q>(
    outer_state: OuterState<FNA, FNR, FNC, N, R, P>,
)
    -> impl Future<Item = Loop<BreakOuter<FNA, FNR, FNC, N, S, R>, OuterState<FNA, FNR, FNC, N, R, P>>, Error = ()>
where FNA: FnMut(P) -> FA,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S>>,
      FNR: FnMut(Resource<P, Q>, Option<R>) -> FR,
      FR: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S>>,
      FNC: FnMut(Resource<P, Q>) -> FC,
      FC: IntoFuture<Item = S, Error = ()>,
      N: AsRef<str>,
{
    let OuterState { core, state_avail, } = outer_state;

    loop_fn(MainState { core, state_avail, }, main_loop)
        .then(move |inner_loop_result| {
            match inner_loop_result {
                Ok(BreakMain::Shutdown) =>
                    Ok(Loop::Break(BreakOuter::Shutdown)),
                Ok(BreakMain::RequireRestart { core, init_state, }) =>
                    Ok(Loop::Break(BreakOuter::RequireRestart { core, init_state, })),
                Ok(BreakMain::WaitRelease { core, state_no_left, }) => {
                    // TODO
                    Ok(Loop::Break(BreakOuter::Shutdown))
                },
                Err(()) =>
                    Err(()),
            }
        })
}

enum BreakMain<FNA, FNR, FNC, N, S, R, Q> {
    Shutdown,
    RequireRestart {
        core: Core<FNA, FNR, FNC, N, R>,
        init_state: S,
    },
    WaitRelease {
        core: Core<FNA, FNR, FNC, N, R>,
        state_no_left: Q,
    },
}

struct MainState<FNA, FNR, FNC, N, R, P> {
    core: Core<FNA, FNR, FNC, N, R>,
    state_avail: P,
}

fn main_loop<FNA, FA, FNR, FR, FNC, FC, N, S, R, P, Q>(
    main_state: MainState<FNA, FNR, FNC, N, R, P>,
)
    -> impl Future<Item = Loop<BreakMain<FNA, FNR, FNC, N, S, R, Q>, MainState<FNA, FNR, FNC, N, R, P>>, Error = ()>
where FNA: FnMut(P) -> FA,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S>>,
      FNR: FnMut(Resource<P, Q>, Option<R>) -> FR,
      FR: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S>>,
      FNC: FnMut(Resource<P, Q>) -> FC,
      FC: IntoFuture<Item = S, Error = ()>,
      N: AsRef<str>,
{
    let MainState {
        core: Core { aquire_rx, release_rx, shutdown_rx, params, mut aquire_fn, mut release_fn, mut close_fn, aquires_count, },
        state_avail,
    } = main_state;

    aquire_rx
        .select2(release_rx)
        .select2(shutdown_rx)
        .then(move |await_result| {
            match await_result {
                Ok(Either::A((Either::A(((Some(AquireReq { reply_tx, }), aquire_rx_stream), release_rx)), shutdown_rx))) => {
                    debug!("aquire request");
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
                                    let core = Core {
                                        aquire_rx: aquire_rx_stream.into_future(),
                                        release_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn, aquires_count,
                                    };
                                    match resource_status {
                                        Resource::Available(state_avail) =>
                                            Ok(Loop::Continue(MainState { core, state_avail, })),
                                        Resource::OutOfStock(state_no_left) =>
                                            Ok(Loop::Break(BreakMain::WaitRelease { core, state_no_left, })),
                                    }
                                },
                                Err(ErrorSeverity::Recoverable { state: init_state, }) => {
                                    Ok(Loop::Break(BreakMain::RequireRestart {
                                        core: Core {
                                            aquire_rx: aquire_rx_stream.into_future(),
                                            release_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn, aquires_count,
                                        },
                                        init_state,
                                    }))
                                },
                                Err(ErrorSeverity::Fatal) => {
                                    error!("{} crashed with fatal error, terminating", params.name.as_ref());
                                    Err(())
                                },
                            }
                        });
                    Either::A(Either::A(future))
                },
                Ok(Either::A((Either::A(((None, _aquire_rx_stream), _release_rx)), _shutdown_rx))) => {
                    debug!("aquire channel depleted");
                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                },
                Ok(Either::A((Either::B(((Some(release_req), release_rx_stream), aquire_rx)), shutdown_rx))) => {
                    debug!("release request");
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
                                        Ok(Loop::Continue(MainState { core, state_avail, })),
                                    Ok(Resource::OutOfStock(state_no_left)) =>
                                        Ok(Loop::Break(BreakMain::WaitRelease { core, state_no_left, })),
                                    Err(ErrorSeverity::Recoverable { state: init_state, }) =>
                                        Ok(Loop::Break(BreakMain::RequireRestart { core, init_state, })),
                                    Err(ErrorSeverity::Fatal) => {
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
                                        aquire_rx, shutdown_rx, params, aquire_fn, release_fn, close_fn, aquires_count,
                                    },
                                    init_state,
                                })
                            });
                        Either::B(future)
                    };
                    Either::A(Either::B(future))
                },
                Ok(Either::A((Either::B(((None, _release_rx_stream), _aquire_rx)), _shutdown_rx))) => {
                    debug!("release channel depleted");
                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                },
                Ok(Either::B((Shutdown, _aquire_release_rxs))) => {
                    debug!("shutdown request");
                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                },
                Err(Either::A((Either::A((((), _aquire_rx), _release_rx)), _shutdown_rx))) => {
                    debug!("aquire channel outer endpoint dropped");
                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                },
                Err(Either::A((Either::B((((), _release_rx), _aquire_rx)), _shutdown_rx))) => {
                    debug!("release channel outer endpoint dropped");
                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                },
                Err(Either::B((oneshot::Canceled, _aquire_release_rxs))) => {
                    debug!("release channel outer endpoint dropped");
                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
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
    if let Some(delay) = restart_state.core.params.restart_delay {
        info!("restarting {} in {:?}", restart_state.core.params.name.as_ref(), delay);
        let future = Delay::new(Instant::now() + delay)
            .then(|_delay_result| Ok(Loop::Continue(restart_state)));
        Either::A(future)
    } else {
        error!("restarting {} immediately", restart_state.core.params.name.as_ref());
        Either::B(result(Ok(Loop::Continue(restart_state))))
    }
}
