use std::{
    time::{
        Instant,
        Duration,
    },
    fmt::Debug,
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
    warn,
    error,
};

use tokio::timer::Delay;

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

pub struct Aquire<R, S> {
    resource: R,
    state: S,
    no_more_left: bool,
}

#[derive(Debug)]
pub enum Error<E> {
    Fatal(E),
    Recoverable(E),
}

pub struct Params<N> {
    name: N,
    restart_delay: Option<Duration>,
}

pub fn spawn<N, FNI, FI, S, FNA, FA, FNR, FR, E, R>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
    init_fn: FNI,
    aquire_fn: FNA,
    release_fn: FNR,
)
    -> Lode<R>
where N: AsRef<str> + Send + 'static,
      FNI: FnMut() -> FI + Send + 'static,
      FI: IntoFuture<Item = S, Error = Error<E>> + 'static,
      FI::Future: Send,
      FNA: FnMut(S) -> FA + Send + 'static,
      FA: IntoFuture<Item = Aquire<R, S>, Error = Error<E>> + 'static,
      FA::Future: Send,
      FNR: FnMut(S, Option<R>) -> FR + Send + 'static,
      FR: IntoFuture<Item = S, Error = Error<E>> + 'static,
      FR::Future: Send,
      E: Debug + Send + 'static,
      R: Send + 'static,
      S: Send + 'static,
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
                },
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

struct Core<N, FNA, FNR, R> {
    aquire_rx: stream::StreamFuture<mpsc::Receiver<AquireReq<R>>>,
    release_rx: stream::StreamFuture<mpsc::UnboundedReceiver<ReleaseReq<R>>>,
    shutdown_rx: oneshot::Receiver<Shutdown>,
    params: Params<N>,
    aquire_fn: FNA,
    release_fn: FNR,
    aquires_count: usize,
}

struct RestartState<N, FNI, FNA, FNR, R> {
    core: Core<N, FNA, FNR, R>,
    init_fn: FNI,
}

fn restart_loop<N, FNI, FS, S, FNA, FA, FNR, FR, E, R>(
    mut restart_state: RestartState<N, FNI, FNA, FNR, R>,
)
    -> impl Future<Item = Loop<(), RestartState<N, FNI, FNA, FNR, R>>, Error = ()>
where N: AsRef<str>,
      FNI: FnMut() -> FS + Send + 'static,
      FS: IntoFuture<Item = S, Error = Error<E>>,
      FNA: FnMut(S) -> FA,
      FA: IntoFuture<Item = Aquire<R, S>, Error = Error<E>>,
      FNR: FnMut(S, Option<R>) -> FR,
      FR: IntoFuture<Item = S, Error = Error<E>>,
      E: Debug + Send + 'static,
{
    (restart_state.init_fn)()
        .into_future()
        .then(move |maybe_lode_state| {
            match maybe_lode_state {
                Ok(lode_state) => {
                    let RestartState { core, init_fn, } = restart_state;
                    let future =
                        loop_fn(OuterState { core, lode_state, }, outer_loop)
                        .then(move |inner_loop_result| {
                            match inner_loop_result {
                                Ok(BreakOuter::Shutdown) =>
                                    Either::A(result(Ok(Loop::Break(())))),
                                Ok(BreakOuter::RequireRestart { error, core, }) => {
                                    let future = proceed_with_restart(RestartState { init_fn, core, }, error);
                                    Either::B(Either::A(future))
                                },
                                Err(()) =>
                                    Either::B(Either::B(result(Err(())))),
                            }
                        });
                    Either::A(future)
                },
                Err(Error::Recoverable(error)) => {
                    let future = proceed_with_restart(restart_state, error);
                    Either::B(Either::A(future))
                },
                Err(Error::Fatal(error)) => {
                    error!("{} crashed with fatal error: {:?}, terminating", restart_state.core.params.name.as_ref(), error);
                    Either::B(Either::B(result(Err(()))))
                },
            }
        })
}

enum BreakOuter<E, R, N, FNA, FNR> {
    Shutdown,
    RequireRestart {
        error: E,
        core: Core<N, FNA, FNR, R>,
    },
}

struct OuterState<N, S, FNA, FNR, R> {
    core: Core<N, FNA, FNR, R>,
    lode_state: S,
}

fn outer_loop<N, S, FNA, FA, FNR, FR, E, R>(
    outer_state: OuterState<N, S, FNA, FNR, R>,
)
    -> impl Future<Item = Loop<BreakOuter<E, R, N, FNA, FNR>, OuterState<N, S, FNA, FNR, R>>, Error = ()>
where N: AsRef<str>,
      FNA: FnMut(S) -> FA,
      FA: IntoFuture<Item = Aquire<R, S>, Error = Error<E>>,
      FNR: FnMut(S, Option<R>) -> FR,
      FR: IntoFuture<Item = S, Error = Error<E>>,
      E: Debug,
{
    let OuterState { core, lode_state, } = outer_state;

    loop_fn(MainState { core, lode_state, }, main_loop)
        .then(move |inner_loop_result| {
            match inner_loop_result {
                Ok(BreakMain::Shutdown) =>
                    Ok(Loop::Break(BreakOuter::Shutdown)),
                Ok(BreakMain::RequireRestart { error, core, }) =>
                    Ok(Loop::Break(BreakOuter::RequireRestart { error, core, })),
                Ok(BreakMain::WaitRelease { core, }) => {
                    // TODO
                    Ok(Loop::Break(BreakOuter::Shutdown))
                },
                Err(()) =>
                    Err(()),
            }
        })
}

enum BreakMain<E, R, N, FNA, FNR> {
    Shutdown,
    RequireRestart {
        error: E,
        core: Core<N, FNA, FNR, R>,
    },
    WaitRelease {
        core: Core<N, FNA, FNR, R>,
    },
}

struct MainState<N, S, FNA, FNR, R> {
    core: Core<N, FNA, FNR, R>,
    lode_state: S,
}

fn main_loop<N, S, FNA, FA, FNR, FR, E, R>(
    main_state: MainState<N, S, FNA, FNR, R>,
)
    -> impl Future<Item = Loop<BreakMain<E, R, N, FNA, FNR>, MainState<N, S, FNA, FNR, R>>, Error = ()>
where N: AsRef<str>,
      FNA: FnMut(S) -> FA,
      FA: IntoFuture<Item = Aquire<R, S>, Error = Error<E>>,
      FNR: FnMut(S, Option<R>) -> FR,
      FR: IntoFuture<Item = S, Error = Error<E>>,
      E: Debug,
{
    let MainState {
        core: Core { aquire_rx, release_rx, shutdown_rx, params, mut aquire_fn, mut release_fn, aquires_count, },
        lode_state,
    } = main_state;

    aquire_rx
        .select2(release_rx)
        .select2(shutdown_rx)
        .then(move |await_result| {
            match await_result {
                Ok(Either::A((Either::A(((Some(AquireReq { reply_tx, }), aquire_rx_stream), release_rx)), shutdown_rx))) => {
                    debug!("aquire request");
                    let future = aquire_fn(lode_state)
                        .into_future()
                        .then(move |lode_result| {
                            match lode_result {
                                Ok(Aquire { resource, state, no_more_left, }) => {
                                    match reply_tx.send(resource) {
                                        Ok(()) =>
                                            unimplemented!(),
                                        Err(_resource) =>
                                            warn!("receiver has been dropped before resource is aquired"),
                                    }
                                    let next_state = MainState {
                                        core: Core {
                                            aquire_rx: aquire_rx_stream.into_future(),
                                            aquires_count: aquires_count + 1,
                                            release_rx, shutdown_rx, params, aquire_fn, release_fn,
                                        },
                                        lode_state: state,
                                    };
                                    Ok(Loop::Continue(next_state))
                                },
                                Err(Error::Recoverable(error)) => {
                                    Ok(Loop::Break(BreakMain::RequireRestart {
                                        core: Core {
                                            aquire_rx: aquire_rx_stream.into_future(),
                                            release_rx, shutdown_rx, params, aquire_fn, release_fn, aquires_count,
                                        },
                                        error,
                                    }))
                                },
                                Err(Error::Fatal(error)) => {
                                    error!("{} crashed with fatal error: {:?}, terminating", params.name.as_ref(), error);
                                    Err(())
                                },
                            }
                        });
                    Either::A(future)
                },
                Ok(Either::A((Either::A(((None, aquire_rx_stream), _release_rx)), _shutdown_rx))) => {
                    debug!("aquire channel depleted");
                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                },
                Ok(Either::A((Either::B(((Some(release_req), release_rx_stream), aquire_rx)), shutdown_rx))) => {
                    // TODO

                    // let next_state = ProcessState {
                    //     release_rx: release_rx_stream.into_future(),
                    //     aquire_rx, params, lode_state,
                    // };
                    // Ok(Loop::Continue(next_state))

                    Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
                },
                Ok(Either::A((Either::B(((None, release_rx_stream), _aquire_rx)), _shutdown_rx))) => {
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

fn proceed_with_restart<N, FNI, FNA, FNR, R, E>(
    mut restart_state: RestartState<N, FNI, FNA, FNR, R>,
    error: E,
)
    -> impl Future<Item = Loop<(), RestartState<N, FNI, FNA, FNR, R>>, Error = ()>
where N: AsRef<str>,
      E: Debug,
{
    if let Some(delay) = restart_state.core.params.restart_delay {
        error!("{} crashed with: {:?}, restarting in {:?}", restart_state.core.params.name.as_ref(), error, delay);
        let future = Delay::new(Instant::now() + delay)
            .then(|_delay_result| Ok(Loop::Continue(restart_state)));
        Either::A(future)
    } else {
        error!("{} crashed with: {:?}, restarting immediately", restart_state.core.params.name.as_ref(), error);
        Either::B(result(Ok(Loop::Continue(restart_state))))
    }
}
