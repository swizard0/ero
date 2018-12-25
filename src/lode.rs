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

fn spawn<N, FNI, FS, S, FNL, FR, E, R>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
    init_fn: FNI,
    lode_fn: FNL,
)
    -> Lode<R>
where N: AsRef<str> + Send + 'static,
      FNI: FnMut() -> FS + Send + 'static,
      FS: IntoFuture<Item = S, Error = Error<E>> + 'static,
      FS::Future: Send,
      FNL: FnMut(S) -> FR + Send + 'static,
      FR: IntoFuture<Item = Aquire<R, S>, Error = Error<E>> + 'static,
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
                aquire_rx: aquire_rx_stream.into_future(),
                release_rx: release_rx_stream.into_future(),
                shutdown_rx,
                params,
                init_fn,
                lode_fn,
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

struct RestartState<N, FNI, FNL, R> {
    aquire_rx: stream::StreamFuture<mpsc::Receiver<AquireReq<R>>>,
    release_rx: stream::StreamFuture<mpsc::UnboundedReceiver<ReleaseReq<R>>>,
    shutdown_rx: oneshot::Receiver<Shutdown>,
    params: Params<N>,
    init_fn: FNI,
    lode_fn: FNL,
}

fn restart_loop<N, FNI, FS, S, FNL, FR, E, R>(
    mut restart_state: RestartState<N, FNI, FNL, R>,
)
    -> impl Future<Item = Loop<(), RestartState<N, FNI, FNL, R>>, Error = ()>
where N: AsRef<str>,
      FNI: FnMut() -> FS + Send + 'static,
      FS: IntoFuture<Item = S, Error = Error<E>>,
      FNL: FnMut(S) -> FR,
      FR: IntoFuture<Item = Aquire<R, S>, Error = Error<E>>,
      E: Debug + Send + 'static,
{
    (restart_state.init_fn)()
        .into_future()
        .then(move |maybe_lode_state| {
            match maybe_lode_state {
                Ok(lode_state) => {
                    let RestartState { aquire_rx, release_rx, shutdown_rx, params, lode_fn, init_fn, } = restart_state;
                    let future =
                        loop_fn(
                            ProcessState { aquire_rx, release_rx, shutdown_rx, params, lode_fn, lode_state, },
                            lode_loop,
                        )
                        .then(move |inner_loop_result| {
                            match inner_loop_result {
                                Ok(BreakReason::Shutdown) =>
                                    Either::A(result(Ok(Loop::Break(())))),
                                Ok(BreakReason::RequireRestart { error, aquire_rx, release_rx, shutdown_rx, params, lode_fn, }) => {
                                    let future = proceed_with_restart(
                                        RestartState { aquire_rx, release_rx, shutdown_rx, params, lode_fn, init_fn, },
                                        error,
                                    );
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
                    error!("{} crashed with fatal error: {:?}, terminating", restart_state.params.name.as_ref(), error);
                    Either::B(Either::B(result(Err(()))))
                },
            }
        })
}

enum BreakReason<E, R, N, FNL> {
    Shutdown,
    RequireRestart {
        error: E,
        aquire_rx: stream::StreamFuture<mpsc::Receiver<AquireReq<R>>>,
        release_rx: stream::StreamFuture<mpsc::UnboundedReceiver<ReleaseReq<R>>>,
        shutdown_rx: oneshot::Receiver<Shutdown>,
        params: Params<N>,
        lode_fn: FNL,
    },
}

struct ProcessState<N, S, FNL, R> {
    aquire_rx: stream::StreamFuture<mpsc::Receiver<AquireReq<R>>>,
    release_rx: stream::StreamFuture<mpsc::UnboundedReceiver<ReleaseReq<R>>>,
    shutdown_rx: oneshot::Receiver<Shutdown>,
    params: Params<N>,
    lode_state: S,
    lode_fn: FNL,
}

fn lode_loop<N, S, FNL, FR, E, R>(
    process_state: ProcessState<N, S, FNL, R>,
)
    -> impl Future<Item = Loop<BreakReason<E, R, N, FNL>, ProcessState<N, S, FNL, R>>, Error = ()>
where N: AsRef<str>,
      FNL: FnMut(S) -> FR,
      FR: IntoFuture<Item = Aquire<R, S>, Error = Error<E>>,
      E: Debug,
{
    let ProcessState { aquire_rx, release_rx, shutdown_rx, params, lode_state, mut lode_fn, } = process_state;

    aquire_rx
        .select2(release_rx)
        .select2(shutdown_rx)
        .then(move |await_result| {
            match await_result {
                Ok(Either::A((Either::A(((Some(AquireReq { reply_tx, }), aquire_rx_stream), release_rx)), shutdown_rx))) => {
                    debug!("aquire request");
                    let future = lode_fn(lode_state)
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
                                    let next_state = ProcessState {
                                        aquire_rx: aquire_rx_stream.into_future(),
                                        lode_state: state,
                                        release_rx, shutdown_rx, params, lode_fn,
                                    };
                                    Ok(Loop::Continue(next_state))
                                },
                                Err(Error::Recoverable(error)) => {
                                    Ok(Loop::Break(BreakReason::RequireRestart {
                                        aquire_rx: aquire_rx_stream.into_future(),
                                        error, release_rx, shutdown_rx, params, lode_fn,
                                    }))
                                },
                                Err(Error::Fatal(error)) => {
                                    error!("{} processing crashed with fatal error: {:?}, terminating", params.name.as_ref(), error);
                                    Err(())
                                },
                            }
                        });
                    Either::A(future)
                },
                Ok(Either::A((Either::A(((None, aquire_rx_stream), _release_rx)), _shutdown_rx))) => {
                    debug!("aquire channel depleted");
                    Either::B(result(Ok(Loop::Break(BreakReason::Shutdown))))
                },
                Ok(Either::A((Either::B(((Some(release_req), release_rx_stream), aquire_rx)), shutdown_rx))) => {
                    // TODO

                    // let next_state = ProcessState {
                    //     release_rx: release_rx_stream.into_future(),
                    //     aquire_rx, params, lode_state,
                    // };
                    // Ok(Loop::Continue(next_state))

                    Either::B(result(Ok(Loop::Break(BreakReason::Shutdown))))
                },
                Ok(Either::A((Either::B(((None, release_rx_stream), _aquire_rx)), _shutdown_rx))) => {
                    debug!("release channel depleted");
                    Either::B(result(Ok(Loop::Break(BreakReason::Shutdown))))
                },
                Ok(Either::B((Shutdown, _aquire_release_rxs))) => {
                    debug!("shutdown request");
                    Either::B(result(Ok(Loop::Break(BreakReason::Shutdown))))
                },
                Err(Either::A((Either::A((((), _aquire_rx), _release_rx)), _shutdown_rx))) => {
                    debug!("aquire channel outer endpoint dropped");
                    Either::B(result(Ok(Loop::Break(BreakReason::Shutdown))))
                },
                Err(Either::A((Either::B((((), _release_rx), _aquire_rx)), _shutdown_rx))) => {
                    debug!("release channel outer endpoint dropped");
                    Either::B(result(Ok(Loop::Break(BreakReason::Shutdown))))
                },
                Err(Either::B((oneshot::Canceled, _aquire_release_rxs))) => {
                    debug!("release channel outer endpoint dropped");
                    Either::B(result(Ok(Loop::Break(BreakReason::Shutdown))))
                },
            }
        })
}

fn proceed_with_restart<N, FNI, FNL, R, E>(
    mut restart_state: RestartState<N, FNI, FNL, R>,
    error: E,
)
    -> impl Future<Item = Loop<(), RestartState<N, FNI, FNL, R>>, Error = ()>
where N: AsRef<str>,
      E: Debug,
{
    if let Some(delay) = restart_state.params.restart_delay {
        error!("{} crashed with: {:?}, restarting in {:?}", restart_state.params.name.as_ref(), error, delay);
        let future = Delay::new(Instant::now() + delay)
            .then(|_delay_result| Ok(Loop::Continue(restart_state)));
        Either::A(future)
    } else {
        error!("{} crashed with: {:?}, restarting immediately", restart_state.params.name.as_ref(), error);
        Either::B(result(Ok(Loop::Continue(restart_state))))
    }
}
