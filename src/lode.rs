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
    Stream,
    Future,
    IntoFuture,
    sync::{
        mpsc,
        oneshot,
    },
};

use log::{
    debug,
    error,
};

use tokio::timer::Delay;

pub struct Lode<R> {
    aquire_tx: mpsc::Sender<oneshot::Sender<R>>,
    release_tx: mpsc::UnboundedSender<R>,
}

pub struct Aquire<R, S> {
    lode: R,
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

fn spawn<N, FNI, FS, FSE, S, FNL, FR, FRE, R>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
    mut init_fn: FNI,
    mut lode_fn: FNL,
)
    -> Lode<R>
where N: AsRef<str> + Sync + Send + 'static,
      FNI: FnMut() -> FS + Send + 'static,
      FS: IntoFuture<Item = S, Error = Error<FSE>>,
      FS::Future: Send + 'static,
      FSE: Debug + Send + 'static,
      FNL: FnMut(S) -> FR,
      FR: IntoFuture<Item = Aquire<R, S>, Error = Error<FRE>>,
      FRE: Debug,
      R: Send + 'static,
      S: Send + 'static,
{
    let (aquire_tx_stream, aquire_rx_stream) = mpsc::channel(0);
    let (release_tx_stream, release_rx_stream) = mpsc::unbounded();

    struct RestartState<A, R, N> {
        aquire_rx: A,
        release_rx: R,
        params: Params<N>,
    }

    let init_state = RestartState {
        aquire_rx: aquire_rx_stream.into_future(),
        release_rx: release_rx_stream.into_future(),
        params,
    };

    let task = loop_fn(init_state, move |restart_state| {
        init_fn()
            .into_future()
            .then(move |maybe_lode_state| {
                match maybe_lode_state {
                    Ok(lode_state) => {
                        enum BreakReason<E, S> {
                            Shutdown,
                            RequireRestart { error: E, state: S, },
                        }

                        struct ProcessState<L, A, R, N> {
                            lode_state: L,
                            aquire_rx: A,
                            release_rx: R,
                            params: Params<N>,
                        }

                        let process_state = ProcessState {
                            lode_state,
                            aquire_rx: restart_state.aquire_rx,
                            release_rx: restart_state.release_rx,
                            params: restart_state.params,
                        };

                        let future = loop_fn(process_state, move |ProcessState { lode_state, aquire_rx, release_rx, params, }| {
                            aquire_rx
                                .select2(release_rx)
                                .then(move |await_result| {
                                    match await_result {
                                        Ok(Either::A(((Some(aquire_req), aquire_rx_stream), release_rx))) => {
                                            let future = lode_fn(lode_state)
                                                .into_future()
                                                .then(move |result| {
                                                    match result {
                                                        Ok(Aquire { lode, state, no_more_left, }) => {
                                                            // TODO

                                                            let next_state = ProcessState {
                                                                aquire_rx: aquire_rx_stream.into_future(),
                                                                release_rx, params, lode_state,
                                                            };
                                                            Ok(Loop::Continue(next_state))
                                                        },
                                                        Err(Error::Recoverable(error)) => {
                                                            // TODO

                                                            Ok(Loop::Break(()))
                                                        },
                                                        Err(Error::Fatal(fatal_error)) => {
                                                            error!(
                                                                "{} processing crashed with fatal error: {:?}, terminating",
                                                                params.name.as_ref(),
                                                                fatal_error,
                                                            );
                                                            Err(())
                                                        },
                                                    }
                                                });
                                            Either::A(future)
                                        },
                                        Ok(Either::A(((None, aquire_rx_stream), _release_rx))) => {
                                            debug!("aquire channel depleted");
                                            Either::B(Ok(Loop::Break(BreakReason::Shutdown)))
                                        },
                                        Ok(Either::B(((Some(release_req), release_rx_stream), aquire_rx))) => {
                                            // TODO

                                            // let next_state = ProcessState {
                                            //     release_rx: release_rx_stream.into_future(),
                                            //     aquire_rx, params, lode_state,
                                            // };
                                            // Ok(Loop::Continue(next_state))
                                            Either::B(Ok(Loop::Break(BreakReason::Shutdown)))
                                        },
                                        Ok(Either::B(((None, release_rx_stream), _aquire_rx))) => {
                                            debug!("release channel depleted");
                                            Either::B(Ok(Loop::Break(BreakReason::Shutdown)))
                                        },
                                        Err(Either::A((((), _aquire_rx), _release_rx))) => {
                                            debug!("aquire channel outer endpoint dropped");
                                            Either::B(Ok(Loop::Break(BreakReason::Shutdown)))
                                        },
                                        Err(Either::B((((), _release_rx), _aquire_rx))) => {
                                            debug!("release channel outer endpoint dropped");
                                            Either::B(Ok(Loop::Break(BreakReason::Shutdown)))
                                        },
                                    }
                                })
                        });
                        let future = future
                            .then(move |inner_loop_result: Result<BreakReason<FSE, ProcessState<S, _, _, N>>, ()>| {
                                match inner_loop_result {
                                    Ok(BreakReason::Shutdown) =>
                                        Either::A(result(Ok(Loop::Break(())))),
                                    Ok(BreakReason::RequireRestart { error, state: ProcessState { aquire_rx, release_rx, params, .. }, }) => {
                                        let maybe_delay = params.restart_delay;
                                        error!(
                                            "{} processing crashed with: {:?}, restarting {}",
                                            params.name.as_ref(),
                                            error,
                                            if let Some(ref delay) = maybe_delay {
                                                format!("in {:?}", delay)
                                            } else {
                                                "immediately".to_string()
                                            },
                                        );
                                        let restart_state =
                                            RestartState { aquire_rx, release_rx, params, };

                                        let future = if let Some(delay) = maybe_delay {
                                            let future = Delay::new(Instant::now() + delay)
                                                .then(|_delay_result| Ok(Loop::Continue(restart_state)));
                                            Either::A(future)
                                        } else {
                                            Either::B(result(Ok(Loop::Continue(restart_state))))
                                        };
                                        Either::B(Either::A(future))
                                    },
                                    Err(()) =>
                                        Either::B(Either::B(result(Err(())))),
                                }
                            });
                        Either::A(future)
                    },
                    Err(Error::Recoverable(error)) => {
                        error!(
                            "{} initializing crashed with: {:?}, restarting {}",
                            restart_state.params.name.as_ref(),
                            error,
                            if let Some(ref delay) = restart_state.params.restart_delay {
                                format!("in {:?}", delay)
                            } else {
                                "immediately".to_string()
                            },
                        );
                        let future = if let Some(delay) = restart_state.params.restart_delay {
                            let future = Delay::new(Instant::now() + delay)
                                .then(|_delay_result| Ok(Loop::Continue(restart_state)));
                            Either::A(future)
                        } else {
                            Either::B(result(Ok(Loop::Continue(restart_state))))
                        };
                        Either::B(Either::A(future))
                    },
                    Err(Error::Fatal(fatal_error)) => {
                        error!("{} initializing crashed with fatal error: {:?}, terminating", restart_state.params.name.as_ref(), fatal_error);
                        Either::B(Either::B(result(Err(()))))
                    },
                }
            })
    });

    executor.spawn(task);

    Lode {
        aquire_tx: aquire_tx_stream,
        release_tx: release_tx_stream,
    }
}
