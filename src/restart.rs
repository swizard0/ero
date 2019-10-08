use std::{
    mem,
    time::Instant,
};

use futures::{
    Poll,
    Async,
    Future,
    IntoFuture,
};

use log::{
    debug,
    info,
    error,
};

use tokio::timer::Delay;

use super::{
    Params,
    ErrorSeverity,
    RestartStrategy,
};

#[derive(Clone, PartialEq, Debug)]
pub enum RestartableError<E> {
    Fatal(E),
    RestartCrashForced,
}

pub struct RestartableFuture<N, F, FI, S> {
    params: Params<N>,
    restartable_fn: F,
    mode: RestartableMode<S, FI>,
}

enum RestartableMode<S, FI> {
    Invalid,
    Invoke { state: S, },
    Poll { future: FI, },
    Delay { future: Delay, state: S, },
}

impl<N, S, F, I, T, E> Future for RestartableFuture<N, F, I::Future, S>
where F: FnMut(S) -> I,
      I: IntoFuture<Item = T, Error = ErrorSeverity<S, E>>,
      N: AsRef<str>,
{
    type Item = T;
    type Error = RestartableError<E>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match mem::replace(&mut self.mode, RestartableMode::Invalid) {
                RestartableMode::Invalid =>
                    panic!("cannot poll RestartableFuture twice"),

                RestartableMode::Invoke { state, } => {
                    debug!("RestartableMode::Invoke");
                    let future = (self.restartable_fn)(state)
                        .into_future();
                    self.mode = RestartableMode::Poll { future, };
                },

                RestartableMode::Poll { mut future, } => {
                    debug!("RestartableMode::Poll");
                    match future.poll() {
                        Ok(Async::NotReady) => {
                            self.mode = RestartableMode::Poll { future, };
                            return Ok(Async::NotReady);
                        },
                        Ok(Async::Ready(item)) =>
                            return Ok(Async::Ready(item)),
                        Err(ErrorSeverity::Recoverable { state, }) =>
                            match self.params.restart_strategy {
                                RestartStrategy::InstantCrash => {
                                    info!("recoverable error in {} but current strategy is to crash", self.params.name.as_ref());
                                    return Err(RestartableError::RestartCrashForced);
                                },
                                RestartStrategy::RestartImmediately => {
                                    info!("recoverable error in {}, restarting immediately", self.params.name.as_ref());
                                    self.mode = RestartableMode::Invoke { state, };
                                },
                                RestartStrategy::Delay { restart_after, } => {
                                    info!("recoverable error in {}, restarting in {:?}", self.params.name.as_ref(), restart_after);
                                    let future = Delay::new(Instant::now() + restart_after);
                                    self.mode = RestartableMode::Delay { future, state, };
                                },
                            },
                        Err(ErrorSeverity::Fatal(error)) => {
                            debug!("{} fatal error", self.params.name.as_ref());
                            return Err(RestartableError::Fatal(error));
                        },
                    }
                },

                RestartableMode::Delay { mut future, state, } => {
                    debug!("RestartableMode::Delay");
                    match future.poll() {
                        Ok(Async::NotReady) => {
                            self.mode = RestartableMode::Delay { future, state, };
                            return Ok(Async::NotReady);
                        },
                        Ok(Async::Ready(())) =>
                            self.mode = RestartableMode::Invoke { state, },
                        Err(error) => {
                            error!("restart delay timer error: {:?}, restarting anyway", error);
                            self.mode = RestartableMode::Invoke { state, };
                        },
                    }
                },
            }
        }
    }
}

pub fn restartable<N, S, F, I, T, E>(
    params: Params<N>,
    state: S,
    restartable_fn: F,
)
    -> RestartableFuture<N, F, I::Future, S>
where F: FnMut(S) -> I,
      I: IntoFuture<Item = T, Error = ErrorSeverity<S, E>>,
      N: AsRef<str>,
{
    RestartableFuture {
        params,
        restartable_fn,
        mode: RestartableMode::Invoke { state, },
    }
}
