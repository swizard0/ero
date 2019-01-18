use std::time::Instant;

use futures::{
    future::{
        self,
        loop_fn,
        Either,
    },
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
    Loop,
    Params,
    ErrorSeverity,
    RestartStrategy,
};

pub fn restartable<N, S, F, I, T, E>(
    params: Params<N>,
    state: S,
    mut restartable_fn: F,
)
    -> impl Future<Item = T, Error = E>
where F: FnMut(S) -> I,
      I: IntoFuture<Item = T, Error = ErrorSeverity<S, E>>,
      N: AsRef<str>,
{
    loop_fn((params, state), move |(params, state)| {
        restartable_fn(state)
            .into_future()
            .then(move |result| {
                match result {
                    Ok(value) => {
                        debug!("{} normal termination", params.name.as_ref());
                        let future = future::result(Ok(Loop::Break(value)));
                        Either::A(future)
                    },
                    Err(ErrorSeverity::Recoverable { state, }) =>
                        match params.restart_strategy {
                            RestartStrategy::RestartImmediately => {
                                info!("recoverable error in {}, restarting immediately", params.name.as_ref());
                                let future = future::result(Ok(Loop::Continue((params, state))));
                                Either::A(future)
                            },
                            RestartStrategy::Delay { restart_after, } => {
                                info!("recoverable error in {}, restarting in {:?}", params.name.as_ref(), restart_after);
                                let future = Delay::new(Instant::now() + restart_after)
                                    .then(|delay_result| {
                                        if let Err(error) = delay_result {
                                            error!("restart delay timer error: {:?}, restarting anyway", error);
                                        }
                                        Ok(Loop::Continue((params, state)))
                                    });
                                Either::B(future)
                            },
                        },
                    Err(ErrorSeverity::Fatal(error)) => {
                        debug!("{} fatal error", params.name.as_ref());
                        let future = future::result(Err(error));
                        Either::A(future)
                    },
                }
            })
    })
}
