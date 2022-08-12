use futures::Future;

use tokio::time::sleep;

use log::{
    debug,
    info,
};

use super::{
    Params,
    ErrorSeverity,
    RestartStrategy,
};

/// Ошибка при рестарте
#[derive(Clone, PartialEq, Debug)]
pub enum RestartableError<E> {
    Fatal(E),
    RestartCrashForced,
}

pub async fn restartable<N, S, R, F, V, E>(
    params: Params<N>,
    mut init_state: S,
    mut restartable_fn: R,
)
    -> Result<V, RestartableError<E>>
where R: FnMut(S) -> F,
      F: Future<Output = Result<V, ErrorSeverity<S, E>>>,
      N: AsRef<str>,
{
    info!("{}: restartable loop started", params.name.as_ref());

    loop {
        match restartable_fn(init_state).await {
            Ok(value) =>
                return Ok(value),
            Err(ErrorSeverity::Recoverable { state, }) =>
                match params.restart_strategy {
                    RestartStrategy::InstantCrash => {
                        info!("recoverable error in {} but current strategy is to crash", params.name.as_ref());
                        return Err(RestartableError::RestartCrashForced);
                    },
                    RestartStrategy::RestartImmediately => {
                        info!("recoverable error in {}, restarting immediately", params.name.as_ref());
                        init_state = state;
                    },
                    RestartStrategy::Delay { restart_after, } => {
                        info!("recoverable error in {}, restarting in {:?}", params.name.as_ref(), restart_after);
                        sleep(restart_after).await;
                        init_state = state;
                    },
                },
            Err(ErrorSeverity::Fatal(error)) => {
                debug!("fatal error in {}", params.name.as_ref());
                return Err(RestartableError::Fatal(error));
            },
        }
    }
}
