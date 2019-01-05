use futures::{
    Stream,
    Future,
    IntoFuture,
};

use log::{info, error};

use super::{
    super::{
        ErrorSeverity,
    },
    Lode,
    Params,
    Resource,
};

pub fn spawn<FNI, FI, N, S, SR, R>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
    init_state: S,
    mut init_fn: FNI,
)
    -> Lode<R>
where FNI: FnMut(S) -> FI + Send + 'static,
      FI: IntoFuture<Item = (SR, S), Error = ErrorSeverity<S, ()>> + 'static,
      FI::Future: Send,
      SR: Stream<Item = R, Error = ()>,
      N: AsRef<str> + Send + 'static,
      S: Send + 'static,
      SR: Send + 'static,
      R: Send + 'static,
{
    super::spawn(
        executor,
        params,
        init_state,
        move |state_s| {
            init_fn(state_s)
                .into_future()
                .and_then(|(stream, state_s)| stream_peek(stream, state_s))
        },
        |(resource, stream, state_s)| {
            stream_peek(stream, state_s)
                .map(move |next_state| (resource, next_state))
        },
        |state_p, _maybe_resource| {
            Ok(Resource::Available(state_p))
        },
        |state_q, _maybe_resource| {
            Ok(Resource::OutOfStock(state_q))
        },
        |(_resource, _stream, state_s)| Ok(state_s),
        |state_s| Ok(state_s),
    )
}

fn stream_peek<S, SR, R>(
    stream: SR,
    state_s: S,
)
    -> impl Future<Item = Resource<(R, SR, S), S>, Error = ErrorSeverity<S, ()>>
where SR: Stream<Item = R, Error = ()>,
{
    stream
        .into_future()
        .then(move |result| {
            match result {
                Ok((Some(resource), stream)) =>
                    Ok(Resource::Available((resource, stream, state_s))),
                Ok((None, _stream)) => {
                    info!("stream depleted");
                    Ok(Resource::OutOfStock(state_s))
                },
                Err(((), _stream)) => {
                    error!("stream error occurred, performing restart");
                    Err(ErrorSeverity::Recoverable { state: state_s, })
                },
            }
        })
}
