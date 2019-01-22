use futures::{
    Stream,
    Future,
    IntoFuture,
};

use log::{info, error};

use super::{
    super::{
        ErrorSeverity,
        supervisor::Supervisor,
    },
    LodeResource,
    Params,
    Resource,
};

pub fn spawn_link<FNI, FI, N, S, SR, R>(
    supervisor: &Supervisor,
    params: Params<N>,
    init_state: S,
    mut init_fn: FNI,
)
    -> LodeResource<Option<R>>
where FNI: FnMut(S) -> FI + Send + 'static,
      FI: IntoFuture<Item = (SR, S), Error = ErrorSeverity<S, ()>> + 'static,
      FI::Future: Send,
      SR: Stream<Item = R, Error = ()>,
      N: AsRef<str> + Send + 'static,
      S: Send + 'static,
      SR: Send + 'static,
      R: Send + 'static,
{
    super::spawn_link(
        supervisor,
        params,
        init_state,
        move |state_s| {
            init_fn(state_s)
                .into_future()
                .map(Resource::Available)
        },
        |(stream, state_s)| {
            stream_peek(stream, state_s)
                .map(|(resource, maybe_stream, state_s)| {
                    if let Some(stream) = maybe_stream {
                        (resource, Resource::Available((stream, state_s)))
                    } else {
                        info!("stream depleted");
                        (resource, Resource::OutOfStock(state_s))
                    }
                })
        },
        |state_p, _maybe_resource| {
            Ok(Resource::Available(state_p))
        },
        |state_q, _maybe_resource| {
            Ok(Resource::OutOfStock(state_q))
        },
        |(_stream, state_s)| Ok(state_s),
        |state_s| Ok(state_s),
    )
}

fn stream_peek<S, SR, R>(
    stream: SR,
    state_s: S,
)
    -> impl Future<Item = (Option<R>, Option<SR>, S), Error = ErrorSeverity<S, ()>>
where SR: Stream<Item = R, Error = ()>,
{
    stream
        .into_future()
        .then(move |result| {
            match result {
                Ok((Some(resource), stream)) =>
                    Ok((Some(resource), Some(stream), state_s)),
                Ok((None, _stream)) =>
                    Ok((None, None, state_s)),
                Err(((), _stream)) => {
                    error!("stream error occurred, performing restart");
                    Err(ErrorSeverity::Recoverable { state: state_s, })
                },
            }
        })
}
