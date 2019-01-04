use futures::{
    future,
    Future,
    IntoFuture,
};

use log::error;

use super::{
    super::{
        ErrorSeverity,
    },
    Lode,
    Params,
    Resource,
};

pub fn spawn<FNI, FI, FNA, FA, FNRM, FRM, FNCM, FCM, N, S, R, P>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
    init_state: S,
    mut init_fn: FNI,
    mut aquire_fn: FNA,
    mut release_fn: FNRM,
    close_fn: FNCM,
)
    -> Lode<R>
where FNI: FnMut(S) -> FI + Send + 'static,
      FI: IntoFuture<Item = P, Error = ErrorSeverity<S, ()>> + 'static,
      FI::Future: Send,
      FNA: FnMut(P) -> FA + Send + 'static,
      FA: IntoFuture<Item = (R, P), Error = ErrorSeverity<S, ()>> + 'static,
      FA::Future: Send,
      FNRM: FnMut(P, Option<R>) -> FRM + Send + 'static,
      FRM: IntoFuture<Item = P, Error = ErrorSeverity<S, ()>> + 'static,
      FRM::Future: Send,
      FNCM: FnMut(P) -> FCM + Send + 'static,
      FCM: IntoFuture<Item = S, Error = ()> + Send + 'static,
      FCM::Future: Send,
      N: AsRef<str> + Send + 'static,
      S: Send + 'static,
      R: Send + 'static,
      P: Send + 'static,
{
    super::spawn(
        executor,
        params,
        init_state,
        move |state_s| {
            init_fn(state_s)
                .into_future()
                .map(Resource::Available)
        },
        move |state_p| {
            aquire_fn(state_p)
                .into_future()
                .map(|(resource, state_p)| (resource, Resource::Available(state_p)))
        },
        move |state_p, maybe_resource| {
            release_fn(state_p, maybe_resource)
                .into_future()
                .map(Resource::Available)
        },
        |(), _maybe_resource| {
            error!("something went wrong: release should not be invoked in wait loop for lode::shared");
            future::result(Err(ErrorSeverity::Fatal(())))
        },
        close_fn,
        |()| {
            error!("something went wrong: close should not be invoked in wait loop for lode::shared");
            future::result(Err(()))
        },
    )
}
