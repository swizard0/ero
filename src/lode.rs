use std::{
    mem,
    time::Instant,
};

use futures::{
    Poll,
    Async,
    sync::{
        mpsc,
        oneshot,
    },
    future::loop_fn,
    Sink,
    Future,
    Stream,
    IntoFuture,
};

use log::{
    debug,
    info,
    warn,
    error,
};

use tokio::timer::Delay;

use super::{
    Loop,
    Params,
    ErrorSeverity,
    RestartStrategy,
    supervisor::Supervisor,
};

pub mod uniq;
pub mod shared;
pub mod stream;

#[cfg(test)]
mod tests;

struct AquireReq<R> {
    reply_tx: oneshot::Sender<ResourceGen<R>>,
}

type Generation = u64;

struct ResourceGen<R> {
    resource: R,
    generation: Generation,
}

struct ReleaseReq<R> {
    generation: Generation,
    status: ResourceStatus<R>,
}

enum ResourceStatus<R> {
    Reimburse(R),
    ResourceLost,
    ResourceFault,
}

type AquirePeer<R> = mpsc::Receiver<AquireReq<R>>;
type ReleasePeer<R> = mpsc::UnboundedReceiver<ReleaseReq<R>>;

pub struct LodeResource<R> {
    aquire_tx: mpsc::Sender<AquireReq<R>>,
    release_tx: mpsc::UnboundedSender<ReleaseReq<R>>,
}

impl<R> Clone for LodeResource<R> {
    fn clone(&self) -> Self {
        LodeResource {
            aquire_tx: self.aquire_tx.clone(),
            release_tx: self.release_tx.clone(),
        }
    }
}

pub enum Resource<P, Q> {
    Available(P),
    OutOfStock(Q),
}

pub fn spawn_link<FNI, FI, FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q>(
    supervisor: &Supervisor,
    params: Params<N>,
    init_state: S,
    init_fn: FNI,
    aquire_fn: FNA,
    release_main_fn: FNRM,
    release_wait_fn: FNRW,
    close_main_fn: FNCM,
    close_wait_fn: FNCW,
)
    -> LodeResource<R>
where FNI: FnMut(S) -> FI + Send + 'static,
      FI: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
      FI::Future: Send,
      FNA: FnMut(P) -> FA + Send + 'static,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>> + 'static,
      FA::Future: Send,
      FNRM: FnMut(P, Option<R>) -> FRM + Send + 'static,
      FRM: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
      FRM::Future: Send,
      FNRW: FnMut(Q, Option<R>) -> FRW + Send + 'static,
      FRW: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
      FRW::Future: Send,
      FNCM: FnMut(P) -> FCM + Send + 'static,
      FCM: IntoFuture<Item = S, Error = ()> + Send + 'static,
      FCM::Future: Send,
      FNCW: FnMut(Q) -> FCW + Send + 'static,
      FCW: IntoFuture<Item = S, Error = ()> + Send + 'static,
      FCW::Future: Send,
      N: AsRef<str> + Send + 'static,
      S: Send + 'static,
      R: Send + 'static,
      P: Send + 'static,
      Q: Send + 'static,
{
    let (aquire_tx, aquire_rx) = mpsc::channel(0);
    let (release_tx, release_rx) = mpsc::unbounded();

    let task_future = LodeFuture {
        core: Core {
            params,
            init_fn,
            aquire_fn,
            release_main_fn,
            release_wait_fn,
            close_main_fn,
            close_wait_fn,
            aquire_rx,
            release_rx,
            generation: 0,
            aquires_count: 0,
        },
        state: State::WantAquireReq(StateWantAquireReq {
            init_state,
        }),
    };
    supervisor.spawn_link(task_future);

    LodeResource { aquire_tx, release_tx, }
}


struct Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R> {
    params: Params<N>,
    init_fn: FNI,
    aquire_fn: FNA,
    release_main_fn: FNRM,
    release_wait_fn: FNRW,
    close_main_fn: FNCM,
    close_wait_fn: FNCW,
    aquire_rx: AquirePeer<R>,
    release_rx: ReleasePeer<R>,
    generation: Generation,
    aquires_count: usize,
}

struct LodeFuture<FNI, FI, FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q>
where FI: IntoFuture,
      FA: IntoFuture,
      FRM: IntoFuture,
      FRW: IntoFuture,
      FCM: IntoFuture,
      FCW: IntoFuture,
{
    core: Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    state: State<FI, FA, FRM, FRW, FCM, FCW, S, R, P, Q>,
}

enum State<FI, FA, FRM, FRW, FCM, FCW, S, R, P, Q>
where FI: IntoFuture,
      FA: IntoFuture,
      FRM: IntoFuture,
      FRW: IntoFuture,
      FCM: IntoFuture,
      FCW: IntoFuture,
{
    Invalid,
    WantAquireReq(StateWantAquireReq<S>),
    WantInitFn(StateWantInitFn<FI::Future, R>),
    WantInitFnRestart(StateWantInitFnRestart<S, R>),
    WantAquireReqRestart(StateWantAquireReqRestart<S>),
    WantInitFnClose(StateWantInitFnClose<FCW::Future, R>),
    WantAquireFn(StateWantAquireFn<FA::Future, R>),
    WantAquireThenRelease(StateWantAquireThenRelease<P>),
    WantReleaseThenAquire(StateWantReleaseThenAquire<P>),
    WantReleaseMainFn(StateWantReleaseFn<FRM::Future>),
    WantCloseMainFn(StateWantCloseFn<FCM::Future>),
    WantReleaseReq(StateWantReleaseReq<Q>),
    WantReleaseWaitFn(StateWantReleaseFn<FRW::Future>),
    WantCloseWaitFn(StateWantCloseFn<FCW::Future>),
}

impl<FNI, FI, FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q> Future
    for LodeFuture<FNI, FI, FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q>
where FNI: FnMut(S) -> FI + Send + 'static,
      FI: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
      FI::Future: Send,
      FNA: FnMut(P) -> FA + Send + 'static,
      FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>> + 'static,
      FA::Future: Send,
      FNRM: FnMut(P, Option<R>) -> FRM + Send + 'static,
      FRM: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
      FRM::Future: Send,
      FNRW: FnMut(Q, Option<R>) -> FRW + Send + 'static,
      FRW: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
      FRW::Future: Send,
      FNCM: FnMut(P) -> FCM + Send + 'static,
      FCM: IntoFuture<Item = S, Error = ()> + Send + 'static,
      FCM::Future: Send,
      FNCW: FnMut(Q) -> FCW + Send + 'static,
      FCW: IntoFuture<Item = S, Error = ()> + Send + 'static,
      FCW::Future: Send,
      N: AsRef<str> + Send + 'static,
      S: Send + 'static,
      R: Send + 'static,
      P: Send + 'static,
      Q: Send + 'static,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match mem::replace(&mut self.state, State::Invalid) {
                State::Invalid =>
                    panic!("cannot poll LodeFuture twice"),

                State::WantAquireReq(state) =>
                    match state.step(&mut self.core) {
                        DoWantAquireReq::NotReady { next_state, } => {
                            self.state = State::WantAquireReq(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantAquireReq::Proceed { next_state, } =>
                            self.state = State::WantInitFn(next_state),
                        DoWantAquireReq::Shutdown =>
                            return Ok(Async::Ready(())),
                    },

                State::WantInitFn(state) =>
                    match state.step(&mut self.core) {
                        DoWantInitFn::NotReady { next_state, } => {
                            self.state = State::WantInitFn(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantInitFn::Aquire { next_state, } =>
                            self.state = State::WantAquireFn(next_state),
                        DoWantInitFn::Close { next_state, } =>
                            self.state = State::WantInitFnClose(next_state),
                        DoWantInitFn::RestartNow { next_state, } =>
                            self.state = State::WantInitFn(next_state),
                        DoWantInitFn::RestartWait { next_state, } =>
                            self.state = State::WantInitFnRestart(next_state),
                        DoWantInitFn::Fatal =>
                            return Err(()),
                    },

                State::WantInitFnRestart(state) =>
                    match state.step(&mut self.core) {
                        DoWantInitFnRestart::NotReady { next_state, } => {
                            self.state = State::WantInitFnRestart(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantInitFnRestart::ItIsTime { next_state, } =>
                            self.state = State::WantInitFn(next_state),
                        DoWantInitFnRestart::Fatal =>
                            return Err(()),
                    },

                State::WantAquireReqRestart(state) =>
                    match state.step(&mut self.core) {
                        DoWantAquireReqRestart::NotReady { next_state, } => {
                            self.state = State::WantAquireReqRestart(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantAquireReqRestart::ItIsTime { next_state, } =>
                            self.state = State::WantAquireReq(next_state),
                        DoWantAquireReqRestart::Fatal =>
                            return Err(()),
                    },

                State::WantInitFnClose(state) =>
                    match state.step(&mut self.core) {
                        DoWantInitFnClose::NotReady { next_state, } => {
                            self.state = State::WantInitFnClose(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantInitFnClose::RestartNow { next_state, } =>
                            self.state = State::WantInitFn(next_state),
                        DoWantInitFnClose::RestartWait { next_state, } =>
                            self.state = State::WantInitFnRestart(next_state),
                        DoWantInitFnClose::Fatal =>
                            return Err(()),
                    },

                State::WantAquireFn(state) =>
                    match state.step(&mut self.core) {
                        DoWantAquireFn::NotReady { next_state, } => {
                            self.state = State::WantAquireFn(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantAquireFn::Available { next_state, } =>
                            self.state = State::WantAquireThenRelease(next_state),
                        DoWantAquireFn::OutOfStock { next_state, } =>
                            self.state = State::WantReleaseReq(next_state),
                        DoWantAquireFn::RestartNow { next_state, } =>
                            self.state = State::WantInitFn(next_state),
                        DoWantAquireFn::RestartWait { next_state, } =>
                            self.state = State::WantInitFnRestart(next_state),
                        DoWantAquireFn::Fatal =>
                            return Err(()),
                    },

                State::WantAquireThenRelease(state) =>
                    match state.step(&mut self.core) {
                        DoWantAquireThenRelease::NotReady { next_state, } =>
                            self.state = State::WantReleaseThenAquire(next_state),
                        DoWantAquireThenRelease::Aquire { next_state, } =>
                            self.state = State::WantAquireFn(next_state),
                        DoWantAquireThenRelease::Shutdown =>
                            return Ok(Async::Ready(())),
                    },

                State::WantReleaseThenAquire(state) =>
                    match state.step(&mut self.core) {
                        DoWantReleaseThenAquire::NotReady { next_state, } => {
                            self.state = State::WantAquireThenRelease(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantReleaseThenAquire::Release { next_state, } =>
                            self.state = State::WantReleaseMainFn(next_state),
                        DoWantReleaseThenAquire::Close { next_state, } =>
                            self.state = State::WantCloseMainFn(next_state),
                        DoWantReleaseThenAquire::TryAgain { next_state, } =>
                            self.state = State::WantReleaseThenAquire(next_state),
                        DoWantReleaseThenAquire::Shutdown =>
                            return Ok(Async::Ready(())),
                    },

                State::WantReleaseMainFn(state) =>
                    match state.step(&mut self.core) {
                        DoWantReleaseFn::NotReady { next_state, } => {
                            self.state = State::WantReleaseMainFn(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantReleaseFn::Available { next_state, } =>
                            self.state = State::WantReleaseThenAquire(next_state),
                        DoWantReleaseFn::OutOfStock { next_state, } =>
                            self.state = State::WantReleaseReq(next_state),
                        DoWantReleaseFn::RestartNow { next_state, } =>
                            self.state = State::WantAquireReq(next_state),
                        DoWantReleaseFn::RestartWait { next_state, } =>
                            self.state = State::WantAquireReqRestart(next_state),
                        DoWantReleaseFn::Fatal =>
                            return Err(()),
                    },

                State::WantCloseMainFn(state) =>
                    match state.step(&mut self.core) {
                        DoWantCloseFn::NotReady { next_state, } => {
                            self.state = State::WantCloseMainFn(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantCloseFn::RestartNow { next_state, } =>
                            self.state = State::WantAquireReq(next_state),
                        DoWantCloseFn::RestartWait { next_state, } =>
                            self.state = State::WantAquireReqRestart(next_state),
                        DoWantCloseFn::Fatal =>
                            return Err(()),
                    },

                State::WantReleaseReq(state) =>
                    match state.step(&mut self.core) {
                        DoWantReleaseReq::NotReady { next_state, } => {
                            self.state = State::WantReleaseReq(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantReleaseReq::Release { next_state, } =>
                            self.state = State::WantReleaseWaitFn(next_state),
                        DoWantReleaseReq::Close { next_state, } =>
                            self.state = State::WantCloseWaitFn(next_state),
                        DoWantReleaseReq::TryAgain { next_state, } =>
                            self.state = State::WantReleaseReq(next_state),
                        DoWantReleaseReq::Shutdown =>
                            return Ok(Async::Ready(())),
                    },

                State::WantReleaseWaitFn(state) =>
                    match state.step(&mut self.core) {
                        DoWantReleaseFn::NotReady { next_state, } => {
                            self.state = State::WantReleaseWaitFn(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantReleaseFn::Available { next_state, } =>
                            self.state = State::WantReleaseThenAquire(next_state),
                        DoWantReleaseFn::OutOfStock { next_state, } =>
                            self.state = State::WantReleaseReq(next_state),
                        DoWantReleaseFn::RestartNow { next_state, } =>
                            self.state = State::WantAquireReq(next_state),
                        DoWantReleaseFn::RestartWait { next_state, } =>
                            self.state = State::WantAquireReqRestart(next_state),
                        DoWantReleaseFn::Fatal =>
                            return Err(()),
                    },

                State::WantCloseWaitFn(state) =>
                    match state.step(&mut self.core) {
                        DoWantCloseFn::NotReady { next_state, } => {
                            self.state = State::WantCloseWaitFn(next_state);
                            return Ok(Async::NotReady);
                        },
                        DoWantCloseFn::RestartNow { next_state, } =>
                            self.state = State::WantAquireReq(next_state),
                        DoWantCloseFn::RestartWait { next_state, } =>
                            self.state = State::WantAquireReqRestart(next_state),
                        DoWantCloseFn::Fatal =>
                            return Err(()),
                    },
            }
        }
    }
}

struct StateWantAquireReq<S> {
    init_state: S,
}

impl<S> StateWantAquireReq<S> {
    fn step<FNI, FI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>(
        self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantAquireReq<FI::Future, S, R>
    where FNI: FnMut(S) -> FI,
          FI: IntoFuture,
    {
        debug!("State::WantAquireReq");
        match core.aquire_rx.poll() {
            Ok(Async::NotReady) =>
                DoWantAquireReq::NotReady { next_state: self, },
            Ok(Async::Ready(Some(aquire_req_pending))) =>
                DoWantAquireReq::Proceed {
                    next_state: StateWantInitFn {
                        future: (core.init_fn)(self.init_state).into_future(),
                        aquire_req_pending,
                    },
                },
            Ok(Async::Ready(None)) => {
                debug!("aquire channel depleted");
                DoWantAquireReq::Shutdown
            },
            Err(()) => {
                debug!("aquire channel outer endpoint dropped");
                DoWantAquireReq::Shutdown
            },
        }
    }
}

enum DoWantAquireReq<FUI, S, R> {
    NotReady { next_state: StateWantAquireReq<S>, },
    Proceed { next_state: StateWantInitFn<FUI, R>, },
    Shutdown,
}

struct StateWantInitFn<FUI, R> {
    future: FUI,
    aquire_req_pending: AquireReq<R>,
}

impl<FUI, R> StateWantInitFn<FUI, R> {
    fn step<FNI, FI, FNA, FA, FNRM, FNRW, FNCM, FNCW, FCW, N, S, P, Q>(
        mut self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantInitFn<FUI, FA::Future, FCW::Future, S, R>
    where FNI: FnMut(S) -> FI,
          FI: IntoFuture<Future = FUI, Item = FUI::Item, Error = FUI::Error>,
          FUI: Future<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
          FNA: FnMut(P) -> FA,
          FA: IntoFuture,
          FNCW: FnMut(Q) -> FCW,
          FCW: IntoFuture,
          N: AsRef<str>,
    {
        debug!("State::WantInitFn");
        match self.future.poll() {
            Ok(Async::NotReady) =>
                DoWantInitFn::NotReady { next_state: self, },
            Ok(Async::Ready(Resource::Available(state_avail))) => {
                core.generation += 1;
                core.aquires_count = 0;
                DoWantInitFn::Aquire {
                    next_state: StateWantAquireFn {
                        future: (core.aquire_fn)(state_avail).into_future(),
                        aquire_req_pending: self.aquire_req_pending,
                    },
                }
            },
            Ok(Async::Ready(Resource::OutOfStock(state_no_left))) => {
                warn!("init_fn gives no resource in {}", core.params.name.as_ref());
                DoWantInitFn::Close {
                    next_state: StateWantInitFnClose {
                        future: (core.close_wait_fn)(state_no_left).into_future(),
                        aquire_req_pending: self.aquire_req_pending,
                    },
                }
            },
            Err(ErrorSeverity::Recoverable { state, }) =>
                match core.params.restart_strategy {
                    RestartStrategy::RestartImmediately => {
                        info!("init_fn failed: restarting {} immediately", core.params.name.as_ref());
                        DoWantInitFn::RestartNow {
                            next_state: StateWantInitFn {
                                future: (core.init_fn)(state).into_future(),
                                aquire_req_pending: self.aquire_req_pending,
                            },
                        }
                    },
                    RestartStrategy::Delay { restart_after, } => {
                        info!("init_fn failed: restarting {} in {:?}", core.params.name.as_ref(), restart_after);
                        DoWantInitFn::RestartWait {
                            next_state: StateWantInitFnRestart {
                                future: Delay::new(Instant::now() + restart_after),
                                init_state: state,
                                aquire_req_pending: self.aquire_req_pending,
                            },
                        }
                    },
                }
            Err(ErrorSeverity::Fatal(())) => {
                error!("init_fn {} crashed with fatal error, terminating", core.params.name.as_ref());
                DoWantInitFn::Fatal
            },
        }
    }
}

enum DoWantInitFn<FUI, FUA, FUC, S, R> {
    NotReady { next_state: StateWantInitFn<FUI, R>, },
    Aquire { next_state: StateWantAquireFn<FUA, R>, },
    Close { next_state: StateWantInitFnClose<FUC, R>, },
    RestartNow { next_state: StateWantInitFn<FUI, R>, },
    RestartWait { next_state: StateWantInitFnRestart<S, R>, },
    Fatal,
}

struct StateWantInitFnRestart<S, R> {
    future: Delay,
    init_state: S,
    aquire_req_pending: AquireReq<R>,
}

impl<S, R> StateWantInitFnRestart<S, R> {
    fn step<FNI, FI, FNA, FNRM, FNRW, FNCM, FNCW, N>(
        mut self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantInitFnRestart<FI::Future, S, R>
    where FNI: FnMut(S) -> FI,
          FI: IntoFuture,
    {
        debug!("State::WantInitFnRestart");
        match self.future.poll() {
            Ok(Async::NotReady) =>
                DoWantInitFnRestart::NotReady { next_state: self, },
            Ok(Async::Ready(())) =>
                DoWantInitFnRestart::ItIsTime {
                    next_state: StateWantInitFn {
                        future: (core.init_fn)(self.init_state).into_future(),
                        aquire_req_pending: self.aquire_req_pending,
                    },
                },
            Err(error) => {
                error!("timer future crashed with fatal error: {:?}, terminating", error);
                DoWantInitFnRestart::Fatal
            },
        }
    }
}

enum DoWantInitFnRestart<FUI, S, R> {
    NotReady { next_state: StateWantInitFnRestart<S, R>, },
    ItIsTime { next_state: StateWantInitFn<FUI, R>, },
    Fatal,
}

struct StateWantAquireReqRestart<S> {
    future: Delay,
    init_state: S,
}

impl<S> StateWantAquireReqRestart<S> {
    fn step<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>(
        mut self,
        _core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantAquireReqRestart<S>
    {
        debug!("State::WantAquireReqRestart");
        match self.future.poll() {
            Ok(Async::NotReady) =>
                DoWantAquireReqRestart::NotReady { next_state: self, },
            Ok(Async::Ready(())) =>
                DoWantAquireReqRestart::ItIsTime {
                    next_state: StateWantAquireReq {
                        init_state: self.init_state,
                    },
                },
            Err(error) => {
                error!("timer future crashed with fatal error: {:?}, terminating", error);
                DoWantAquireReqRestart::Fatal
            },
        }
    }
}

enum DoWantAquireReqRestart<S> {
    NotReady { next_state: StateWantAquireReqRestart<S>, },
    ItIsTime { next_state: StateWantAquireReq<S>, },
    Fatal,
}

struct StateWantInitFnClose<FUC, R> {
    future: FUC,
    aquire_req_pending: AquireReq<R>,
}

impl<FUC, R> StateWantInitFnClose<FUC, R> {
    fn step<FNI, FI, FNA, FNRM, FNRW, FNCM, FNCW, N, S>(
        mut self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantInitFnClose<FUC, FI::Future, S, R>
    where FNI: FnMut(S) -> FI,
          FI: IntoFuture,
          FUC: Future<Item = S, Error = ()>,
          N: AsRef<str>,
    {
        debug!("State::WantInitFnClose");
        match self.future.poll() {
            Ok(Async::NotReady) =>
                DoWantInitFnClose::NotReady { next_state: self, },
            Ok(Async::Ready(state)) =>
                match core.params.restart_strategy {
                    RestartStrategy::RestartImmediately => {
                        info!("restarting {} immediately after close_wait_fn", core.params.name.as_ref());
                        DoWantInitFnClose::RestartNow {
                            next_state: StateWantInitFn {
                                future: (core.init_fn)(state).into_future(),
                                aquire_req_pending: self.aquire_req_pending,
                            },
                        }
                    },
                    RestartStrategy::Delay { restart_after, } => {
                        info!("restarting {} in {:?} after close_wait_fn", core.params.name.as_ref(), restart_after);
                        DoWantInitFnClose::RestartWait {
                            next_state: StateWantInitFnRestart {
                                future: Delay::new(Instant::now() + restart_after),
                                init_state: state,
                                aquire_req_pending: self.aquire_req_pending,
                            },
                        }
                    },
                }
            Err(()) => {
                error!("close_wait_fn crashed with fatal error, terminating");
                DoWantInitFnClose::Fatal
            },
        }
    }
}

enum DoWantInitFnClose<FUC, FUI, S, R> {
    NotReady { next_state: StateWantInitFnClose<FUC, R>, },
    RestartNow { next_state: StateWantInitFn<FUI, R>, },
    RestartWait { next_state: StateWantInitFnRestart<S, R>, },
    Fatal,
}

struct StateWantAquireFn<FUA, R> {
    future: FUA,
    aquire_req_pending: AquireReq<R>,
}

impl<FUA, R> StateWantAquireFn<FUA, R> {
    fn step<FNI, FI, FNA, FNRM, FNRW, FNCM, FNCW, N, S, P, Q>(
        mut self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantAquireFn<FUA, FI::Future, S, R, P, Q>
    where FNI: FnMut(S) -> FI,
          FI: IntoFuture,
          FUA: Future<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>>,
          N: AsRef<str>,
    {
        debug!("State::WantAquireFn");
        match self.future.poll() {
            Ok(Async::NotReady) =>
                DoWantAquireFn::NotReady { next_state: self, },
            Ok(Async::Ready((resource, resource_status))) => {
                match self.aquire_req_pending.reply_tx.send(ResourceGen { resource, generation: core.generation, }) {
                    Ok(()) =>
                        core.aquires_count += 1,
                    Err(_resource) =>
                        warn!("receiver has been dropped before resource is aquired"),
                };
                match resource_status {
                    Resource::Available(state_avail) =>
                        DoWantAquireFn::Available {
                            next_state: StateWantAquireThenRelease {
                                state_avail,
                            },
                        },
                    Resource::OutOfStock(state_no_left) =>
                        DoWantAquireFn::OutOfStock {
                            next_state: StateWantReleaseReq {
                                state_no_left,
                            },
                        },
                }
            },
            Err(ErrorSeverity::Recoverable { state, }) =>
                match core.params.restart_strategy {
                    RestartStrategy::RestartImmediately => {
                        info!("aquire_fn failed: restarting {} immediately", core.params.name.as_ref());
                        DoWantAquireFn::RestartNow {
                            next_state: StateWantInitFn {
                                future: (core.init_fn)(state).into_future(),
                                aquire_req_pending: self.aquire_req_pending,
                            },
                        }
                    },
                    RestartStrategy::Delay { restart_after, } => {
                        info!("aquire_fn failed: restarting {} in {:?}", core.params.name.as_ref(), restart_after);
                        DoWantAquireFn::RestartWait {
                            next_state: StateWantInitFnRestart {
                                future: Delay::new(Instant::now() + restart_after),
                                init_state: state,
                                aquire_req_pending: self.aquire_req_pending,
                            },
                        }
                    },
                }
            Err(ErrorSeverity::Fatal(())) => {
                error!("aquire_fn {} crashed with fatal error, terminating", core.params.name.as_ref());
                DoWantAquireFn::Fatal
            },
        }
    }
}

enum DoWantAquireFn<FUA, FUI, S, R, P, Q> {
    NotReady { next_state: StateWantAquireFn<FUA, R>, },
    Available { next_state: StateWantAquireThenRelease<P>, },
    OutOfStock { next_state: StateWantReleaseReq<Q>, },
    RestartNow { next_state: StateWantInitFn<FUI, R>, },
    RestartWait { next_state: StateWantInitFnRestart<S, R>, },
    Fatal,
}

struct StateWantAquireThenRelease<P> {
    state_avail: P,
}

impl<P> StateWantAquireThenRelease<P> {
    fn step<FNI, FNA, FA, FNRM, FNRW, FNCM, FNCW, N, R>(
        self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantAquireThenRelease<FA::Future, R, P>
    where FNA: FnMut(P) -> FA,
          FA: IntoFuture,
    {
        debug!("State::WantAquireThenRelease");
        match core.aquire_rx.poll() {
            Ok(Async::NotReady) =>
                DoWantAquireThenRelease::NotReady {
                    next_state: StateWantReleaseThenAquire {
                        state_avail: self.state_avail,
                    },
                },
            Ok(Async::Ready(Some(aquire_req_pending))) =>
                DoWantAquireThenRelease::Aquire {
                    next_state: StateWantAquireFn {
                        future: (core.aquire_fn)(self.state_avail).into_future(),
                        aquire_req_pending: aquire_req_pending,
                    },
                },
            Ok(Async::Ready(None)) => {
                debug!("aquire channel depleted");
                DoWantAquireThenRelease::Shutdown
            },
            Err(()) => {
                debug!("aquire channel outer endpoint dropped");
                DoWantAquireThenRelease::Shutdown
            },
        }
    }
}

enum DoWantAquireThenRelease<FUA, R, P> {
    NotReady { next_state: StateWantReleaseThenAquire<P>, },
    Aquire { next_state: StateWantAquireFn<FUA, R>, },
    Shutdown,
}

struct StateWantReleaseThenAquire<P> {
    state_avail: P,
}

impl<P> StateWantReleaseThenAquire<P> {
    fn step<FNI, FNA, FNRM, FRM, FNRW, FNCM, FCM, FNCW, N, R>(
        self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantReleaseThenAquire<FRM::Future, FCM::Future, P>
    where  FNRM: FnMut(P, Option<R>) -> FRM,
           FRM: IntoFuture,
           FNCM: FnMut(P) -> FCM,
           FCM: IntoFuture,
    {
        debug!("State::WantReleaseThenAquire");
        match core.release_rx.poll() {
            Ok(Async::NotReady) =>
                DoWantReleaseThenAquire::NotReady {
                    next_state: StateWantAquireThenRelease {
                        state_avail: self.state_avail,
                    },
                },
            Ok(Async::Ready(Some(release_req))) =>
                if release_req.generation == core.generation {
                    match release_req.status {
                        ResourceStatus::Reimburse(resource) => {
                            debug!("release request (resource reimbursed)");
                            DoWantReleaseThenAquire::Release {
                                next_state: StateWantReleaseFn {
                                    future: (core.release_main_fn)(self.state_avail, Some(resource)).into_future(),
                                    source: "release_main_fn",
                                },
                            }
                        },
                        ResourceStatus::ResourceLost => {
                            debug!("release request (resource lost)");
                            DoWantReleaseThenAquire::Release {
                                next_state: StateWantReleaseFn {
                                    future: (core.release_main_fn)(self.state_avail, None).into_future(),
                                    source: "release_main_fn",
                                },
                            }
                        },
                        ResourceStatus::ResourceFault => {
                            warn!("resource fault report: performing restart");
                            DoWantReleaseThenAquire::Close {
                                next_state: StateWantCloseFn {
                                    future: (core.close_main_fn)(self.state_avail).into_future(),
                                    source: "close_main_fn",
                                    force_immediately: false,
                                },
                            }
                        },
                    }
                } else {
                    debug!(
                        "skipping release request for obsolete resource (generation {} while currently is {})",
                        release_req.generation,
                        core.generation,
                    );
                    DoWantReleaseThenAquire::TryAgain { next_state: self, }
                },
            Ok(Async::Ready(None)) => {
                debug!("release channel depleted");
                DoWantReleaseThenAquire::Shutdown
            },
            Err(()) => {
                debug!("release channel outer endpoint dropped");
                DoWantReleaseThenAquire::Shutdown
            },
        }
    }
}

enum DoWantReleaseThenAquire<FURM, FUCM, P> {
    NotReady { next_state: StateWantAquireThenRelease<P>, },
    Release { next_state: StateWantReleaseFn<FURM>, },
    Close { next_state: StateWantCloseFn<FUCM>, },
    TryAgain { next_state: StateWantReleaseThenAquire<P>, },
    Shutdown,
}

struct StateWantReleaseFn<FUR> {
    future: FUR,
    source: &'static str,
}

impl<FUR> StateWantReleaseFn<FUR> {
    fn step<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, S, R, P, Q>(
        mut self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantReleaseFn<FUR, S, P, Q>
    where FUR: Future<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
          N: AsRef<str>,
    {
        debug!("State::WantReleaseFn ({})", self.source);
        match self.future.poll() {
            Ok(Async::NotReady) =>
                DoWantReleaseFn::NotReady { next_state: self, },
            Ok(Async::Ready(Resource::Available(state_avail))) => {
                core.aquires_count = core.aquires_count.saturating_sub(1);
                DoWantReleaseFn::Available {
                    next_state: StateWantReleaseThenAquire {
                        state_avail,
                    },
                }
            },
            Ok(Async::Ready(Resource::OutOfStock(state_no_left))) => {
                core.aquires_count = core.aquires_count.saturating_sub(1);
                DoWantReleaseFn::OutOfStock {
                    next_state: StateWantReleaseReq {
                        state_no_left,
                    },
                }
            },
            Err(ErrorSeverity::Recoverable { state, }) =>
                match core.params.restart_strategy {
                    RestartStrategy::RestartImmediately => {
                        info!("{} failed: restarting {} immediately", self.source, core.params.name.as_ref());
                        DoWantReleaseFn::RestartNow {
                            next_state: StateWantAquireReq {
                                init_state: state,
                            },
                        }
                    },
                    RestartStrategy::Delay { restart_after, } => {
                        info!("{} failed: restarting {} in {:?}", self.source, core.params.name.as_ref(), restart_after);
                        DoWantReleaseFn::RestartWait {
                            next_state: StateWantAquireReqRestart {
                                future: Delay::new(Instant::now() + restart_after),
                                init_state: state,
                            },
                        }
                    },
                }
            Err(ErrorSeverity::Fatal(())) => {
                error!("{} {} crashed with fatal error, terminating", self.source, core.params.name.as_ref());
                DoWantReleaseFn::Fatal
            },
        }
    }
}

enum DoWantReleaseFn<FUR, S, P, Q> {
    NotReady { next_state: StateWantReleaseFn<FUR>, },
    Available { next_state: StateWantReleaseThenAquire<P>, },
    OutOfStock { next_state: StateWantReleaseReq<Q>, },
    RestartNow { next_state: StateWantAquireReq<S>, },
    RestartWait { next_state: StateWantAquireReqRestart<S>, },
    Fatal,
}

struct StateWantCloseFn<FUC> {
    future: FUC,
    source: &'static str,
    force_immediately: bool,
}

impl<FUC> StateWantCloseFn<FUC> {
    fn step<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, S, R>(
        mut self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantCloseFn<FUC, S>
    where FUC: Future<Item = S, Error = ()>,
          N: AsRef<str>,
    {
        debug!("State::WantCloseFn ({})", self.source);
        match self.future.poll() {
            Ok(Async::NotReady) =>
                DoWantCloseFn::NotReady { next_state: self, },
            Ok(Async::Ready(state)) =>
                match (self.force_immediately, &core.params.restart_strategy) {
                    (true, _) | (false, &RestartStrategy::RestartImmediately) => {
                        info!("restarting {} immediately after {}", core.params.name.as_ref(), self.source);
                        DoWantCloseFn::RestartNow {
                            next_state: StateWantAquireReq {
                                init_state: state,
                            },
                        }
                    },
                    (false, &RestartStrategy::Delay { restart_after, }) => {
                        info!("restarting {} in {:?} after {}", core.params.name.as_ref(), restart_after, self.source);
                        DoWantCloseFn::RestartWait {
                            next_state: StateWantAquireReqRestart {
                                future: Delay::new(Instant::now() + restart_after),
                                init_state: state,
                            },
                        }
                    },
                }
            Err(()) => {
                error!("{} crashed with fatal error, terminating", self.source);
                DoWantCloseFn::Fatal
            },
        }
    }
}

enum DoWantCloseFn<FUC, S> {
    NotReady { next_state: StateWantCloseFn<FUC>, },
    RestartNow { next_state: StateWantAquireReq<S>, },
    RestartWait { next_state: StateWantAquireReqRestart<S>, },
    Fatal,
}

struct StateWantReleaseReq<Q> {
    state_no_left: Q,
}

impl<Q> StateWantReleaseReq<Q> {
    fn step<FNI, FNA, FNRM, FNRW, FRW, FNCM, FNCW, FCW, N, R>(
        self,
        core: &mut Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    )
        -> DoWantReleaseReq<FRW::Future, FCW::Future, Q>
    where  FNRW: FnMut(Q, Option<R>) -> FRW,
           FRW: IntoFuture,
           FNCW: FnMut(Q) -> FCW,
           FCW: IntoFuture,
           N: AsRef<str>,
    {
        debug!("State::WantReleaseReq");
        if core.aquires_count == 0 {
            info!("{} runs out of resources, performing restart", core.params.name.as_ref());
            return DoWantReleaseReq::Close {
                next_state: StateWantCloseFn {
                    future: (core.close_wait_fn)(self.state_no_left).into_future(),
                    source: "close_wait_fn",
                    force_immediately: true,
                },
            }
        }

        match core.release_rx.poll() {
            Ok(Async::NotReady) =>
                DoWantReleaseReq::NotReady { next_state: self, },
            Ok(Async::Ready(Some(release_req))) =>
                if release_req.generation == core.generation {
                    match release_req.status {
                        ResourceStatus::Reimburse(resource) => {
                            debug!("release request (resource reimbursed)");
                            DoWantReleaseReq::Release {
                                next_state: StateWantReleaseFn {
                                    future: (core.release_wait_fn)(self.state_no_left, Some(resource)).into_future(),
                                    source: "release_wait_fn",
                                },
                            }
                        },
                        ResourceStatus::ResourceLost => {
                            debug!("release request (resource lost)");
                            DoWantReleaseReq::Release {
                                next_state: StateWantReleaseFn {
                                    future: (core.release_wait_fn)(self.state_no_left, None).into_future(),
                                    source: "release_wait_fn",
                                },
                            }
                        },
                        ResourceStatus::ResourceFault => {
                            warn!("resource fault report: performing restart");
                            DoWantReleaseReq::Close {
                                next_state: StateWantCloseFn {
                                    future: (core.close_wait_fn)(self.state_no_left).into_future(),
                                    source: "close_wait_fn",
                                    force_immediately: false,
                                },
                            }
                        },
                    }
                } else {
                    debug!(
                        "skipping release request for obsolete resource (generation {} while currently is {})",
                        release_req.generation,
                        core.generation,
                    );
                    DoWantReleaseReq::TryAgain { next_state: self, }
                },
            Ok(Async::Ready(None)) => {
                debug!("release channel depleted");
                DoWantReleaseReq::Shutdown
            },
            Err(()) => {
                debug!("release channel outer endpoint dropped");
                DoWantReleaseReq::Shutdown
            },
        }
    }
}

enum DoWantReleaseReq<FURW, FUCW, Q> {
    NotReady { next_state: StateWantReleaseReq<Q>, },
    Release { next_state: StateWantReleaseFn<FURW>, },
    Close { next_state: StateWantCloseFn<FUCW>, },
    TryAgain { next_state: StateWantReleaseReq<Q>, },
    Shutdown,
}

#[derive(Clone, PartialEq, Debug)]
pub enum UsingError<E> {
    ResourceTaskGone,
    Fatal(E),
}

pub enum UsingResource<R> {
    Lost,
    Reimburse(R),
}

impl<R> LodeResource<R> {
    pub fn steal_resource(self) -> impl Future<Item = (R, LodeResource<R>), Error = ()> {
        let LodeResource { aquire_tx, release_tx, } = self;
        let (resource_tx, resource_rx) = oneshot::channel();
        aquire_tx
            .send(AquireReq { reply_tx: resource_tx, })
            .map_err(|_send_error| {
                warn!("resource task is gone while aquiring resource");
            })
            .and_then(move |aquire_tx| {
                resource_rx
                    .map_err(|oneshot::Canceled| {
                        warn!("resouce task is gone while receiving aquired resource");
                    })
                    .and_then(move |ResourceGen { resource, generation, }| {
                        release_tx
                            .send(ReleaseReq { generation, status: ResourceStatus::ResourceLost, })
                            .map_err(|_send_error| {
                                warn!("resource task is gone while releasing resource");
                            })
                            .map(move |release_tx| {
                                (resource, LodeResource { aquire_tx, release_tx, })
                            })
                    })
            })
    }

    pub fn using_resource_loop<F, T, E, S, FI>(
        self,
        state: S,
        using_fn: F,
    )
        -> impl Future<Item = (T, LodeResource<R>), Error = UsingError<E>>
    where F: FnMut(R, S) -> FI,
          FI: IntoFuture<Item = (UsingResource<R>, Loop<T, S>), Error = ErrorSeverity<S, E>>
    {
        loop_fn(
            (self, using_fn, state),
            move |(LodeResource { aquire_tx, release_tx, }, mut using_fn, state)| {
                debug!("using_resource_loop: aquiring resource");
                let (resource_tx, resource_rx) = oneshot::channel();
                aquire_tx
                    .send(AquireReq { reply_tx: resource_tx, })
                    .map_err(|_send_error| {
                        warn!("resource task is gone while aquiring resource");
                        UsingError::ResourceTaskGone
                    })
                    .and_then(move |aquire_tx| {
                        resource_rx
                            .map_err(|oneshot::Canceled| {
                                warn!("resouce task is gone while receiving aquired resource");
                                UsingError::ResourceTaskGone
                            })
                            .and_then(move |ResourceGen { resource, generation, }| {
                                debug!("using_resource_loop: resource aquired, passing control to user proc");
                                using_fn(resource, state)
                                    .into_future()
                                    .then(move |using_result| {
                                        match using_result {
                                            Ok((maybe_resource, loop_action)) => {
                                                debug!(
                                                    "using_resource_loop: resource: {}, loop action: {}, releasing resource",
                                                    match maybe_resource {
                                                        UsingResource::Lost =>
                                                            "lost",
                                                        UsingResource::Reimburse(..) =>
                                                            "reimbursed",
                                                    },
                                                    match loop_action {
                                                        Loop::Break(..) =>
                                                            "break",
                                                        Loop::Continue(..) =>
                                                            "continue",
                                                    },
                                                );
                                                release_tx
                                                    .unbounded_send(ReleaseReq {
                                                        generation,
                                                        status: match maybe_resource {
                                                            UsingResource::Lost =>
                                                                ResourceStatus::ResourceLost,
                                                            UsingResource::Reimburse(resource) =>
                                                                ResourceStatus::Reimburse(resource),
                                                        },
                                                    })
                                                    .map_err(|_send_error| {
                                                        warn!("resource task is gone while releasing resource");
                                                        UsingError::ResourceTaskGone
                                                    })
                                                    .map(move |()| {
                                                        let lode = LodeResource { aquire_tx, release_tx, };
                                                        match loop_action {
                                                            Loop::Break(item) =>
                                                                Loop::Break((item, lode)),
                                                            Loop::Continue(state) =>
                                                                Loop::Continue((lode, using_fn, state)),
                                                        }
                                                    })
                                            },
                                            Err(error) => {
                                                debug!("using_resource_loop: an error occurred, releasing resource");
                                                release_tx
                                                    .unbounded_send(ReleaseReq { generation, status: ResourceStatus::ResourceFault, })
                                                    .map_err(|_send_error| {
                                                        warn!("resource task is gone while releasing resource");
                                                        UsingError::ResourceTaskGone
                                                    })
                                                    .and_then(move |()| {
                                                        match error {
                                                            ErrorSeverity::Recoverable { state, } =>
                                                                Ok(Loop::Continue((
                                                                    LodeResource { aquire_tx, release_tx, },
                                                                    using_fn,
                                                                    state,
                                                                ))),
                                                            ErrorSeverity::Fatal(fatal_error) =>
                                                                Err(UsingError::Fatal(fatal_error)),
                                                        }
                                                    })
                                            }
                                        }
                                    })
                            })
                    })
            })
    }

    pub fn using_resource_once<F, T, E, S, FI>(
        self,
        state: S,
        mut using_fn: F,
    )
        -> impl Future<Item = (T, LodeResource<R>), Error = UsingError<E>>
    where F: FnMut(R, S) -> FI,
          FI: IntoFuture<Item = (UsingResource<R>, T), Error = ErrorSeverity<S, E>>
    {
        self.using_resource_loop(
            state,
            move |resource, state| {
                using_fn(resource, state)
                    .into_future()
                    .map(|(using_resource, value)| (using_resource, Loop::Break(value)))
            },
        )
    }
}
