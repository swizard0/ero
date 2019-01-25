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
    blend::{
        Gone,
        Blender,
        Decompose,
        ErrorEvent,
        DecomposeZip,
    },
    supervisor::Supervisor,
};

// pub mod uniq;
// pub mod shared;
// pub mod stream;

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

struct LodeFuture<FNI, FI, FNA, FA, FNRM, FRM, FNRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q>
where FI: IntoFuture,
      FA: IntoFuture,
      FRM: IntoFuture,
      FCM: IntoFuture,
      FCW: IntoFuture,
{
    core: Core<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, R>,
    state: State<FI, FA, FRM, FCM, FCW, S, R, P, Q>,
}

enum State<FI, FA, FRM, FCM, FCW, S, R, P, Q>
where FI: IntoFuture,
      FA: IntoFuture,
      FRM: IntoFuture,
      FCM: IntoFuture,
      FCW: IntoFuture,
{
    Invalid,
    WantAquireReq(StateWantAquireReq<S>),
    WantInitFn(StateWantInitFn<FI::Future, R>),
    WantInitFnRestart(StateWantInitFnRestart<S, R>),
    WantInitFnClose(StateWantInitFnClose<FCW::Future, R>),
    WantAquireFn(StateWantAquireFn<FA::Future, R>),
    WantAquireThenRelease(StateWantAquireThenRelease<P>),
    WantReleaseThenAquire(StateWantReleaseThenAquire<P>),
    WantReleaseMainFn(StateWantReleaseMainFn<FRM::Future>),
    WantCloseMainFn(StateWantCloseMainFn<FCM::Future>),
    WantReleaseWaitFn(StateWantReleaseWaitFn<Q>),
    WantCloseWaitFn(StateWantCloseWaitFn<FCW::Future, R>),
}

impl<FNI, FI, FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q> Future
    for LodeFuture<FNI, FI, FNA, FA, FNRM, FRM, FNRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q>
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
                            self.state = State::WantReleaseWaitFn(next_state),
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
                    unimplemented!(),

                State::WantCloseMainFn(state) =>
                    unimplemented!(),

                State::WantReleaseWaitFn(state) =>
                    unimplemented!(),

                State::WantCloseWaitFn(state) =>
                    unimplemented!(),
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
                        info!("restarting {} immediately after close", core.params.name.as_ref());
                        DoWantInitFnClose::RestartNow {
                            next_state: StateWantInitFn {
                                future: (core.init_fn)(state).into_future(),
                                aquire_req_pending: self.aquire_req_pending,
                            },
                        }
                    },
                    RestartStrategy::Delay { restart_after, } => {
                        info!("restarting {} in {:?} after close", core.params.name.as_ref(), restart_after);
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
                            next_state: StateWantReleaseWaitFn {
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
    OutOfStock { next_state: StateWantReleaseWaitFn<Q>, },
    RestartNow { next_state: StateWantInitFn<FUI, R>, },
    RestartWait { next_state: StateWantInitFnRestart<S, R>, },
    Fatal,
}

struct StateWantAquireThenRelease<P> {
    state_avail: P,
}

impl<P> StateWantAquireThenRelease<P> {
    fn step<FNI, FNA, FA, FNRM, FNRW, FNCM, FNCW, N, R>(
        mut self,
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
        mut self,
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
                                next_state: StateWantReleaseMainFn {
                                    future: (core.release_main_fn)(self.state_avail, Some(resource)).into_future(),
                                },
                            }
                        },
                        ResourceStatus::ResourceLost => {
                            debug!("release request (resource lost)");
                            DoWantReleaseThenAquire::Release {
                                next_state: StateWantReleaseMainFn {
                                    future: (core.release_main_fn)(self.state_avail, None).into_future(),
                                },
                            }
                        },
                        ResourceStatus::ResourceFault => {
                            warn!("resource fault report: performing restart");
                            DoWantReleaseThenAquire::Close {
                                next_state: StateWantCloseMainFn {
                                    future: (core.close_main_fn)(self.state_avail).into_future(),
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
    Release { next_state: StateWantReleaseMainFn<FURM>, },
    Close { next_state: StateWantCloseMainFn<FUCM>, },
    TryAgain { next_state: StateWantReleaseThenAquire<P>, },
    Shutdown,
}

struct StateWantReleaseMainFn<FURM> {
    future: FURM,
}

struct StateWantCloseMainFn<FUCM> {
    future: FUCM,
}

struct StateWantReleaseWaitFn<Q> {
    state_no_left: Q,
}

struct StateWantCloseWaitFn<FCW, R> {
    future: FCW,
    aquire_req_pending: AquireReq<R>,
}


// pub fn spawn_link<FNI, FI, FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q>(
//     supervisor: &Supervisor,
//     params: Params<N>,
//     init_state: S,
//     init_fn: FNI,
//     aquire_fn: FNA,
//     release_main_fn: FNRM,
//     release_wait_fn: FNRW,
//     close_main_fn: FNCM,
//     close_wait_fn: FNCW,
// )
//     -> LodeResource<R>
// where FNI: FnMut(S) -> FI + Send + 'static,
//       FI: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
//       FI::Future: Send,
//       FNA: FnMut(P) -> FA + Send + 'static,
//       FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>> + 'static,
//       FA::Future: Send,
//       FNRM: FnMut(P, Option<R>) -> FRM + Send + 'static,
//       FRM: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
//       FRM::Future: Send,
//       FNRW: FnMut(Q, Option<R>) -> FRW + Send + 'static,
//       FRW: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>> + 'static,
//       FRW::Future: Send,
//       FNCM: FnMut(P) -> FCM + Send + 'static,
//       FCM: IntoFuture<Item = S, Error = ()> + Send + 'static,
//       FCM::Future: Send,
//       FNCW: FnMut(Q) -> FCW + Send + 'static,
//       FCW: IntoFuture<Item = S, Error = ()> + Send + 'static,
//       FCW::Future: Send,
//       N: AsRef<str> + Send + 'static,
//       S: Send + 'static,
//       R: Send + 'static,
//       P: Send + 'static,
//       Q: Send + 'static,
// {
//     let (aquire_tx_stream, aquire_rx_stream) = mpsc::channel(0);
//     let (release_tx_stream, release_rx_stream) = mpsc::unbounded();

//     let task_future = loop_fn(
//         RestartState {
//             peers: Peers {
//                 aquire_rx: aquire_rx_stream,
//                 release_rx: release_rx_stream,
//             },
//             core: Core {
//                 aquires_count: 0,
//                 generation: 0,
//                 params,
//                 vtable: VTable {
//                     aquire_fn,
//                     release_main_fn,
//                     release_wait_fn,
//                     close_main_fn,
//                     close_wait_fn,
//                 },
//             },
//             aquire_req_pending: None,
//             init_state,
//             init_fn,
//         },
//         restart_loop,
//     );
//     supervisor.spawn_link(task_future);

//     LodeResource {
//         aquire_tx: aquire_tx_stream,
//         release_tx: release_tx_stream,
//     }
// }

// struct VTable<FNA, FNRM, FNRW, FNCM, FNCW> {
//     aquire_fn: FNA,
//     release_main_fn: FNRM,
//     release_wait_fn: FNRW,
//     close_main_fn: FNCM,
//     close_wait_fn: FNCW,
// }

// struct Peers<R> {
//     aquire_rx: AquirePeer<R>,
//     release_rx: ReleasePeer<R>,
// }

// struct Core<FNA, FNRM, FNRW, FNCM, FNCW, N> {
//     params: Params<N>,
//     vtable: VTable<FNA, FNRM, FNRW, FNCM, FNCW>,
//     aquires_count: usize,
//     generation: Generation,
// }

// struct RestartState<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, S, R> {
//     peers: Peers<R>,
//     core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//     aquire_req_pending: Option<AquireReq<R>>,
//     init_state: S,
//     init_fn: FNI,
// }

// fn restart_loop<FNI, FS, FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q>(
//     restart_state: RestartState<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, S, R>,
// )
//     -> impl Future<Item = Loop<(), RestartState<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, S, R>>, Error = ()>
// where FNI: FnMut(S) -> FS + Send + 'static,
//       FS: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
//       FNA: FnMut(P) -> FA,
//       FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>>,
//       FNRM: FnMut(P, Option<R>) -> FRM,
//       FRM: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
//       FNRW: FnMut(Q, Option<R>) -> FRW,
//       FRW: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
//       FNCM: FnMut(P) -> FCM,
//       FCM: IntoFuture<Item = S, Error = ()>,
//       FNCW: FnMut(Q) -> FCW,
//       FCW: IntoFuture<Item = S, Error = ()>,
//       N: AsRef<str>,
// {
//     let RestartState { peers, mut core, init_state, mut init_fn, mut aquire_req_pending, } = restart_state;
//     let future = if let Some(aquire_req) = aquire_req_pending.take() {
//         Either::A(result(Ok(AquireWait::Arrived { aquire_req, peers, })))
//     } else {
//         Either::B(wait_for_aquire(peers))
//     };
//     future
//         .and_then(move |aquire_wait| {
//             match aquire_wait {
//                 AquireWait::Arrived { aquire_req, peers, } => {
//                     let future = init_fn(init_state)
//                         .into_future()
//                         .then(move |maybe_lode_state| {
//                             match maybe_lode_state {
//                                 Ok(Resource::Available(state_avail)) => {
//                                     core.generation += 1;
//                                     let future =
//                                         loop_fn(OuterState { peers, core, state_avail, aquire_req_pending: Some(aquire_req), }, outer_loop)
//                                         .then(move |inner_loop_result| {
//                                             match inner_loop_result {
//                                                 Ok(BreakOuter::Shutdown) =>
//                                                     Either::A(result(Ok(Loop::Break(())))),
//                                                 Ok(BreakOuter::RequireRestart { peers, core, init_state, aquire_req_pending, }) => {
//                                                     let future = proceed_with_restart(RestartState {
//                                                         peers, core, init_state, init_fn, aquire_req_pending,
//                                                     });
//                                                     Either::B(future)
//                                                 },
//                                                 Err(()) =>
//                                                     Either::A(result(Err(()))),
//                                             }
//                                         });
//                                     Either::A(Either::A(future))
//                                 },
//                                 Ok(Resource::OutOfStock(state_no_left)) => {
//                                     warn!("initializing gives no resource in {}", core.params.name.as_ref());
//                                     let future = (core.vtable.close_wait_fn)(state_no_left)
//                                         .into_future()
//                                         .and_then(move |init_state| {
//                                             proceed_with_restart(RestartState {
//                                                 aquire_req_pending: Some(aquire_req),
//                                                 peers, core, init_state, init_fn,
//                                             })
//                                         });
//                                     Either::A(Either::B(future))
//                                 },
//                                 Err(ErrorSeverity::Recoverable { state: init_state, }) => {
//                                     let future = proceed_with_restart(RestartState {
//                                         aquire_req_pending: Some(aquire_req),
//                                         peers, core, init_state, init_fn,
//                                     });
//                                     Either::B(Either::A(future))
//                                 },
//                                 Err(ErrorSeverity::Fatal(())) => {
//                                     error!("{} crashed with fatal error, terminating", core.params.name.as_ref());
//                                     Either::B(Either::B(result(Err(()))))
//                                 },
//                             }
//                         });
//                     Either::A(future)
//                 },
//                 AquireWait::Shutdown =>
//                     Either::B(result(Ok(Loop::Break(())))),
//             }
//         })
// }

// enum AquireWait<R> {
//     Arrived { aquire_req: AquireReq<R>, peers: Peers<R>, },
//     Shutdown,
// }

// fn wait_for_aquire<R>(peers: Peers<R>) -> impl Future<Item = AquireWait<R>, Error = ()> {
//     let Peers { aquire_rx, release_rx, } = peers;
//     aquire_rx
//         .into_future()
//         .then(move |await_result| {
//             match await_result {
//                 Ok((Some(aquire_req), aquire_rx)) => {
//                     debug!("wait_for_aquire: aquire request");
//                     Ok(AquireWait::Arrived {
//                         aquire_req,
//                         peers: Peers { aquire_rx, release_rx, },
//                     })
//                 },
//                 Ok((None, _aquire_rx)) => {
//                     debug!("wait_for_aquire: release channel depleted");
//                     Ok(AquireWait::Shutdown)
//                 },
//                 Err(((), _aquire_rx)) => {
//                     debug!("wait_for_aquire: aquire channel outer endpoint dropped");
//                     Ok(AquireWait::Shutdown)
//                 },
//             }
//         })
// }

// enum BreakOuter<FNA, FNRM, FNRW, FNCM, FNCW, N, S, R> {
//     Shutdown,
//     RequireRestart {
//         peers: Peers<R>,
//         core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//         init_state: S,
//         aquire_req_pending: Option<AquireReq<R>>,
//     },
// }

// struct OuterState<FNA, FNRM, FNRW, FNCM, FNCW, N, R, P> {
//     peers: Peers<R>,
//     core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//     state_avail: P,
//     aquire_req_pending: Option<AquireReq<R>>,
// }

// fn outer_loop<FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q>(
//     outer_state: OuterState<FNA, FNRM, FNRW, FNCM, FNCW, N, R, P>,
// )
//     -> impl Future<Item = Loop<BreakOuter<FNA, FNRM, FNRW, FNCM, FNCW, N, S, R>, OuterState<FNA, FNRM, FNRW, FNCM, FNCW, N, R, P>>, Error = ()>
// where FNA: FnMut(P) -> FA,
//       FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>>,
//       FNRM: FnMut(P, Option<R>) -> FRM,
//       FRM: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
//       FNRW: FnMut(Q, Option<R>) -> FRW,
//       FRW: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
//       FNCM: FnMut(P) -> FCM,
//       FCM: IntoFuture<Item = S, Error = ()>,
//       FNCW: FnMut(Q) -> FCW,
//       FCW: IntoFuture<Item = S, Error = ()>,
//       N: AsRef<str>,
// {
//     let OuterState { peers, core, state_avail, aquire_req_pending, } = outer_state;
//     let blender = Blender::new()
//         .add(peers.aquire_rx)
//         .add(peers.release_rx)
//         .finish_sources()
//         .fold(Either::B, Either::B)
//         .fold(Either::A, Either::A)
//         .finish();
//     loop_fn(MainState { blender, core, state_avail, aquire_req_pending, }, main_loop)
//         .and_then(move |main_loop_result| {
//             match main_loop_result {
//                 BreakMain::Shutdown =>
//                     Either::A(result(Ok(Loop::Break(BreakOuter::Shutdown)))),
//                 BreakMain::RequireRestart { peers, core, init_state, aquire_req_pending, } =>
//                     Either::A(result(Ok(Loop::Break(BreakOuter::RequireRestart { peers, core, init_state, aquire_req_pending, })))),
//                 BreakMain::WaitRelease { peers, core, state_no_left, } => {
//                     let Peers { aquire_rx, release_rx, } = peers;
//                     let future = loop_fn(WaitState { release_rx, core, state_no_left, }, wait_loop)
//                         .map(move |wait_loop_result| {
//                             match wait_loop_result {
//                                 BreakWait::Shutdown =>
//                                     Loop::Break(BreakOuter::Shutdown),
//                                 BreakWait::RequireRestart { release_rx, core, init_state, } =>
//                                     Loop::Break(BreakOuter::RequireRestart {
//                                         peers: Peers { aquire_rx, release_rx, },
//                                         aquire_req_pending: None,
//                                         core, init_state,
//                                     }),
//                                 BreakWait::ProceedMain { release_rx, core, state_avail, } =>
//                                     Loop::Continue(OuterState {
//                                         peers: Peers { aquire_rx, release_rx, },
//                                         aquire_req_pending: None,
//                                         core, state_avail,
//                                     }),
//                             }
//                         });
//                     Either::B(future)
//                 },
//             }
//         })
// }

// enum BreakMain<FNA, FNRM, FNRW, FNCM, FNCW, N, S, R, Q> {
//     Shutdown,
//     RequireRestart {
//         peers: Peers<R>,
//         core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//         init_state: S,
//         aquire_req_pending: Option<AquireReq<R>>,
//     },
//     WaitRelease {
//         peers: Peers<R>,
//         core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//         state_no_left: Q,
//     },
// }

// struct MainState<FNA, FNRM, FNRW, FNCM, FNCW, N, R, P, B> {
//     blender: B,
//     core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//     state_avail: P,
//     aquire_req_pending: Option<AquireReq<R>>,
// }

// type BlenderItem<R, B> = (Either<AquireReq<R>, ReleaseReq<R>>, B);
// type BlenderError<R> = Either<ErrorEvent<(ReleasePeer<R>, ()), Gone, (), ()>, ErrorEvent<(), Gone, (AquirePeer<R>, ()), ()>>;

// fn main_loop<FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q, B>(
//     main_state: MainState<FNA, FNRM, FNRW, FNCM, FNCW, N, R, P, B>,
// )
//     -> impl Future<Item = Loop<BreakMain<FNA, FNRM, FNRW, FNCM, FNCW, N, S, R, Q>, MainState<FNA, FNRM, FNRW, FNCM, FNCW, N, R, P, B>>, Error = ()>
// where FNA: FnMut(P) -> FA,
//       FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>>,
//       FNRM: FnMut(P, Option<R>) -> FRM,
//       FRM: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
//       FNRW: FnMut(Q, Option<R>) -> FRW,
//       FRW: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
//       FNCM: FnMut(P) -> FCM,
//       FCM: IntoFuture<Item = S, Error = ()>,
//       FNCW: FnMut(Q) -> FCW,
//       FCW: IntoFuture<Item = S, Error = ()>,
//       N: AsRef<str>,
//       B: Future<Item = BlenderItem<R, B>, Error = BlenderError<R>> + Decompose<Parts = (AquirePeer<R>, (ReleasePeer<R>, ()))>,
// {
//     let MainState {
//         core: Core { params, mut vtable, aquires_count, generation, },
//         blender,
//         state_avail,
//         aquire_req_pending,
//     } = main_state;

//     enum AquireProcess<FNA, FNRM, FNRW, FNCM, FNCW, N, S, R, P, Q> {
//         Proceed { core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>, state_avail: P, },
//         WaitRelease { core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>, state_no_left: Q, },
//         RequireRestart {
//             core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//             init_state: S,
//             aquire_req_pending: Option<AquireReq<R>>,
//         }
//     }

//     let future = if let Some(AquireReq { reply_tx, }) = aquire_req_pending {
//         debug!("main_loop: process the aquire request");
//         let future = (vtable.aquire_fn)(state_avail)
//             .into_future()
//             .then(move |aquire_result| {
//                 match aquire_result {
//                     Ok((resource, resource_status)) => {
//                         let aquires_count = match reply_tx.send(ResourceGen { resource, generation, }) {
//                             Ok(()) =>
//                                 aquires_count + 1,
//                             Err(_resource) => {
//                                 warn!("receiver has been dropped before resource is aquired");
//                                 aquires_count
//                             },
//                         };
//                         let core = Core { params, vtable, aquires_count, generation, };
//                         match resource_status {
//                             Resource::Available(state_avail) =>
//                                 Ok(AquireProcess::Proceed { core, state_avail, }),
//                             Resource::OutOfStock(state_no_left) =>
//                                 Ok(AquireProcess::WaitRelease { core, state_no_left, }),
//                         }

//                     },
//                     Err(ErrorSeverity::Recoverable { state: init_state, }) => {
//                         Ok(AquireProcess::RequireRestart {
//                             core: Core { params, vtable, aquires_count, generation, },
//                             aquire_req_pending: Some(AquireReq { reply_tx, }),
//                             init_state,
//                         })
//                     },
//                     Err(ErrorSeverity::Fatal(())) => {
//                         error!("{} crashed with fatal error, terminating", params.name.as_ref());
//                         Err(())
//                     },
//                 }
//             });
//         Either::A(future)
//     } else {
//         Either::B(result(Ok(AquireProcess::Proceed {
//             core: Core { params, vtable, aquires_count, generation, },
//             state_avail,
//         })))
//     };

//     future
//         .and_then(move |aquire_process| {
//             match aquire_process {
//                 AquireProcess::Proceed { core, state_avail, } => {
//                     let Core { params, mut vtable, aquires_count, generation, } = core;
//                     let future = blender
//                         .then(move |await_result| {
//                             match await_result {
//                                 Ok((Either::A(aquire_req), blender)) => {
//                                     debug!("main_loop: aquire request");
//                                     Either::B(result(Ok(Loop::Continue(MainState {
//                                         core: Core { params, vtable, aquires_count, generation, },
//                                         aquire_req_pending: Some(aquire_req),
//                                         blender,
//                                         state_avail,
//                                     }))))
//                                 },
//                                 Ok((Either::B(release_req), blender)) => {
//                                     if release_req.generation == generation {
//                                         let maybe_resource = match release_req.status {
//                                             ResourceStatus::Reimburse(resource) => {
//                                                 debug!("main_loop: release request (resource reimbursed)");
//                                                 Some(Some(resource))
//                                             },
//                                             ResourceStatus::ResourceLost => {
//                                                 debug!("main_loop: release request (resource lost)");
//                                                 Some(None)
//                                             },
//                                             ResourceStatus::ResourceFault => {
//                                                 debug!("main_loop: release request (resource fault)");
//                                                 None
//                                             },
//                                         };
//                                         let future = if let Some(released_resource) = maybe_resource {
//                                             // resource is actually released
//                                             let future = (vtable.release_main_fn)(state_avail, released_resource)
//                                                 .into_future()
//                                                 .then(move |release_result| {
//                                                     let core = Core {
//                                                         aquires_count: if aquires_count > 0 { aquires_count - 1 } else { 0 },
//                                                         params, vtable, generation,
//                                                     };
//                                                     match release_result {
//                                                         Ok(Resource::Available(state_avail)) =>
//                                                             Ok(Loop::Continue(MainState { blender, core, state_avail, aquire_req_pending: None, })),
//                                                         Ok(Resource::OutOfStock(state_no_left)) => {
//                                                             let (aquire_rx, (release_rx, ())) = blender.decompose();
//                                                             let peers = Peers { aquire_rx, release_rx, };
//                                                             Ok(Loop::Break(BreakMain::WaitRelease { peers, core, state_no_left, }))
//                                                         },
//                                                         Err(ErrorSeverity::Recoverable { state: init_state, }) => {
//                                                             let (aquire_rx, (release_rx, ())) = blender.decompose();
//                                                             let peers = Peers { aquire_rx, release_rx, };
//                                                             Ok(Loop::Break(BreakMain::RequireRestart {
//                                                                 peers, core, init_state, aquire_req_pending: None,
//                                                             }))
//                                                         },
//                                                         Err(ErrorSeverity::Fatal(())) => {
//                                                             error!("{} crashed with fatal error, terminating", core.params.name.as_ref());
//                                                             Err(())
//                                                         },
//                                                     }
//                                                 });
//                                             Either::A(future)
//                                         } else {
//                                             // something wrong with resource, schedule restart
//                                             warn!("resource fault report: performing restart");
//                                             let future = (vtable.close_main_fn)(state_avail)
//                                                 .into_future()
//                                                 .map(move |init_state| {
//                                                     let (aquire_rx, (release_rx, ())) = blender.decompose();
//                                                     let peers = Peers { aquire_rx, release_rx, };
//                                                     Loop::Break(BreakMain::RequireRestart {
//                                                         core: Core {
//                                                             aquires_count: if aquires_count > 0 { aquires_count - 1 } else { 0 },
//                                                             params, vtable, generation,
//                                                         },
//                                                         aquire_req_pending: None,
//                                                         init_state,
//                                                         peers,
//                                                     })
//                                                 });
//                                             Either::B(future)
//                                         };
//                                         Either::A(future)
//                                     } else {
//                                         debug!("main_loop: skipping obsolete release request");
//                                         Either::B(result(Ok(Loop::Continue(MainState {
//                                             core: Core { params, vtable, aquires_count, generation, },
//                                             aquire_req_pending: None,
//                                             state_avail,
//                                             blender,
//                                         }))))
//                                     }
//                                 },
//                                 Err(Either::A(ErrorEvent::Depleted {
//                                     decomposed: DecomposeZip { left_dir: (_release_rx, ()), myself: Gone, right_rev: (), },
//                                 })) => {
//                                     debug!("main_loop: aquire channel depleted");
//                                     Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
//                                 },
//                                 Err(Either::A(ErrorEvent::Error {
//                                     error: (),
//                                     decomposed: DecomposeZip { left_dir: (_release_rx, ()), myself: Gone, right_rev: (), },
//                                 })) => {
//                                     debug!("main_loop: aquire channel outer endpoint dropped");
//                                     Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
//                                 },
//                                 Err(Either::B(ErrorEvent::Depleted {
//                                     decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_aquire_rx, ()), },
//                                 })) => {
//                                     debug!("main_loop: release channel depleted");
//                                     Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
//                                 },
//                                 Err(Either::B(ErrorEvent::Error {
//                                     error: (),
//                                     decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (_aquire_rx, ()), },
//                                 })) => {
//                                     debug!("main_loop: release channel outer endpoint dropped");
//                                     Either::B(result(Ok(Loop::Break(BreakMain::Shutdown))))
//                                 },
//                             }
//                         });
//                     Either::A(future)
//                 },
//                 AquireProcess::WaitRelease { core, state_no_left, } => {
//                     let (aquire_rx, (release_rx, ())) = blender.decompose();
//                     let peers = Peers { aquire_rx, release_rx, };
//                     Either::B(result(Ok(Loop::Break(BreakMain::WaitRelease { peers, core, state_no_left, }))))
//                 },
//                 AquireProcess::RequireRestart { core, init_state, aquire_req_pending, } => {
//                     let (aquire_rx, (release_rx, ())) = blender.decompose();
//                     let peers = Peers { aquire_rx, release_rx, };
//                     Either::B(result(Ok(Loop::Break(BreakMain::RequireRestart { peers, core, init_state, aquire_req_pending, }))))
//                 },
//             }
//         })
// }

// enum BreakWait<FNA, FNRM, FNRW, FNCM, FNCW, N, S, R, P> {
//     Shutdown,
//     RequireRestart {
//         release_rx: ReleasePeer<R>,
//         core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//         init_state: S,
//     },
//     ProceedMain {
//         release_rx: ReleasePeer<R>,
//         core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//         state_avail: P,
//     },
// }

// struct WaitState<FNA, FNRM, FNRW, FNCM, FNCW, N, R, Q> {
//     release_rx: ReleasePeer<R>,
//     core: Core<FNA, FNRM, FNRW, FNCM, FNCW, N>,
//     state_no_left: Q,
// }

// fn wait_loop<FNA, FA, FNRM, FRM, FNRW, FRW, FNCM, FCM, FNCW, FCW, N, S, R, P, Q>(
//     wait_state: WaitState<FNA, FNRM, FNRW, FNCM, FNCW, N, R, Q>,
// )
//     -> impl Future<Item = Loop<BreakWait<FNA, FNRM, FNRW, FNCM, FNCW, N, S, R, P>, WaitState<FNA, FNRM, FNRW, FNCM, FNCW, N, R, Q>>, Error = ()>
// where FNA: FnMut(P) -> FA,
//       FA: IntoFuture<Item = (R, Resource<P, Q>), Error = ErrorSeverity<S, ()>>,
//       FNRM: FnMut(P, Option<R>) -> FRM,
//       FRM: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
//       FNRW: FnMut(Q, Option<R>) -> FRW,
//       FRW: IntoFuture<Item = Resource<P, Q>, Error = ErrorSeverity<S, ()>>,
//       FNCM: FnMut(P) -> FCM,
//       FCM: IntoFuture<Item = S, Error = ()>,
//       FNCW: FnMut(Q) -> FCW,
//       FCW: IntoFuture<Item = S, Error = ()>,
//       N: AsRef<str>,
// {
//     let WaitState {
//         core: Core { params, mut vtable, aquires_count, generation, },
//         release_rx,
//         state_no_left,
//     } = wait_state;

//     release_rx
//         .into_future()
//         .then(move |await_result| {
//             match await_result {
//                 Ok((Some(ReleaseReq { generation: release_gen, status, }), release_rx)) => {
//                     if release_gen == generation {
//                         let maybe_resource = match status {
//                             ResourceStatus::Reimburse(resource) => {
//                                 debug!("wait_loop: release request (resource reimbursed)");
//                                 Some(Some(resource))
//                             },
//                             ResourceStatus::ResourceLost => {
//                                 debug!("wait_loop: release request (resource lost)");
//                                 Some(None)
//                             },
//                             ResourceStatus::ResourceFault => {
//                                 debug!("wait_loop: release request (resource fault)");
//                                 None
//                             },
//                         };
//                         let future = if let Some(released_resource) = maybe_resource {
//                             // resource is actually released
//                             let future = (vtable.release_wait_fn)(state_no_left, released_resource)
//                                 .into_future()
//                                 .then(move |release_result| {
//                                     let mut core = Core {
//                                         aquires_count: if aquires_count > 0 { aquires_count - 1 } else { 0 },
//                                         params, vtable, generation,
//                                     };
//                                     match release_result {
//                                         Ok(Resource::Available(state_avail)) => {
//                                             debug!("{} got more resources, proceeding to main loop", core.params.name.as_ref());
//                                             Either::A(result(Ok(Loop::Break(BreakWait::ProceedMain { release_rx, core, state_avail, }))))
//                                         },
//                                         Ok(Resource::OutOfStock(state_no_left)) => {
//                                             debug!("{} got no more resources, aquires left: {}", core.params.name.as_ref(), core.aquires_count);
//                                             if core.aquires_count > 0 {
//                                                 Either::A(result(Ok(Loop::Continue(WaitState { release_rx, core, state_no_left, }))))
//                                             } else {
//                                                 info!("{} runs out of resources, performing restart", core.params.name.as_ref());
//                                                 let future = (core.vtable.close_wait_fn)(state_no_left)
//                                                     .into_future()
//                                                     .map(move |init_state| {
//                                                         Loop::Break(BreakWait::RequireRestart { release_rx, core, init_state, })
//                                                     });
//                                                 Either::B(future)
//                                             }
//                                         },
//                                         Err(ErrorSeverity::Recoverable { state: init_state, }) =>
//                                             Either::A(result(Ok(Loop::Break(BreakWait::RequireRestart { release_rx, core, init_state, })))),
//                                         Err(ErrorSeverity::Fatal(())) => {
//                                             error!("{} crashed with fatal error, terminating", core.params.name.as_ref());
//                                             Either::A(result(Err(())))
//                                         },
//                                     }
//                                 });
//                             Either::A(future)
//                         } else {
//                             // something wrong with resource, schedule restart
//                             warn!("resource fault report: performing restart");
//                             let future = (vtable.close_wait_fn)(state_no_left)
//                                 .into_future()
//                                 .map(move |init_state| {
//                                     Loop::Break(BreakWait::RequireRestart {
//                                         core: Core {
//                                             aquires_count: if aquires_count > 0 { aquires_count - 1 } else { 0 },
//                                             params, vtable, generation,
//                                         },
//                                         init_state,
//                                         release_rx,
//                                     })
//                                 });
//                             Either::B(future)
//                         };
//                         Either::A(future)
//                     } else {
//                         debug!("wait_loop: skipping obsolete release request");
//                         let core = Core { params, vtable, aquires_count, generation, };
//                         Either::B(result(Ok(Loop::Continue(WaitState { release_rx, core, state_no_left, }))))
//                     }
//                 },
//                 Ok((None, _release_rx)) => {
//                     debug!("wait_loop: release channel depleted");
//                     Either::B(result(Ok(Loop::Break(BreakWait::Shutdown))))
//                 },
//                 Err(((), _release_rx)) => {
//                     debug!("wait_loop: release channel outer endpoint dropped");
//                     Either::B(result(Ok(Loop::Break(BreakWait::Shutdown))))
//                 },
//             }
//         })
// }

// fn proceed_with_restart<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, S, R>(
//     restart_state: RestartState<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, S, R>,
// )
//     -> impl Future<Item = Loop<(), RestartState<FNI, FNA, FNRM, FNRW, FNCM, FNCW, N, S, R>>, Error = ()>
// where N: AsRef<str>,
// {
//     match restart_state.core.params.restart_strategy {
//         RestartStrategy::RestartImmediately => {
//             info!("restarting {} immediately", restart_state.core.params.name.as_ref());
//             Either::B(result(Ok(Loop::Continue(restart_state))))
//         },
//         RestartStrategy::Delay { restart_after, } => {
//             info!("restarting {} in {:?}", restart_state.core.params.name.as_ref(), restart_after);
//             let future = Delay::new(Instant::now() + restart_after)
//                 .then(|_delay_result| Ok(Loop::Continue(restart_state)));
//             Either::A(future)
//         },
//     }
// }

// #[derive(Clone, PartialEq, Debug)]
// pub enum UsingError<E> {
//     ResourceTaskGone,
//     Fatal(E),
// }

// pub enum UsingResource<R> {
//     Lost,
//     Reimburse(R),
// }

// impl<R> LodeResource<R> {
//     pub fn steal_resource(self) -> impl Future<Item = (R, LodeResource<R>), Error = ()> {
//         let LodeResource { aquire_tx, release_tx, } = self;
//         let (resource_tx, resource_rx) = oneshot::channel();
//         aquire_tx
//             .send(AquireReq { reply_tx: resource_tx, })
//             .map_err(|_send_error| {
//                 warn!("resource task is gone while aquiring resource");
//             })
//             .and_then(move |aquire_tx| {
//                 resource_rx
//                     .map_err(|oneshot::Canceled| {
//                         warn!("resouce task is gone while receiving aquired resource");
//                     })
//                     .and_then(move |ResourceGen { resource, generation, }| {
//                         release_tx
//                             .send(ReleaseReq { generation, status: ResourceStatus::ResourceLost, })
//                             .map_err(|_send_error| {
//                                 warn!("resource task is gone while releasing resource");
//                             })
//                             .map(move |release_tx| {
//                                 (resource, LodeResource { aquire_tx, release_tx, })
//                             })
//                     })
//             })
//     }

//     pub fn using_resource_loop<F, T, E, S, FI>(
//         self,
//         state: S,
//         using_fn: F,
//     )
//         -> impl Future<Item = (T, LodeResource<R>), Error = UsingError<E>>
//     where F: FnMut(R, S) -> FI,
//           FI: IntoFuture<Item = (UsingResource<R>, Loop<T, S>), Error = ErrorSeverity<S, E>>
//     {
//         loop_fn(
//             (self, using_fn, state),
//             move |(LodeResource { aquire_tx, release_tx, }, mut using_fn, state)| {
//                 debug!("using_resource_loop: aquiring resource");
//                 let (resource_tx, resource_rx) = oneshot::channel();
//                 aquire_tx
//                     .send(AquireReq { reply_tx: resource_tx, })
//                     .map_err(|_send_error| {
//                         warn!("resource task is gone while aquiring resource");
//                         UsingError::ResourceTaskGone
//                     })
//                     .and_then(move |aquire_tx| {
//                         resource_rx
//                             .map_err(|oneshot::Canceled| {
//                                 warn!("resouce task is gone while receiving aquired resource");
//                                 UsingError::ResourceTaskGone
//                             })
//                             .and_then(move |ResourceGen { resource, generation, }| {
//                                 debug!("using_resource_loop: resource aquired, passing control to user proc");
//                                 using_fn(resource, state)
//                                     .into_future()
//                                     .then(move |using_result| {
//                                         match using_result {
//                                             Ok((maybe_resource, loop_action)) => {
//                                                 debug!(
//                                                     "using_resource_loop: resource: {}, loop action: {}, releasing resource",
//                                                     match maybe_resource {
//                                                         UsingResource::Lost =>
//                                                             "lost",
//                                                         UsingResource::Reimburse(..) =>
//                                                             "reimbursed",
//                                                     },
//                                                     match loop_action {
//                                                         Loop::Break(..) =>
//                                                             "break",
//                                                         Loop::Continue(..) =>
//                                                             "continue",
//                                                     },
//                                                 );
//                                                 release_tx
//                                                     .unbounded_send(ReleaseReq {
//                                                         generation,
//                                                         status: match maybe_resource {
//                                                             UsingResource::Lost =>
//                                                                 ResourceStatus::ResourceLost,
//                                                             UsingResource::Reimburse(resource) =>
//                                                                 ResourceStatus::Reimburse(resource),
//                                                         },
//                                                     })
//                                                     .map_err(|_send_error| {
//                                                         warn!("resource task is gone while releasing resource");
//                                                         UsingError::ResourceTaskGone
//                                                     })
//                                                     .map(move |()| {
//                                                         let lode = LodeResource { aquire_tx, release_tx, };
//                                                         match loop_action {
//                                                             Loop::Break(item) =>
//                                                                 Loop::Break((item, lode)),
//                                                             Loop::Continue(state) =>
//                                                                 Loop::Continue((lode, using_fn, state)),
//                                                         }
//                                                     })
//                                             },
//                                             Err(error) => {
//                                                 debug!("using_resource_loop: an error occurred, releasing resource");
//                                                 release_tx
//                                                     .unbounded_send(ReleaseReq { generation, status: ResourceStatus::ResourceFault, })
//                                                     .map_err(|_send_error| {
//                                                         warn!("resource task is gone while releasing resource");
//                                                         UsingError::ResourceTaskGone
//                                                     })
//                                                     .and_then(move |()| {
//                                                         match error {
//                                                             ErrorSeverity::Recoverable { state, } =>
//                                                                 Ok(Loop::Continue((
//                                                                     LodeResource { aquire_tx, release_tx, },
//                                                                     using_fn,
//                                                                     state,
//                                                                 ))),
//                                                             ErrorSeverity::Fatal(fatal_error) =>
//                                                                 Err(UsingError::Fatal(fatal_error)),
//                                                         }
//                                                     })
//                                             }
//                                         }
//                                     })
//                             })
//                     })
//             })
//     }

//     pub fn using_resource_once<F, T, E, S, FI>(
//         self,
//         state: S,
//         mut using_fn: F,
//     )
//         -> impl Future<Item = (T, LodeResource<R>), Error = UsingError<E>>
//     where F: FnMut(R, S) -> FI,
//           FI: IntoFuture<Item = (UsingResource<R>, T), Error = ErrorSeverity<S, E>>
//     {
//         self.using_resource_loop(
//             state,
//             move |resource, state| {
//                 using_fn(resource, state)
//                     .into_future()
//                     .map(|(using_resource, value)| (using_resource, Loop::Break(value)))
//             },
//         )
//     }
// }
