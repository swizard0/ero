use std::time::Duration;

pub mod supervisor;
pub mod restart;
pub mod pool;
mod macros;

pub enum ErrorSeverity<S, A> {
    Fatal(A),
    Recoverable { state: S, },
}

pub enum RestartStrategy {
    InstantCrash,
    RestartImmediately,
    Delay { restart_after: Duration, },
}

pub struct Terminate<R>(pub R);

pub struct Params<N> {
    pub name: N,
    pub restart_strategy: RestartStrategy,
}
