#![type_length_limit="8388608"]

use std::time::Duration;

pub use futures::future::Loop;

pub mod restart;
pub mod blend;
pub mod lode;
pub mod net;

pub enum ErrorSeverity<S, A> {
    Fatal(A),
    Recoverable { state: S, },
}

pub enum RestartStrategy {
    RestartImmediately,
    Delay { restart_after: Duration, },
}

pub struct Params<N> {
    pub name: N,
    pub restart_strategy: RestartStrategy,
}
