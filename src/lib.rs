#![type_length_limit="8388608"]

use std::time::Duration;

pub use futures::future::Loop;

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
