use std::time::Duration;

pub use futures::future::Loop;

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
