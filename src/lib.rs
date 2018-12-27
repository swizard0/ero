use std::time::Duration;

pub mod lode;

pub enum ErrorSeverity<S> {
    Fatal,
    Recoverable { state: S, },
}

pub enum RestartStrategy {
    RestartImmediately,
    Delay { restart_after: Duration, },
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
