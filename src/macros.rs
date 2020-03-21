
#[macro_export]
macro_rules! try_recover {
    ($future:expr, $state:expr, $fmt:expr, $($arg:tt)*) => {
        match $future.await {
            Ok(value) =>
                value,
            Err(error) => {
                error!(concat!("{:?}: ", $fmt), error, $($arg)*);
                return Err(ero::ErrorSeverity::Recoverable { state: $state, });
            },
        }
    };
}
