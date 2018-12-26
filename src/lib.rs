pub mod lode;

pub enum ErrorSeverity<S> {
    Fatal,
    Recoverable { state: S, },
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
