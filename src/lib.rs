#![forbid(unsafe_code)]

use std::time::Duration;

mod macros;
pub mod pool;
pub mod restart;
pub mod supervisor;

/// Тип ошибки
pub enum ErrorSeverity<S, A> {
    /// Критическая ошибка, восстанавливать бесполезно
    Fatal(A),
    /// Можем восстановиться, в качестве параметра передаем состояние, которое нужно передать назад очередной попытки
    Recoverable { state: S },
}

/// Стратегия для рестарта
pub enum RestartStrategy {
    /// Сразу же падаем
    InstantCrash,
    /// Рестарт мгновенный
    RestartImmediately,
    /// Рестарт с определенной паузой
    Delay { restart_after: Duration },
}

/// Параметры политики рестарта
pub struct Params<N> {
    pub name: N,
    pub restart_strategy: RestartStrategy,
}

// TODO: ???
pub struct Terminate<R>(pub R);

/// Ошибка, что актор недоступен для работы
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct NoProcError;
