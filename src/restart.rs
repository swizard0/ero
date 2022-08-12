use futures::Future;

use tokio::time::sleep;

use log::{
    debug,
    info,
};

use super::{
    Params,
    ErrorSeverity,
    RestartStrategy,
};

/// Ошибка при рестарте
#[derive(Clone, PartialEq, Debug)]
pub enum RestartableError<E> {
    /// Критичная ошибка
    Fatal(E),
    // Актор вернул ошибку, которую можно восстановить, но настроено все так, что надо рестартовать
    RestartCrashForced,
}

/// Оборачиваем футуру в рестартуемую
/// 
/// - `params` - параметры, которые настраивают логику рестарта
/// - `init_state` - параметры для самого первого старта футуры
/// - `restartable_fn` - функтор, который генерируем на каждый рестарт 
/// новую футуру и возвращает состояние при ошибке
pub async fn restartable<N, S, R, F, V, E>(
    params: Params<N>,
    mut init_state: S,
    mut restartable_fn: R,
)
    -> Result<V, RestartableError<E>>
where R: FnMut(S) -> F,
      F: Future<Output = Result<V, ErrorSeverity<S, E>>>,
      N: AsRef<str>,
{
    info!("{}: restartable loop started", params.name.as_ref());

    loop {
        // Создаем футуру с переданным состоянием и сразу же начинаем ее исполнять
        match restartable_fn(init_state).await {
            // Футура вернула Ok - значит можно
            Ok(value) =>
                return Ok(value),
            // Если вернулась ошибка, которую можно восстановить
            Err(ErrorSeverity::Recoverable { state, }) =>
                // Какая у нас политика рестарта?
                match params.restart_strategy {
                    // Сразу же падаем даже если ошибка восстанавливаемая 
                    // Тогда сразу же кидаем ошибку на уровень выше и не пробуем стартануть снова.
                    RestartStrategy::InstantCrash => {
                        info!("recoverable error in {} but current strategy is to crash", params.name.as_ref());
                        return Err(RestartableError::RestartCrashForced);
                    },
                    // Рестартовать надо сразу же - тогда так и делаем
                    RestartStrategy::RestartImmediately => {
                        info!("recoverable error in {}, restarting immediately", params.name.as_ref());
                        init_state = state;
                    },
                    // Рестартуем с определенной задержкой
                    RestartStrategy::Delay { restart_after, } => {
                        info!("recoverable error in {}, restarting in {:?}", params.name.as_ref(), restart_after);
                        sleep(restart_after).await;
                        init_state = state;
                    },
                },
            // Критичная ошибка произошла - кидаем ошибку выше
            Err(ErrorSeverity::Fatal(error)) => {
                debug!("fatal error in {}", params.name.as_ref());
                return Err(RestartableError::Fatal(error));
            },
        }
    }
}
