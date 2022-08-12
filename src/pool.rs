use std::{
    sync::Arc,
    fmt::Debug,
};

use futures::{
    select,
    Future,
    stream::{
        self,
        StreamExt,
    },
    sink::SinkExt,
    channel::{
        mpsc,
        oneshot,
    },
};

use log::{
    info,
    error,
};

use super::{
    supervisor,
    restart,
    Params,
    NoProcError,
    ErrorSeverity,
};

/////////////////////////////////////////////////////////////////////////////////////

pub struct PoolGenServer<MT> {
    tasks_tx: mpsc::Sender<MT>,
    fused_tasks_rx: stream::Fuse<mpsc::Receiver<MT>>,
}

impl<MT> Default for PoolGenServer<MT>{
    fn default() -> Self {
        let (tasks_tx, tasks_rx) = mpsc::channel(0);

        PoolGenServer {
            tasks_tx,
            fused_tasks_rx: tasks_rx.fuse(),
        }
    }
}

impl<MT> PoolGenServer<MT> {
    pub fn new() -> PoolGenServer<MT> {
        Self::default()
    }

    pub fn pid(&self) -> PoolPid<MT> {
        PoolPid {
            tasks_tx: self.tasks_tx.clone(),
        }
    }

    /// Запуск в работу пула
    /// 
    /// Параметры:
    /// - `parent_supervisor` - родительский супервизор
    /// - `master_params` - настройки политики рестарта мастер-футуры
    /// - `master_init_state` - начальные данные для создаваемой мастер-футуры
    /// - `master_bootstrap` - функтор, создающий мастер футуру
    /// - `master_converter` - ?
    /// - `slaves_bootstrap` - функтор, который создает футуру слейва
    /// - `slaves_handler` -  TODO: функтор, обрабатывающий изменение состояние слейва 
    /// - `slaves_iter` - итератор по параметрам запускаемых слейвов
    #[allow(clippy::too_many_arguments)]
    pub async fn run<MN, SN, MB, FMB, EMB, BMS, MS, C, FC, EC, SB, FSB, ESB, BSS, SS, H, FH, EH, ST, I>(
        self,
        parent_supervisor: supervisor::SupervisorPid,
        master_params: Params<MN>,
        master_init_state: BMS,
        master_bootstrap: MB,
        master_converter: C,
        slaves_bootstrap: SB,
        slaves_handler: H,
        slaves_iter: I,
    )
    where MN: AsRef<str> + Send + 'static,
          SN: AsRef<str> + Send + 'static,
          MB: Fn(BMS) -> FMB + Send + Sync + 'static,
          FMB: Future<Output = Result<MS, ErrorSeverity<BMS, EMB>>> + Send,
          BMS: Send + 'static,
          EMB: Debug + Send,
          C: Fn(MT, MS) -> FC + Send + Sync + 'static,
          FC: Future<Output = Result<(ST, MS), ErrorSeverity<BMS, EC>>> + Send,
          MS: Send,
          EC: Debug + Send,
          SB: Fn(BSS) -> FSB + Send + Sync + 'static,
          BSS: Send + 'static,
          FSB: Future<Output = Result<SS, ErrorSeverity<BSS, ESB>>> + Send,
          ESB: Debug + Send,
          H: Fn(ST, SS) -> FH + Send + Sync + 'static,
          SS: Send,
          FH: Future<Output = Result<SS, ErrorSeverity<BSS, EH>>> + Send,
          EH: Debug + Send,
          MT: Send + 'static,
          ST: Send + 'static,
          I: IntoIterator<Item = (Params<SN>, BSS)>,

    {
        // Получаем дочерний гипервизор
        let supervisor_gen_server =
            parent_supervisor.child_supervisor();
        // Затем его pid для коммуникации
        let mut supervisor = supervisor_gen_server.pid();

        // Создаем канал для коммуникации
        let (slaves_tx, slaves_rx) = mpsc::channel(0);
        // Рессивер для задач
        let fused_tasks_rx = self.fused_tasks_rx;

        // Спавним актор под супервизором
        supervisor.spawn_link_permanent(async move {
            // TODO: ???
            let master_result = run_master(
                master_params,
                master_init_state,
                master_bootstrap,
                fused_tasks_rx,
                slaves_rx.fuse(),
                master_converter,
            ).await;
            match master_result {
                Ok(()) =>
                    (),
                Err(MasterError::Bootstrap(error)) =>
                    error!("state bootstrap error in master: {:?}", error),
                Err(MasterError::Converter(error)) =>
                    error!("tasks converter error in master: {:?}", error),
                Err(MasterError::TasksChannelDropped) =>
                    error!("tasks channel dropped in master"),
                Err(MasterError::TasksChannelDepleted) =>
                    info!("tasks channel depleted in master"),
                Err(MasterError::SlavesChannelDropped) =>
                    error!("slaves channel dropped in master"),
                Err(MasterError::SlavesChannelDepleted) =>
                    info!("slaves channel depleted in master"),
                Err(MasterError::SlaveTaskChannelDropped) =>
                    error!("slave task channel dropped in master"),
                Err(MasterError::CrashForced) =>
                    info!("crash forced after error in master"),
            }
        });

        // Оборачиваем в Arc переданные параметры для удобной возможности шарить между слейвами
        let shared_bootstrap = Arc::new(slaves_bootstrap);
        let shared_handler = Arc::new(slaves_handler);

        // Итератор по слейвам
        for (params, init_state) in slaves_iter {
            // Клонируем "параметры" для создания слейвов
            // Клонируем функтор для создания нашего актора-слейва с параметрами
            let bootstrap = shared_bootstrap.clone();
            // TODO: Клонируем обработчик результатов работы слейва
            let handler = shared_handler.clone();
            // Отправитель статусов работы слейвов
            let slaves_tx = slaves_tx.clone();
            // Запуск слейва под гипервизором
            supervisor.spawn_link_permanent(async move {
                // Стартуем слейва
                let slave_result = run_slave(
                    params,
                    init_state,
                    move |state| bootstrap(state),
                    slaves_tx,
                    move |task, state| handler(task, state),
                ).await;

                match slave_result {
                    Ok(()) =>
                        (),
                    Err(SlaveError::Bootstrap(error)) =>
                        error!("state bootstrap error in slave: {:?}", error),
                    Err(SlaveError::Handler(error)) =>
                        error!("task handler error in slave: {:?}", error),
                    Err(SlaveError::MasterChannelDropped) =>
                        error!("master channel dropped in slave"),
                    Err(SlaveError::TasksChannelDropped) =>
                        error!("tasks channel dropped in slave"),
                    Err(SlaveError::CrashForced) =>
                        info!("crash forced after error in slave"),
                }
            });
        }

        supervisor_gen_server.run().await
    }

}

/////////////////////////////////////////////////////////////////////////////////////

pub struct PoolPid<MT> {
    tasks_tx: mpsc::Sender<MT>,
}

impl<MT> Clone for PoolPid<MT> {
    fn clone(&self) -> Self {
        PoolPid {
            tasks_tx: self.tasks_tx.clone(),
        }
    }
}

impl<MT> PoolPid<MT> {
    pub async fn push_task(&mut self, task: MT) -> Result<(), NoProcError> {
        self.tasks_tx.send(task).await
            .map_err(|_send_error| NoProcError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum MasterError<EB, EC> {
    Bootstrap(EB),
    Converter(EC),
    TasksChannelDropped,
    TasksChannelDepleted,
    SlavesChannelDropped,
    SlavesChannelDepleted,
    SlaveTaskChannelDropped,
    CrashForced,
}

/// Запуск родительского актора
async fn run_master<N, B, FB, EB, BS, C, FC, EC, S, MT, ST>(
    params: Params<N>,
    init_state: BS,
    bootstrap: B,
    fused_tasks_rx: stream::Fuse<mpsc::Receiver<MT>>,
    fused_slaves_rx: stream::Fuse<mpsc::Receiver<oneshot::Sender<ST>>>,
    converter: C,
)
    -> Result<(), MasterError<EB, EC>>
where N: AsRef<str>,
      B: Fn(BS) -> FB,
      FB: Future<Output = Result<S, ErrorSeverity<BS, EB>>>,
      C: Fn(MT, S) -> FC,
      FC: Future<Output = Result<(ST, S), ErrorSeverity<BS, EC>>>,
{
    struct RestartableState<BS, B, MT, ST, C> {
        state: BS,
        bootstrap: B,
        fused_tasks_rx: stream::Fuse<mpsc::Receiver<MT>>,
        fused_slaves_rx: stream::Fuse<mpsc::Receiver<oneshot::Sender<ST>>>,
        converter: C,
    }

    // Создаем перезапускаемую футуру из текущей
    restart::restartable(
        params,
        RestartableState {
            state: init_state,
            bootstrap,
            fused_tasks_rx,
            fused_slaves_rx,
            converter,
        },
        |RestartableState { state: init_state, bootstrap, mut fused_tasks_rx, mut fused_slaves_rx, converter, }| {
            async move {
                // TODO:
                // Создаем параметры для старта из начального состояния
                let bootstrap_result = bootstrap(init_state).await;

                // Состояние после старта
                let mut state = match bootstrap_result {
                    Ok(state) =>
                        state,
                    // Там внутри критичная ошибка
                    Err(ErrorSeverity::Fatal(error)) =>
                        return Err(ErrorSeverity::Fatal(MasterError::Bootstrap(error))),
                    // Восстанавливаемая ошибка
                    Err(ErrorSeverity::Recoverable { state, }) =>
                        return Err(ErrorSeverity::Recoverable {
                            state: RestartableState { state, bootstrap, fused_tasks_rx, fused_slaves_rx, converter, },
                        }),
                };

                let mut pending = Vec::new();
                let mut slaves: Vec<oneshot::Sender<_>> = Vec::new();

                enum Source<A, B> {
                    Task(A),
                    Slave(B),
                }

                loop {
                    // Выбираем какие-то запросы либо из слейвов, либо из канала для текущего актора
                    let req = select! {
                        result = fused_tasks_rx.next() =>
                            Source::Task(result),
                        result = fused_slaves_rx.next() =>
                            Source::Slave(result),
                    };

                    match req {
                        Source::Task(Some(master_task)) =>
                            match converter(master_task, state).await {
                                Ok((task, next_state)) => {
                                    state = next_state;
                                    match slaves.pop() {
                                        Some(slave_tx) => {
                                            slave_tx.send(task)
                                                .map_err(|_send_error| ErrorSeverity::Fatal(MasterError::SlaveTaskChannelDropped))?;
                                        },
                                        None =>
                                            pending.push(task),
                                    }
                                },
                                Err(ErrorSeverity::Fatal(error)) =>
                                    return Err(ErrorSeverity::Fatal(MasterError::Converter(error))),
                                Err(ErrorSeverity::Recoverable { state: next_state, }) =>
                                    return Err(ErrorSeverity::Recoverable {
                                        state: RestartableState { state: next_state, bootstrap, fused_tasks_rx, fused_slaves_rx, converter, },
                                    }),
                            },
                        Source::Task(None) =>
                            return Err(ErrorSeverity::Fatal(MasterError::TasksChannelDepleted)),
                        Source::Slave(Some(slave_online_tx)) => {
                            match pending.pop() {
                                Some(task) => {
                                    slave_online_tx.send(task)
                                        .map_err(|_send_error| ErrorSeverity::Fatal(MasterError::SlaveTaskChannelDropped))?;
                                    },
                                None =>
                                    slaves.push(slave_online_tx),
                            }
                        },
                        Source::Slave(None) =>
                            return Err(ErrorSeverity::Fatal(MasterError::SlavesChannelDepleted)),
                    }
                }
            }
        })
        .await
        .map_err(|restartable_error| match restartable_error {
            restart::RestartableError::Fatal(error) =>
                error,
            restart::RestartableError::RestartCrashForced =>
                MasterError::CrashForced,
        })
}

/////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum SlaveError<EB, EH> {
    Bootstrap(EB),
    Handler(EH),
    MasterChannelDropped,
    TasksChannelDropped,
    CrashForced,
}

/// Запуск в работу дочернего актора
/// - `params` - параметры рестарта актора
/// - `init_state` - начальные параметры для старта актора
/// - `bootstrap` - функтор, который пересоздает футуру, которая и будет нашим актором
/// - `slaves_tx` - TODO: канал для отправки состояния слейвов
/// - `handler` - TODO: обработчик для запуска
async fn run_slave<N, B, EB, FB, BS, S, T, H, EH, FH>(
    params: Params<N>,
    init_state: BS,
    bootstrap: B,
    slaves_tx: mpsc::Sender<oneshot::Sender<T>>,
    handler: H,
)
    -> Result<(), SlaveError<EB, EH>>
where N: AsRef<str>,
      B: Fn(BS) -> FB,
      FB: Future<Output = Result<S, ErrorSeverity<BS, EB>>>,
      H: Fn(T, S) -> FH,
      FH: Future<Output = Result<S, ErrorSeverity<BS, EH>>>,
{
    struct RestartableState<S, B, T, H> {
        state: S,
        bootstrap: B,
        slaves_tx: mpsc::Sender<oneshot::Sender<T>>,
        handler: H,
    }

    // Запускаем рестартуемую футуру
    restart::restartable(
        // Параметры рестарта
        params,
        // Непосредственно параметры для старта актора
        RestartableState { state: init_state, bootstrap, slaves_tx, handler, },
        |RestartableState { state: init_state, bootstrap, mut slaves_tx, handler, }| {
            async move {
                // Создаем футуру на исполнение
                let bootstrap_result = bootstrap(init_state).await;
                
                // Успешно ли смогли создать эту футуру?
                let mut state = match bootstrap_result {
                    Ok(state) =>
                        state,
                    // Если не смогли совсем - кидаем ошибку создания
                    Err(ErrorSeverity::Fatal(error)) =>
                        return Err(ErrorSeverity::Fatal(SlaveError::Bootstrap(error))),
                    // Вернулась восстанавливаемая ошибка
                    Err(ErrorSeverity::Recoverable { state, }) => 
                        // Возвращаем значит снова восстанавливаемую ошибку с тем же самым состоянием
                        return Err(ErrorSeverity::Recoverable {
                            state: RestartableState { state, bootstrap, slaves_tx, handler, },
                        }),
                };

                // Цикл работы со слейвами
                loop {
                    // Одноразовый канал
                    let (task_tx, task_rx) = oneshot::channel();

                    // Отправляем в обратный управления этот одноразовый канал
                    slaves_tx.send(task_tx).await
                        .map_err(|_send_error| ErrorSeverity::Fatal(SlaveError::MasterChannelDropped))?;

                    // Ждем ответа для получения задачи
                    let task = task_rx.await
                        .map_err(|oneshot::Canceled| ErrorSeverity::Fatal(SlaveError::TasksChannelDropped))?;

                    // Вызываем обработчик задачи
                    match handler(task, state).await {
                        Ok(next_state) =>
                            state = next_state,
                        Err(ErrorSeverity::Recoverable { state: next_state, }) =>
                            return Err(ErrorSeverity::Recoverable {
                                state: RestartableState {
                                    state: next_state, bootstrap, slaves_tx, handler,
                                },
                            }),
                        Err(ErrorSeverity::Fatal(error)) =>
                            return Err(ErrorSeverity::Fatal(SlaveError::Handler(error))),
                    }
                }
            }
        })
        .await
        .map_err(|restartable_error| match restartable_error {
            restart::RestartableError::Fatal(error) =>
                error,
            restart::RestartableError::RestartCrashForced =>
                SlaveError::CrashForced,
        })
}
