use futures::{
    Future,
    SinkExt,
    StreamExt,
    FutureExt,
    channel::{
        mpsc,
        oneshot,
    },
    select,
    pin_mut,
};

use log::{
    warn,
    debug,
    error,
};

use super::Terminate;

/////////////////////////////////////////////////////////////////////////////////////

/// Супервизор актора
pub struct SupervisorGenServer {
    sup_tx: mpsc::Sender<Command>,
    sup_rx: mpsc::Receiver<Command>,
}

impl Default for SupervisorGenServer {
    fn default() -> Self {
        let (sup_tx, sup_rx) = mpsc::channel(0);
        SupervisorGenServer { sup_tx, sup_rx, }
    }
}

impl SupervisorGenServer {
    /// Создаем новый экземпляр
    pub fn new() -> SupervisorGenServer {
        Self::default()
    }

    /// Получаем PID для управления
    pub fn pid(&self) -> SupervisorPid {
        SupervisorPid {
            sup_tx: self.sup_tx.clone(),
        }
    }

    /// Запускаем цикл работы и ждем его завершения
    pub async fn run(self) {
        supervisor_loop(self.sup_rx).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////

/// Канал для контроля супервизора
#[derive(Clone)]
pub struct SupervisorPid {
    sup_tx: mpsc::Sender<Command>,
}

impl SupervisorPid {
    /// Закидываем новую футуру, которая окончательно завершает работу гипервизора после завершения.
    pub fn spawn_link_permanent<F>(&mut self, future: F) where F: Future<Output = ()> + Send + 'static {
        self.spawn_link(future, |pid| Command::PermanentProcessExited { pid, })
    }

    /// Закидываем новую футуру, которая будет рестартоваться при выходе
    pub fn spawn_link_temporary<F>(&mut self, future: F) where F: Future<Output = ()> + Send + 'static {
        self.spawn_link(future, |pid| Command::TemporaryProcessExited { pid, })
    }

    /// Закидываем новую футуру, которая окончательно завершает работу гипервизора после завершения.
    /// Запуск будет происходить на определенном обработчике tokio
    pub fn spawn_link_permanent_in<F>(&mut self, handle: &tokio::runtime::Handle, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_link_in(handle, future, |pid| Command::PermanentProcessExited {
            pid,
        })
    }

    /// Закидываем новую футуру, которая будет рестартоваться при выходе.
    /// Запуск будет происходить на определенном обработчике tokio
    pub fn spawn_link_temporary_in<F>(&mut self, handle: &tokio::runtime::Handle, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_link_in(handle, future, |pid| Command::TemporaryProcessExited { pid })
    }

    /// Создаем новый супервизор
    pub fn child_supervisor(&self) -> SupervisorGenServer {
        SupervisorGenServer::new()
    }

    /// Запуск актора-чилда в отдельной корутине
    fn spawn_link<F, E>(
        &mut self,
        future: F,
        report_exit: E,
    )
    where F: Future<Output = ()> + Send + 'static,
          E: FnOnce(ProcessId) -> Command + Send + 'static,
    {
        // Создаем клон канала для контроля
        let child_sup_tx = self.sup_tx.clone();
        // Запускаем нашу футуру в отдельной корутине
        tokio::spawn(async {
            // Запускаем наш актор в работу
            let _ = run_child(child_sup_tx, future, report_exit).await;
        });
    }

    /// Запуск актора-чилда на определенном tokio-хендлере
    fn spawn_link_in<F, E>(&mut self, handle: &tokio::runtime::Handle, future: F, report_exit: E)
    where
        F: Future<Output = ()> + Send + 'static,
        E: FnOnce(ProcessId) -> Command + Send + 'static,
    {
        let child_sup_tx = self.sup_tx.clone();
        handle.spawn(async {
            let _ = run_child(child_sup_tx, future, report_exit).await;
        });
    }
}

/// Запуск футуры-чилда с возможностью оповещения о завершении
async fn run_child<F, E>(
    mut child_sup_tx: mpsc::Sender<Command>,
    future: F,
    report_exit: E,
)
    -> Result<(), ()>
where F: Future<Output = ()> + Send + 'static,
      E: FnOnce(ProcessId) -> Command + Send + 'static,
{   
    // Создаем канал на одноразовое сообщение активации
    let (init_tx, init_rx) = oneshot::channel();
    // Создаем канал оповещения о завершении работы
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Отправляем во внешний канал сообщение, что наш процесс запустился, передавая туда
    // те самые передатчики каналов для активации и для завершения
    child_sup_tx.send(Command::ProcessSpawned { init_tx, shutdown_tx, }).await
        .map_err(|_send_error| warn!("supervisor is gone before child is actually spawned"))?;

    // Получаем из канала назначенный pid для нашего актора текущего
    let pid = init_rx.await
        .map_err(|_send_error| warn!("supervisor is gone before child pid received"))?;

    // Для футуры-актора делаем предохранитель от повторных вызовов, на повторный вызов после Ready будет прилетать Pending
    let fused_future = future.fuse();
    // Аналогично делаем дляя канала завершения работы
    let mut fused_shutdown_rx = shutdown_rx.fuse();

    // Пинируем футуру на стеке, unsafe там внутри
    // Футура действительно никуда не перемещается из текущего контста, так что можно
    pin_mut!(fused_future);

    // Выбираем футуру, которую как раз можно использовать
    select! {
        // Наш актор с предохранителем
        () = fused_future =>
            debug!("a process was terminated"),
        // Получили сообщение о завершении, можно прекратить работу с футурой выше
        result = fused_shutdown_rx =>
            match result {
                Ok(Terminate(())) =>
                    debug!("a process was terminated (shutted down)"),
                Err(oneshot::Canceled) =>
                    debug!("a process was terminated (shutdown channel supervisor endpoint dropped)"),
            },
    };

    // В канал управления данным актором отправляем сообщение о завершении актора
    child_sup_tx.send(report_exit(pid)).await.ok();

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, Debug)]
struct ProcessId {
    pid: usize,
}

/// Различные коменды для гипервизора
enum Command {
    /// Актор успешно был запущен
    ProcessSpawned { init_tx: oneshot::Sender<ProcessId>, shutdown_tx: oneshot::Sender<Terminate<()>>, },
    /// Актор завершен и не требует перезапуска
    PermanentProcessExited { pid: ProcessId, },
    /// Актор завершен и требует перезапуска
    TemporaryProcessExited { pid: ProcessId, },
}

/// Главный цикл гипервизора. В виде параметров получает команды из канала
async fn supervisor_loop(mut notify_rx: mpsc::Receiver<Command>) {
    debug!("supervisor loop started");

    // Массив, который содержит каналы для завершения работающих акторов
    let mut children_txs: Vec<Option<oneshot::Sender<Terminate<()>>>> = Vec::new();
    // TODO: Свободные акторы ???
    let mut free_cells = Vec::new();

    // Ждем новых команд из актора
    while let Some(command) = notify_rx.next().await {
        match command {
            // Дочерний актор был запущен
            Command::ProcessSpawned { init_tx, shutdown_tx, } => {
                // Извлекаем свободные ячейки если есть
                let index = match free_cells.pop() {
                    // Если нету, тогда создаем новую ячейку в список активных
                    None => {
                        let index = children_txs.len();
                        children_txs.push(Some(shutdown_tx));
                        index
                    },
                    // Если нашли свободный слот
                    Some(index) => {
                        // TODO: Может быть как-то можно обработать ошибку вместо паники?

                        // Тогда берем и заменяем слот по свободному индексу на канал завершения
                        let prev_value = std::mem::replace(
                            children_txs.get_mut(index).expect("invalid childrens index"),
                            Some(shutdown_tx),
                        );
                        // Проверка, что раньше там был None
                        assert!(prev_value.is_none());
                        index
                    },
                };

                // В качестве PID будем использовать просто индекс канала в массиве
                let pid = ProcessId { pid: index, };
                // В ответный канал отправляем полученный индекс в виде PID
                if let Err(_send_error) = init_tx.send(pid) {
                    warn!("supervised task has gone before PID is sent");
                }
                debug!("a supervised process {:?} was spawned", pid);
            },
            // Дочерний актор завершил свою работу
            Command::PermanentProcessExited { pid, } => {
                debug!("a permanent supervised process {:?} was terminated", pid);

                // Обрабатываем завершение
                process_finish(pid, &mut children_txs, &mut free_cells);

                // Завершаем в целом гипервизор даже
                break;
            },
            // Прилетело событие о завершении дочернего актора
            Command::TemporaryProcessExited { pid, } => {
                debug!("a temporary supervised process {:?} was terminated", pid);

                // Обрабатываем завершение
                process_finish(pid, &mut children_txs, &mut free_cells);
            },
        }
    }

    debug!("supervisor shutdown: terminating {} linked processes", children_txs.len());

    for (index, maybe_shutdown_tx) in children_txs.into_iter().enumerate() {
        if let Some(shutdown_tx) = maybe_shutdown_tx {
            let pid = ProcessId { pid: index, };
            if let Err(..) = shutdown_tx.send(Terminate(())) {
                debug!("supervised process {:?} is gone while performing shutdown", pid);
            }
        }
    }
}

// Обрабатываем завершение работы дочернего актора
fn process_finish(
    pid: ProcessId, 
    children_txs: &mut [Option<oneshot::Sender<Terminate<()>>>], 
    free_cells: &mut Vec<usize>
) {
    
    // Здесь не паникуем, а пишем ошибки, так как работа происходит с внешними сервисами.
    
    let index = pid.pid;

    // Ищем по нужному индексу
    if let Some(child) = children_txs.get_mut(index){
        // Извлекаем элемент и делаем его None
        if child.take().is_some() {
            // При этом сохраняя в списке свободных слотов
            free_cells.push(index);
        }else{
            error!("invalid pid {} received, child cannot be None", index);
        }
    }else{
        error!("invalid pid {} received, index out of bound", index);
    }
}
