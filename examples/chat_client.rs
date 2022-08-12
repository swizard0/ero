use std::{
    time::Duration,
    io::Error as IoError,
};

use futures::{
    select,
    channel::mpsc,
    FutureExt,
    StreamExt,
    SinkExt,
};

use tokio::{
    io::{
        self,
        AsyncBufReadExt,
        AsyncWriteExt,
    },
    net::{
        self,
        ToSocketAddrs,
    },
};

use log::{
    debug,
    info,
    warn,
    error,
};

use ero::{
    restart,
    NoProcError,
    ErrorSeverity,
    RestartStrategy,
    supervisor::SupervisorGenServer,
};

/////////////////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() {
    // Запускаем систему логирования
    pretty_env_logger::init_timed();

    // Создаем супервизор
    let supervisor_gen_server = SupervisorGenServer::new();
    // Получаем канал для общения с супервизором
    let mut supervisor_pid = supervisor_gen_server.pid();

    // Создаем актор по работе с stdin
    info!("creating stdio gen_server");
    let stdio_gen_server = StdioGenServer::new();
    let stdio_pid = stdio_gen_server.pid();

    // Создаем актор по работе с сетевым сокетом
    info!("creating network gen_server");
    let network_gen_server = NetworkGenServer::new();
    let network_pid = network_gen_server.pid();

    // Стартуем актор работы со stdin под супервизором
    supervisor_pid.spawn_link_permanent(stdio_gen_server.run(
        ero::Params {
            name: "chat_client stdio",
            // Рестартовать будем сразу же
            restart_strategy: RestartStrategy::RestartImmediately,
        },
        network_pid,
    ));

    // Стартуем актор работы с сетевым сокетом под супервизором
    supervisor_pid.spawn_link_permanent(network_gen_server.run(
        ero::Params {
            name: "chat_client network",
            // Рестартовать будем c небольшой задержкой
            restart_strategy: RestartStrategy::Delay {
                restart_after: Duration::from_secs(8),
            },
        },
        "127.0.0.1:4447",
        stdio_pid,
    ));

    // Стартуем супервизор и ждем завершения всех работ
    supervisor_gen_server.run().await;
}

/////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct Line(String);

struct StdioGenServer {
    external_tx: mpsc::Sender<Line>,
    external_rx: mpsc::Receiver<Line>,
}

impl StdioGenServer {
    /// Создаем наш тестовый сервер
    pub fn new() -> StdioGenServer {
        let (external_tx, external_rx) = mpsc::channel(0);
        StdioGenServer {
            external_tx,
            external_rx,
        }
    }

    /// Получаем его канал для общения
    pub fn pid(&self) -> StdioPid {
        StdioPid {
            external_tx: self.external_tx.clone(),
        }
    }

    /// Стартуем сервис, поглощая self
    pub async fn run<N>(self, params: ero::Params<N>, network_pid: NetworkPid) where N: AsRef<str> {
        // Специально оставляем экземпляр external_tx (внешнего канала для передачи сюда сообщений).
        // Иначе, если не был ни разу вызван pid(), то канал получения будет тоже закрыт

        // Делаем предохранитель для множественных вызовов на закрытом канале
        let fused_external_rx = self.external_rx.fuse();

        // Оборачиваем в рестартуемую обертку
        let task = restart::restartable(
            // Параметры рестарта
            params,
            // Начальные значения для футуры рестартуемой
            (fused_external_rx, network_pid),
            // Собственно сама наша футура
            |(mut fused_external_rx, mut network_pid)| async move {
                // Получаем асинхронный stdin + stdout из tokio
                let stdin = io::stdin();
                let mut stdout = io::stdout();

                // Делаем буфферизированную обертку
                let mut stdin = io::BufReader::new(stdin)
                    .lines();

                loop {
                    enum Req {
                        Stdin(Result<Option<String>, IoError>),
                        Input(Option<Line>),
                    }

                    // Читаем либо из внешнего канала данные, либо из stdin строку
                    let req = select! {
                        result = stdin.next_line().fuse() =>
                            Req::Stdin(result),
                        result = fused_external_rx.next() =>
                            Req::Input(result),
                    };

                    // Смотрим что прилетело
                    match req {
                        // Строка в stdin
                        Req::Stdin(Ok(Some(line))) => {
                            debug!("STDIN: here comes a line: {:?}", line);
                            // Отправляем во внешний актор полученную строку
                            if let Err(NoProcError) = network_pid.send_line(Line(line)).await {
                                return Ok(());
                            }
                        },
                        // Ошибка чтения из stdin
                        Req::Stdin(Err(error)) => {
                            error!("stdin read error: {:?}, terminating", error);
                            return Err(ErrorSeverity::Fatal(()));
                        },
                        // Завершилась обработка stdin - выходим из цикла обработки
                        Req::Stdin(Ok(None)) => {
                            info!("stdin completed, terminating");
                            return Ok(());
                        },
                        // Снаружи прилетела строка текста
                        Req::Input(Some(Line(mut line))) => {
                            // Добавляем перенос строки
                            line.push('\n');
                            // Пишем в текущем акторе, что прилетело снаружи
                            if let Err(error) = stdout.write_all(line.as_bytes()).await {
                                error!("stdout write error: {:?}, restarting", error);
                                // При ошибке мы возвращаем снова состояние назад, тогда
                                // при повторном запуске нам прилетит снова объект
                                return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, network_pid), });
                            }
                            // Сбрасываем раз буфер
                            if let Err(error) = stdout.flush().await {
                                error!("stdout flush error: {:?}, restarting", error);
                                // Снова пытаемся восстановиться повторно
                                return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, network_pid), });
                            }
                        },
                        // Снаружи прилетело окончание - завершаем наш актор
                        Req::Input(None) => {
                            info!("stdio gen_server pid shutted down, terminating");
                            return Ok(());
                        },
                    }
                }
            }
        );
        // Запускаем нашу рестартуемую футуру-актор в бой
        match task.await {
            // Футура закончила свою работу нормально
            Ok(()) =>
                info!("stdio gen_server terminated normally"),
            // Какая-то ошибка внутри произошла, которую уже не можем восстановить
            Err(restart::RestartableError::Fatal(())) =>
                info!("stdio gen_server terminated with fatal error"),
            // TODO: ???
            Err(restart::RestartableError::RestartCrashForced) =>
                unreachable!(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////

/// Канал для общения с [StdioGenServer]
#[derive(Clone)]
struct StdioPid {
    external_tx: mpsc::Sender<Line>,
}

impl StdioPid {
    /// Пишем какое-то сообщение в [StdioGenServer]
    /// 
    /// Выполнение не идет дальше до вычитывания так как используется канал размером буффера 0
    pub async fn display_line(&mut self, line: Line) -> Result<(), NoProcError> {
        self.external_tx.send(line).await
            .map_err(|_send_error| {
                warn!("stdio gen_server has gone while sending line");
                NoProcError
            })
    }
}

/////////////////////////////////////////////////////////////////////////////////////


struct NetworkGenServer {
    external_tx: mpsc::Sender<Line>,
    external_rx: mpsc::Receiver<Line>,
}

impl NetworkGenServer {
    /// Создаем билдер для актора
    pub fn new() -> NetworkGenServer {
        let (external_tx, external_rx) = mpsc::channel(0);
        NetworkGenServer {
            external_tx,
            external_rx,
        }
    }

    /// Получаем канал для общения, можно вызывать много раз
    pub fn pid(&self) -> NetworkPid {
        NetworkPid {
            external_tx: self.external_tx.clone(),
        }
    }

    /// Стартуем актор, поглощая экземпляр
    pub async fn run<N, A>(
        self,
        params: ero::Params<N>,
        connect_addr: A,
        stdio_pid: StdioPid,
    )
    where N: AsRef<str>,
          A: ToSocketAddrs + std::fmt::Debug,
    {
        // Создаем предохранитель для канала получения команд снаружи
        let fused_external_rx = self.external_rx.fuse();

        // Стартуем актор
        let task = restart::restartable(
            // Параметры для рестарта
            params,
            // Параметры актора
            (fused_external_rx, connect_addr, stdio_pid),
            // Сам актор, который создается на каждой итерации
            |(mut fused_external_rx, connect_addr, mut stdio_pid)| async move {
                // Подключаемся к адресу
                info!("connecting to {:?}", connect_addr);
                let mut stream = match net::TcpStream::connect(&connect_addr).await {
                    Ok(stream) =>
                        stream,
                    Err(error) => {
                        // При ошибке подключения пытаемся заново повторить данную процедуру возвращаея контекст назад
                        error!("connection to {:?} failed: {:?}, restarting", connect_addr, error);
                        return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                    },
                };

                // Разделяем на читателя и писателя
                let (tcp_read, mut tcp_write) = stream.split();

                // Оборачиваем читателя в буффер + читатель построчно
                let mut tcp_read = io::BufReader::new(tcp_read)
                    .lines();

                info!("connected to {:?}!", connect_addr);
                loop {
                    enum Req {
                        TcpRead(Result<Option<String>, IoError>),
                        Input(Option<Line>),
                    }

                    // Смотрим что прилетело в TCP поток или внешний канал для актора
                    let req = select! {
                        result = tcp_read.next_line().fuse() =>
                            Req::TcpRead(result),
                        result = fused_external_rx.next() =>
                            Req::Input(result),
                    };

                    match req {
                        // Читаем данные, которые прилетели
                        Req::TcpRead(Ok(Some(line))) => {
                            debug!("TCP: here comes a line: {:?}", line);
                            // Отсылаем сообщение в другой актор, если он завершился - выходим тоже
                            if let Err(NoProcError) = stdio_pid.display_line(Line(line)).await {
                                return Ok(());
                            }
                        },
                        // Вылезла ошибка при чтении из TCP сокета
                        Req::TcpRead(Err(error)) => {
                            error!("tcp read failed: {:?}, restarting", error);
                            // Пробуем сделать еще одну попытку
                            return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                        },
                        // Сокет был закрыт, можно рестартануть работу
                        Req::TcpRead(Ok(None)) => {
                            info!("tcp socket closed, restarting");
                            // Пробуем сделать еще одну попытку
                            return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                        },
                        // Снаружи нам прилетели какие-то данные через PID
                        Req::Input(Some(Line(mut line))) => {
                            line.push('\n');
                            // Пишем их в сокет
                            if let Err(error) = tcp_write.write_all(line.as_bytes()).await {
                                error!("tcp write failed: {:?}, restarting", error);
                                return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                            }
                            // Сбрасываем данные сокета
                            if let Err(error) = tcp_write.flush().await {
                                error!("tcp flush failed: {:?}, restarting", error);
                                return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                            }
                        },
                        // Если канал закрыт - выходим
                        Req::Input(None) => {
                            info!("network gen_server pid shutted down, terminating");
                            return Ok(());
                        },
                    }
                }
            }
        );
        // Стартуем наш актор с циклом и ждем результата работы
        match task.await {
            Ok(()) =>
                info!("network gen_server terminated normally"),
            Err(restart::RestartableError::Fatal(())) =>
                info!("network gen_server terminated with fatal error"),
            Err(restart::RestartableError::RestartCrashForced) =>
                unreachable!(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct NetworkPid {
    external_tx: mpsc::Sender<Line>,
}

impl NetworkPid {
    /// Пишем какое-то сообщение в [NetworkGenServer]
    /// 
    /// Выполнение не идет дальше до вычитывания так как используется канал размером буффера 0
    pub async fn send_line(&mut self, line: Line) -> Result<(), NoProcError> {
        self.external_tx.send(line).await
            .map_err(|_send_error| {
                warn!("network gen_server has gone while sending line");
                NoProcError
            })
    }
}
