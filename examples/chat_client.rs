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

#[tokio::main]
async fn main() {
    pretty_env_logger::init_timed();
    let supervisor_gen_server = SupervisorGenServer::new();
    let mut supervisor_pid = supervisor_gen_server.pid();

    info!("creating stdio gen_server");
    let stdio_gen_server = StdioGenServer::new();
    let stdio_pid = stdio_gen_server.pid();

    info!("creating network gen_server");
    let network_gen_server = NetworkGenServer::new();
    let network_pid = network_gen_server.pid();

    supervisor_pid.spawn_link_permanent(stdio_gen_server.run(
        ero::Params {
            name: "chat_client stdio",
            restart_strategy: RestartStrategy::RestartImmediately,
        },
        network_pid,
    ));

    supervisor_pid.spawn_link_permanent(network_gen_server.run(
        ero::Params {
            name: "chat_client network",
            restart_strategy: RestartStrategy::Delay {
                restart_after: Duration::from_secs(8),
            },
        },
        "127.0.0.1:4447",
        stdio_pid,
    ));

    supervisor_gen_server.run().await;
}

#[derive(Debug)]
struct Line(String);

struct StdioGenServer {
    external_tx: mpsc::Sender<Line>,
    external_rx: mpsc::Receiver<Line>,
}

#[derive(Clone)]
struct StdioPid {
    external_tx: mpsc::Sender<Line>,
}

impl StdioGenServer {
    pub fn new() -> StdioGenServer {
        let (external_tx, external_rx) = mpsc::channel(0);
        StdioGenServer {
            external_tx,
            external_rx,
        }
    }

    pub fn pid(&self) -> StdioPid {
        StdioPid {
            external_tx: self.external_tx.clone(),
        }
    }

    pub async fn run<N>(self, params: ero::Params<N>, network_pid: NetworkPid) where N: AsRef<str> {
        let fused_external_rx = self.external_rx.fuse();

        let task = restart::restartable(
            params,
            (fused_external_rx, network_pid),
            |(mut fused_external_rx, mut network_pid)| async move {
                let stdin = io::stdin();
                let mut stdout = io::stdout();
                let mut stdin = io::BufReader::new(stdin)
                    .lines();

                loop {
                    enum Req {
                        Stdin(Result<Option<String>, IoError>),
                        Input(Option<Line>),
                    }

                    let req = select! {
                        result = stdin.next_line().fuse() =>
                            Req::Stdin(result),
                        result = fused_external_rx.next() =>
                            Req::Input(result),
                    };

                    match req {
                        Req::Stdin(Ok(Some(line))) => {
                            debug!("STDIN: here comes a line: {:?}", line);
                            if let Err(NoProcError) = network_pid.send_line(Line(line)).await {
                                return Ok(());
                            }
                        },
                        Req::Stdin(Err(error)) => {
                            error!("stdin read error: {:?}, terminating", error);
                            return Err(ErrorSeverity::Fatal(()));
                        },
                        Req::Stdin(Ok(None)) => {
                            info!("stdin depleted, terminating");
                            return Ok(());
                        },
                        Req::Input(Some(Line(mut line))) => {
                            line.push('\n');
                            if let Err(error) = stdout.write_all(line.as_bytes()).await {
                                error!("stdout write error: {:?}, restarting", error);
                                return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, network_pid), });
                            }
                            if let Err(error) = stdout.flush().await {
                                error!("stdout flush error: {:?}, restarting", error);
                                return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, network_pid), });
                            }
                        },
                        Req::Input(None) => {
                            info!("stdio gen_server pid shutted down, terminating");
                            return Ok(());
                        },
                    }
                }
            }
        );
        match task.await {
            Ok(()) =>
                info!("stdio gen_server terminated normally"),
            Err(restart::RestartableError::Fatal(())) =>
                info!("stdio gen_server terminated with fatal error"),
            Err(restart::RestartableError::RestartCrashForced) =>
                unreachable!(),
        }
    }
}

impl StdioPid {
    pub async fn display_line(&mut self, line: Line) -> Result<(), NoProcError> {
        self.external_tx.send(line).await
            .map_err(|_send_error| {
                warn!("stdio gen_server has gone while sending line");
                NoProcError
            })
    }
}

struct NetworkGenServer {
    external_tx: mpsc::Sender<Line>,
    external_rx: mpsc::Receiver<Line>,
}

#[derive(Clone)]
struct NetworkPid {
    external_tx: mpsc::Sender<Line>,
}

impl NetworkGenServer {
    pub fn new() -> NetworkGenServer {
        let (external_tx, external_rx) = mpsc::channel(0);
        NetworkGenServer {
            external_tx,
            external_rx,
        }
    }

    pub fn pid(&self) -> NetworkPid {
        NetworkPid {
            external_tx: self.external_tx.clone(),
        }
    }

    pub async fn run<N, A>(
        self,
        params: ero::Params<N>,
        connect_addr: A,
        stdio_pid: StdioPid,
    )
    where N: AsRef<str>,
          A: ToSocketAddrs + std::fmt::Debug,
    {
        let fused_external_rx = self.external_rx.fuse();
        let task = restart::restartable(
            params,
            (fused_external_rx, connect_addr, stdio_pid),
            |(mut fused_external_rx, connect_addr, mut stdio_pid)| async move {
                info!("connecting to {:?}", connect_addr);
                let mut stream = match net::TcpStream::connect(&connect_addr).await {
                    Ok(stream) =>
                        stream,
                    Err(error) => {
                        error!("connection to {:?} failed: {:?}, restarting", connect_addr, error);
                        return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                    },
                };
                let (tcp_read, mut tcp_write) = stream.split();
                let mut tcp_read = io::BufReader::new(tcp_read)
                    .lines();

                info!("connected to {:?}!", connect_addr);
                loop {
                    enum Req {
                        TcpRead(Result<Option<String>, IoError>),
                        Input(Option<Line>),
                    }

                    let req = select! {
                        result = tcp_read.next_line().fuse() =>
                            Req::TcpRead(result),
                        result = fused_external_rx.next() =>
                            Req::Input(result),
                    };

                    match req {
                        Req::TcpRead(Ok(Some(line))) => {
                            debug!("TCP: here comes a line: {:?}", line);
                            if let Err(NoProcError) = stdio_pid.display_line(Line(line)).await {
                                return Ok(());
                            }
                        },
                        Req::TcpRead(Err(error)) => {
                            error!("tcp read failed: {:?}, restarting", error);
                            return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                        },
                        Req::TcpRead(Ok(None)) => {
                            info!("tcp socket closed, restarting");
                            return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                        },
                        Req::Input(Some(Line(mut line))) => {
                            line.push('\n');
                            if let Err(error) = tcp_write.write_all(line.as_bytes()).await {
                                error!("tcp write failed: {:?}, restarting", error);
                                return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                            }
                            if let Err(error) = tcp_write.flush().await {
                                error!("tcp flush failed: {:?}, restarting", error);
                                return Err(ErrorSeverity::Recoverable { state: (fused_external_rx, connect_addr, stdio_pid), });
                            }
                        },
                        Req::Input(None) => {
                            info!("network gen_server pid shutted down, terminating");
                            return Ok(());
                        },
                    }
                }
            }
        );
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

impl NetworkPid {
    pub async fn send_line(&mut self, line: Line) -> Result<(), NoProcError> {
        self.external_tx.send(line).await
            .map_err(|_send_error| {
                warn!("network gen_server has gone while sending line");
                NoProcError
            })
    }
}
