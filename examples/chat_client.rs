#![type_length_limit="8388608"]

use std::{
    time::Duration,
    net::ToSocketAddrs,
};

use futures::{
    Sink,
    Future,
    Stream,
    future::{
        self,
        Either,
    },
};

use tokio_codec::{
    Decoder,
    LinesCodec,
    FramedRead,
};

use log::{
    debug,
    info,
    warn,
    error,
};

use erlust::{
    Loop,
    ErrorSeverity,
    RestartStrategy,
    lode::{
        self,
        UsingError,
        UsingResource,
    },
    net::tcp::tcp_stream,
};

fn main() {
    pretty_env_logger::init_timed();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let executor = runtime.executor();

    info!("creating stdin task");
    let lode::Lode { resource: stdin_resource, shutdown: stdin_shutdown, } = lode::stream::spawn(
        &executor,
        lode::Params {
            name: "chat_client stdin",
            restart_strategy: RestartStrategy::RestartImmediately,
        },
        (),
        |()| Ok((
            FramedRead::new(tokio::io::stdin(), LinesCodec::new())
                .map_err(|error| {
                    error!("something wrong while reading from stdin: {:?}", error);
                }),
            (),
        )),
    );

    info!("creating tcp_stream task");
    let tcp_stream_lode = tcp_stream::spawn(&executor, tcp_stream::Params {
        sock_addr: "127.0.0.1:4447".to_socket_addrs().unwrap().next().unwrap(),
        lode_params: lode::Params {
            name: "chat_client tcp_stream",
            restart_strategy: RestartStrategy::Delay {
                restart_after: Duration::from_secs(8),
            },
        },
    });

    let client_future = tcp_stream_lode
        .resource
        .using_resource_loop(
            (stdin_resource.steal_resource(), lode::LodeResource::steal_resource),
            using_loop,
        )
        .then(|result| {
            stdin_shutdown.shutdown();
            match result {
                Ok(((), _lode)) => {
                    info!("client terminated successfully");
                    Ok(())
                },
                Err(UsingError::ResourceTaskGone) => {
                    error!("connection task suddenly gone");
                    Err(())
                },
                Err(UsingError::Fatal(())) => {
                    error!("client has terminated with fatal error");
                    Err(())
                },
            }
        });
    executor.spawn(client_future);

    let _ = runtime.shutdown_on_idle().wait();
}

fn using_loop<TS, SF, F>(
    tcp_stream: TS,
    (stdin_steal, mk_stdin_steal): (SF, F),
)
    -> impl Future<Item = (UsingResource<TS>, Loop<(), (SF, F)>), Error = ErrorSeverity<(SF, F), ()>>
where TS: tokio::io::AsyncRead + tokio::io::AsyncWrite,
      SF: Future<Item = (Option<String>, lode::LodeResource<Option<String>>), Error = ()>,
      F: Fn(lode::LodeResource<Option<String>>) -> SF,
{
    info!("connected successfully");

    let tcp_stream_codec = LinesCodec::new();
    let tcp_stream_lines = tcp_stream_codec.framed(tcp_stream);
    let (tcp_stream_lines_tx, tcp_stream_lines_rx) =
        tcp_stream_lines.split();

    let future = future::loop_fn(
        (
            tcp_stream_lines_rx.into_future(),
            tcp_stream_lines_tx,
            stdin_steal,
            mk_stdin_steal,
        ),
        |(tcp_rx, tcp_tx, stdin_steal, mk_stdin_steal)| {
            stdin_steal
                .select2(tcp_rx)
                .then(move |result| {
                    match result {
                        Ok(Either::A(((Some(line), stdin_resource), tcp_rx))) => {
                            debug!("STDIN: here comes a line: {:?}", line);
                            let future = tcp_tx.send(line)
                                .then(move |send_result| {
                                    let stdin_steal = mk_stdin_steal(stdin_resource);
                                    match send_result {
                                        Ok(tcp_tx) =>
                                            Ok(Loop::Continue((tcp_rx, tcp_tx, stdin_steal, mk_stdin_steal))),
                                        Err(error) => {
                                            error!("TCP: send: {:?}", error);
                                            Err(ErrorSeverity::Recoverable { state: (stdin_steal, mk_stdin_steal), })
                                        },
                                    }
                                });
                            Either::A(future)
                        },
                        Ok(Either::A(((None, stdin_resource), tcp_rx))) => {
                            warn!("STDIN: unexpected stream termination, proceeding");
                            Either::B(future::result(Ok(Loop::Continue((tcp_rx, tcp_tx, mk_stdin_steal(stdin_resource), mk_stdin_steal)))))
                        },
                        Ok(Either::B(((Some(line), tcp_rx), stdin_steal))) => {
                            debug!("TCP: here comes a line: {:?}", line);
                            println!("{}", line);
                            Either::B(future::result(Ok(Loop::Continue((tcp_rx.into_future(), tcp_tx, stdin_steal, mk_stdin_steal)))))
                        },
                        Ok(Either::B(((None, _tcp_rx), stdin_steal))) => {
                            info!("TCP: broken pipe");
                            Either::B(future::result(Ok(Loop::Break(Loop::Continue((stdin_steal, mk_stdin_steal))))))
                        },
                        Err(Either::A(((), _tcp_rx))) => {
                            error!("STDIN task dropped channel endpoint");
                            Either::B(future::result(Ok(Loop::Break(Loop::Break(())))))
                        },
                        Err(Either::B(((error, _tcp_rx), stdin_steal))) => {
                            error!("TCP: {:?}", error);
                            Either::B(future::result(Err(ErrorSeverity::Recoverable { state: (stdin_steal, mk_stdin_steal), })))
                        },
                    }
                })
        },
    );
    future.map(|action| (UsingResource::Lost, action))
}
