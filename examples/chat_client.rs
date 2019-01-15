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
    blend::{
        Gone,
        Blender,
        Decompose,
        ErrorEvent,
        DecomposeZip,
        FutureGenerator,
    },
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
            FutureGenerator { future: stdin_resource.steal_resource(), next: lode::LodeResource::steal_resource, },
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
    stdin_gen: FutureGenerator<SF, F>,
)
    -> impl Future<Item = (UsingResource<TS>, Loop<(), FutureGenerator<SF, F>>), Error = ErrorSeverity<FutureGenerator<SF, F>, ()>>
where TS: tokio::io::AsyncRead + tokio::io::AsyncWrite,
      SF: Future<Item = (Option<String>, lode::LodeResource<Option<String>>), Error = ()>,
      F: Fn(lode::LodeResource<Option<String>>) -> SF,
{
    info!("connected successfully");

    let tcp_stream_codec = LinesCodec::new();
    let tcp_stream_lines = tcp_stream_codec.framed(tcp_stream);
    let (tcp_stream_lines_tx, tcp_stream_lines_rx) =
        tcp_stream_lines.split();

    let blender = Blender::new()
        .add(stdin_gen)
        .add(tcp_stream_lines_rx)
        .finish_sources()
        .fold(Either::B, Either::B)
        .fold(Either::A, Either::A)
        .finish();

    future::loop_fn((blender, tcp_stream_lines_tx), |(blender, tcp_tx)| {
        blender
            .then(move |result| {
                match result {
                    Ok((Either::A(line), blender)) => {
                        debug!("STDIN: here comes a line: {:?}", line);
                        let future = tcp_tx.send(line)
                            .then(move |send_result| {
                                match send_result {
                                    Ok(tcp_tx) =>
                                        Ok(Loop::Continue((blender, tcp_tx))),
                                    Err(error) => {
                                        error!("TCP: send: {:?}", error);
                                        let (stdin_gen, (_tcp_rx, ())) = blender.decompose();
                                        Err(ErrorSeverity::Recoverable { state: stdin_gen, })
                                    },
                                }
                            });
                        Either::A(future)
                    },
                    Ok((Either::B(line), blender)) => {
                        debug!("TCP: here comes a line: {:?}", line);
                        println!("{}", line);
                        Either::B(future::result(Ok(Loop::Continue((blender, tcp_tx)))))
                    },
                    Err(Either::A(ErrorEvent::Depleted {
                        decomposed: DecomposeZip { left_dir: (_tcp_rx, ()), myself: (_stdin_resource, _mk_stdin_steal), right_rev: (), },
                    })) => {
                        warn!("STDIN: unexpected stream termination, shutting down");
                        Either::B(future::result(Ok(Loop::Break(Loop::Break(())))))
                    },
                    Err(Either::B(ErrorEvent::Depleted {
                        decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (stdin_gen, ()), },
                    })) => {
                        info!("TCP: broken pipe");
                        Either::B(future::result(Ok(Loop::Break(Loop::Continue(stdin_gen)))))
                    },
                    Err(Either::A(ErrorEvent::Error {
                        error: (),
                        decomposed: DecomposeZip { left_dir: (_tcp_rx, ()), myself: Gone, right_rev: (), },
                    })) => {
                        error!("STDIN task dropped channel endpoint");
                        Either::B(future::result(Ok(Loop::Break(Loop::Break(())))))
                    },
                    Err(Either::B(ErrorEvent::Error {
                        error,
                        decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (stdin_gen, ()), },
                    })) => {
                        error!("TCP: {:?}", error);
                        Either::B(future::result(Err(ErrorSeverity::Recoverable { state: stdin_gen, })))
                    },
                }
            })
    }).map(|action| (UsingResource::Lost, action))
}
