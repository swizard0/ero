use std::net::SocketAddr;

use futures::{
    future,
    Future,
    Stream,
};

use tokio::net::tcp::{
    TcpStream,
    TcpListener,
};

use log::{debug, error};

use super::super::super::{
    Params as LodeParams,
    ErrorSeverity,
    lode::{self, Lode},
};

pub struct Params<N> {
    pub sock_addr: SocketAddr,
    pub lode_params: LodeParams<N>,
}

pub fn spawn<N>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
)
    -> Lode<Option<TcpStream>>
where N: AsRef<str> + Send + 'static,
{
    let Params { sock_addr, lode_params, } = params;

    lode::stream::spawn(
        executor,
        lode_params,
        sock_addr,
        init,
    )
}

fn init(
    sock_addr: SocketAddr,
)
    -> impl Future<Item = (impl Stream<Item = TcpStream, Error = ()>, SocketAddr), Error = ErrorSeverity<SocketAddr, ()>>
{
    debug!("TcpListener initialize");
    future::result(TcpListener::bind(&sock_addr))
        .then(move |bind_result| {
            match bind_result {
                Ok(tcp_listener) => {
                    let connections = tcp_listener
                        .incoming()
                        .map_err(|error| {
                            error!("connection accept error: {:?}", error);
                        });
                    Ok((connections, sock_addr))
                },
                Err(error) => {
                    error!("TcpListener bind error: {:?}", error);
                    Err(ErrorSeverity::Recoverable { state: sock_addr, })
                }
            }
        })
}
