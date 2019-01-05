use std::net::SocketAddr;

use futures::{
    future,
    Future,
};

use tokio::net::TcpStream;

use log::{debug, error};

use super::super::super::{
    ErrorSeverity,
    lode::{
        self,
        Lode,
        Resource,
    },
};

pub struct Params<N> {
    pub sock_addr: SocketAddr,
    pub lode_params: lode::Params<N>,
}

pub fn spawn<N>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
)
    -> Lode<TcpStream>
where N: AsRef<str> + Send + 'static,
{
    let Params { sock_addr, lode_params, } = params;

    lode::uniq::spawn(
        executor,
        lode_params,
        sock_addr,
        init,
        aquire,
        release,
        close_main,
        close_wait,
    )
}

struct ConnectedState {
    tcp_stream: TcpStream,
    sock_addr: SocketAddr,
}

struct DisconnectedState {
    sock_addr: SocketAddr,
}

fn init(
    sock_addr: SocketAddr,
)
    -> impl Future<Item = ConnectedState, Error = ErrorSeverity<SocketAddr, ()>>
{
    debug!("TcpStream initialize");
    TcpStream::connect(&sock_addr)
        .then(move |connect_result| {
            match connect_result {
                Ok(tcp_stream) =>
                    Ok(ConnectedState {
                        tcp_stream,
                        sock_addr,
                    }),
                Err(error) => {
                    error!("TcpStream connect error: {:?}", error);
                    Err(ErrorSeverity::Recoverable { state: sock_addr, })
                },
            }
        })
}

fn aquire(
    connected_state: ConnectedState,
)
    -> impl Future<Item = (TcpStream, DisconnectedState), Error = ErrorSeverity<SocketAddr, ()>>
{
    debug!("TcpStream aquire");
    let ConnectedState { tcp_stream, sock_addr, } = connected_state;
    future::result(Ok((tcp_stream, DisconnectedState { sock_addr, })))
}

fn release(
    state: DisconnectedState,
    maybe_tcp_stream: Option<TcpStream>,
)
    -> impl Future<Item = Resource<ConnectedState, DisconnectedState>, Error = ErrorSeverity<SocketAddr, ()>>
{
    future::result(Ok(match (state, maybe_tcp_stream) {
        (DisconnectedState { sock_addr, }, Some(tcp_stream)) => {
            debug!("TcpStream reimbursement");
            Resource::Available(ConnectedState { sock_addr, tcp_stream, })
        },
        (disconnected_state, None) => {
            debug!("TcpStream is completely lost");
            Resource::OutOfStock(disconnected_state)
        },
    }))
}

fn close_main(state: ConnectedState) -> impl Future<Item = SocketAddr, Error = ()> {
    future::result(Ok(state.sock_addr))
}

fn close_wait(state: DisconnectedState) -> impl Future<Item = SocketAddr, Error = ()> {
    future::result(Ok(state.sock_addr))
}
