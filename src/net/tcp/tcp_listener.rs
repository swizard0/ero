use std::net::SocketAddr;

use futures::{
    future,
    Future,
    Stream,
};

use tokio::net::tcp::{
    TcpStream,
    TcpListener,
    Incoming,
};

use log::{
    debug,
    warn,
    error,
};

use super::super::super::{
    ErrorSeverity,
    lode::{
        self,
        Lode,
        Resource,
    },
};

pub struct Params<N> {
    sock_addr: SocketAddr,
    lode_params: lode::Params<N>,
}

pub fn spawn<N>(
    executor: &tokio::runtime::TaskExecutor,
    params: Params<N>,
)
    -> Lode<TcpStream>
where N: AsRef<str> + Send + 'static,
{
    let Params { sock_addr, lode_params, } = params;

    lode::spawn(
        executor,
        lode_params,
        sock_addr,
        init,
        aquire,
        release,
        close,
    )
}

struct BoundState {
    incoming: Incoming,
    sock_addr: SocketAddr,
}

struct UnboundState {
    sock_addr: SocketAddr,
}

fn init(
    sock_addr: SocketAddr,
)
    -> impl Future<Item = Resource<BoundState, UnboundState>, Error = ErrorSeverity<SocketAddr, ()>>
{
    debug!("TcpListener initialize");
    future::result(TcpListener::bind(&sock_addr))
        .then(move |bind_result| {
            match bind_result {
                Ok(tcp_listener) => {
                    Ok(Resource::Available(BoundState {
                        incoming: tcp_listener.incoming(),
                        sock_addr,
                    }))
                },
                Err(error) => {
                    error!("TcpListener bind error: {:?}", error);
                    Err(ErrorSeverity::Recoverable { state: sock_addr, })
                }
            }
        })
}

fn aquire(
    bound_state: BoundState,
)
    -> impl Future<Item = (TcpStream, Resource<BoundState, UnboundState>), Error = ErrorSeverity<SocketAddr, ()>>
{
    debug!("TcpListener aquire");
    let BoundState { incoming, sock_addr, } = bound_state;
    incoming
        .into_future()
        .then(move |incoming_result| {
            match incoming_result {
                Ok((Some(tcp_stream), incoming)) =>
                    Ok((tcp_stream, Resource::Available(BoundState { incoming, sock_addr, }))),
                Ok((None, _incoming)) => {
                    error!("TcpListener accept stream suddenly closed");
                    Err(ErrorSeverity::Recoverable { state: sock_addr, })
                },
                Err(error) => {
                    error!("TcpListener accept error: {:?}", error);
                    Err(ErrorSeverity::Recoverable { state: sock_addr, })
                },
            }
        })
}

fn release(
    state: Resource<BoundState, UnboundState>,
    maybe_tcp_stream: Option<TcpStream>,
)
    -> impl Future<Item = Resource<BoundState, UnboundState>, Error = ErrorSeverity<SocketAddr, ()>>
{
    future::result(Ok(match (state, maybe_tcp_stream) {
        (Resource::Available(bound_state), Some(..)) => {
            debug!("TcpListener reimbursement (dropping resource)");
            Resource::Available(bound_state)
        },
        (Resource::Available(bound_state), None) => {
            debug!("TcpListener release (resource lost)");
            Resource::Available(bound_state)
        },
        (Resource::OutOfStock(unbound_state), _) => {
            warn!("unbound state (probably something went wrong)");
            Resource::OutOfStock(unbound_state)
        },
    }))
}

fn close(
    state: Resource<BoundState, UnboundState>,
)
    -> impl Future<Item = SocketAddr, Error = ()>
{
    future::result(Ok(match state {
        Resource::Available(BoundState { sock_addr, .. }) =>
            sock_addr,
        Resource::OutOfStock(UnboundState { sock_addr, }) =>
            sock_addr,
    }))
}
