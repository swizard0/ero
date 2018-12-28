use futures::{
    Sink,
    Future,
    Stream,
    future,
    sync::{
        mpsc,
        oneshot,
    },
};

use super::{
    Resource,
    super::{
        RestartStrategy,
        ErrorSeverity,
    },
};

#[test]
fn check_sequence() {
    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    let executor = runtime.executor();

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Notify {
        InitFn,
        AquireFn,
        ReleaseFn,
        CloseFn,
    }

    struct ResourceItem;

    let (tx, rx) = mpsc::unbounded();
    let super::Lode { aquire_tx, release_tx, shutdown_tx, } = super::spawn(
        &executor,
        super::Params {
            name: "check_sequence",
            restart_strategy: RestartStrategy::RestartImmediately,
        },
        tx,
        |tx| {
            tx.unbounded_send(Notify::InitFn)
                .map_err(|_| ErrorSeverity::Fatal(()))
                .map(|_| Resource::Available(tx))
        },
        |tx| {
            tx.unbounded_send(Notify::AquireFn)
                .map_err(|_| ErrorSeverity::Fatal(()))
                .map(|_| (ResourceItem, Resource::Available(tx)))
        },
        |resource, _maybe_resource| {
            let tx = match resource {
                Resource::Available(tx) => tx,
                Resource::OutOfStock(tx) => tx,
            };
            tx.unbounded_send(Notify::ReleaseFn)
                .map_err(|_| ErrorSeverity::Fatal(()))
                .map(|_| Resource::Available(tx))
        },
        |resource| {
            let tx = match resource {
                Resource::Available(tx) => tx,
                Resource::OutOfStock(tx) => tx,
            };
            tx.unbounded_send(Notify::CloseFn)
                .map_err(|_| ())
                .map(|_| tx)
        }
    );

    let result: Result<(), ()> = runtime.block_on(
        future::result(Ok((aquire_tx, release_tx, shutdown_tx, rx)))
            .and_then(|(aquire_tx, release_tx, shutdown_tx, rx)| {
                let (resource_tx, resource_rx) = oneshot::channel();
                aquire_tx
                    .send(super::AquireReq { reply_tx: resource_tx, })
                    .then(|result| {
                        if let Ok(aquire_tx) = result {
                            Ok((aquire_tx, release_tx, shutdown_tx, rx, resource_rx))
                        } else {
                            panic!("aquire tx endpoint dropped unexpectedly");
                        }
                    })
            })
            .and_then(|(aquire_tx, release_tx, shutdown_tx, rx, resource_rx)| {
                rx.into_future()
                    .then(|result| {
                        match result {
                            Ok((Some(Notify::InitFn), rx)) =>
                                Ok((aquire_tx, release_tx, shutdown_tx, rx, resource_rx)),
                            Ok((other, _rx)) =>
                                panic!("expected tracking rx to be InitFn but it is: {:?}", other),
                            Err(..) =>
                                panic!("expected tracking rx to be InitFn"),
                        }
                    })
            })
            .and_then(|(aquire_tx, release_tx, shutdown_tx, rx, resource_rx)| {
                rx.into_future()
                    .then(|result| {
                        match result {
                            Ok((Some(Notify::AquireFn), rx)) =>
                                Ok((aquire_tx, release_tx, shutdown_tx, rx, resource_rx)),
                            Ok((other, _rx)) =>
                                panic!("expected tracking rx to be AquireFn but it is: {:?}", other),
                            Err(..) =>
                                panic!("expected tracking rx to be AquireFn"),
                        }
                    })
            })
            .and_then(|(aquire_tx, release_tx, shutdown_tx, rx, resource_rx)| {
                resource_rx
                    .then(|result| {
                        match result {
                            Ok(ResourceItem) =>
                                Ok((aquire_tx, release_tx, shutdown_tx, rx)),
                            Err(..) =>
                                panic!("expected resource but resource task is gone"),
                        }
                    })
            })
            .and_then(|(aquire_tx, release_tx, shutdown_tx, rx)| {
                release_tx
                    .send(super::ReleaseReq::Reimburse(ResourceItem))
                    .then(|result| {
                        if let Ok(release_tx) = result {
                            Ok((aquire_tx, release_tx, shutdown_tx, rx))
                        } else {
                            panic!("release tx endpoint dropped unexpectedly");
                        }
                    })
            })
            .and_then(|(aquire_tx, release_tx, shutdown_tx, rx)| {
                rx.into_future()
                    .then(|result| {
                        match result {
                            Ok((Some(Notify::ReleaseFn), rx)) =>
                                Ok((aquire_tx, release_tx, shutdown_tx, rx)),
                            Ok((other, _rx)) =>
                                panic!("expected tracking rx to be ReleaseFn but it is: {:?}", other),
                            Err(..) =>
                                panic!("expected tracking rx to be ReleaseFn"),
                        }
                    })
            })
            .and_then(|(aquire_tx, release_tx, shutdown_tx, rx)| {
                let (resource_tx, resource_rx) = oneshot::channel();
                aquire_tx
                    .send(super::AquireReq { reply_tx: resource_tx, })
                    .then(|result| {
                        if let Ok(aquire_tx) = result {
                            Ok((aquire_tx, release_tx, shutdown_tx, rx, resource_rx))
                        } else {
                            panic!("aquire tx endpoint dropped unexpectedly");
                        }
                    })
            })
            .and_then(|(aquire_tx, release_tx, shutdown_tx, rx, resource_rx)| {
                rx.into_future()
                    .then(|result| {
                        match result {
                            Ok((Some(Notify::AquireFn), rx)) =>
                                Ok((aquire_tx, release_tx, shutdown_tx, rx, resource_rx)),
                            Ok((other, _rx)) =>
                                panic!("expected tracking rx to be AquireFn but it is: {:?}", other),
                            Err(..) =>
                                panic!("expected tracking rx to be AquireFn"),
                        }
                    })
            })
            .and_then(|(aquire_tx, release_tx, shutdown_tx, rx, resource_rx)| {
                resource_rx
                    .then(|result| {
                        match result {
                            Ok(ResourceItem) =>
                                Ok((aquire_tx, release_tx, shutdown_tx, rx)),
                            Err(..) =>
                                panic!("expected resource but resource task is gone"),
                        }
                    })
            })
            .and_then(move |(_aquire_tx, _release_tx, shutdown_tx, rx)| {
                let _ = shutdown_tx.send(super::Shutdown);
                rx.into_future()
                    .then(|result| {
                        match result {
                            Ok((None, _rx)) =>
                                Ok(()),
                            Ok((Some(notify), _rx)) =>
                                panic!("expected tracking rx to be dropped but it is: {:?}", notify),
                            Err(..) =>
                                panic!("expected tracking rx to be dropped"),
                        }
                    })
            })
    );
    assert_eq!(result, Ok(()));
}
