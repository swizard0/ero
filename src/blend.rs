use std::{
    marker::PhantomData,
};

use futures::{
    Poll,
    Async,
    Future,
    Stream,
};

pub struct Blender<U, V> {
    _marker: PhantomData<(U, V)>,
}

impl<U, V> Blender<U, V> {
    pub fn new() -> Blender<U, V> {
        Blender {
            _marker: PhantomData,
        }
    }

    pub fn add<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> BlenderNil<U, V, S, MO, ME> {
        BlenderNil {
            inner: Some(Inner { stream, map_ok, map_err, }),
            _marker: PhantomData,
        }
    }
}

struct Inner<S, MO, ME> {
    stream: S,
    map_ok: MO,
    map_err: ME,
}

pub struct BlenderNil<U, V, S, MO, ME> {
    inner: Option<Inner<S, MO, ME>>,
    _marker: PhantomData<(U, V)>,
}

impl<U, V, SS, MOS, MOE> BlenderNil<U, V, SS, MOS, MOE> {
    pub fn add<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> BlenderCons<U, V, BlenderNil<U, V, SS, MOS, MOE>, S, MO, ME> {
        BlenderCons {
            inner: Some(Inner { stream, map_ok, map_err, }),
            cdr: Some(self),
            _marker: PhantomData,
        }
    }
}

pub struct BlenderCons<U, V, P, S, MO, ME> {
    inner: Option<Inner<S, MO, ME>>,
    cdr: Option<P>,
    _marker: PhantomData<(U, V)>,
}

impl<U, V, P, SS, MOS, MES> BlenderCons<U, V, P, SS, MOS, MES> {
    pub fn add<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> BlenderCons<U, V, BlenderCons<U, V, P, SS, MOS, MES>, S, MO, ME> {
        BlenderCons {
            inner: Some(Inner { stream, map_ok, map_err, }),
            cdr: Some(self),
            _marker: PhantomData,
        }
    }
}

impl<U, V, S, MO, ME> Inner<S, MO, ME> where S: Stream, MO: Fn(Option<S::Item>) -> U, ME: Fn(S::Error) -> V {
    fn try_poll_inner(mut self) -> (Poll<U, V>, Option<Self>) {
        match self.stream.poll() {
            Ok(Async::NotReady) => {
                (Ok(Async::NotReady), Some(self))
            },
            Ok(Async::Ready(Some(item))) => {
                let item = (self.map_ok)(Some(item));
                (Ok(Async::Ready(item)), Some(self))
            },
            Ok(Async::Ready(None)) => {
                let item = (self.map_ok)(None);
                (Ok(Async::Ready(item)), None)
            },
            Err(error) => {
                let error = (self.map_err)(error);
                (Err(error), None)
            },
        }
    }
}

trait TryFuture {
    type Item;
    type Error;

    fn try_poll(&mut self) -> Option<Poll<Self::Item, Self::Error>>;
}

impl<U, V, S, MO, ME> TryFuture for BlenderNil<U, V, S, MO, ME>
where S: Stream,
      MO: Fn(Option<S::Item>) -> U,
      ME: Fn(S::Error) -> V,
{
    type Item = (U, Self);
    type Error = (V, Self);

    fn try_poll(&mut self) -> Option<Poll<Self::Item, Self::Error>> {
        let inner = self.inner.take()?;
        let (poll, maybe_inner) = inner.try_poll_inner();
        Some(match poll {
            Ok(Async::NotReady) => {
                self.inner = maybe_inner;
                Ok(Async::NotReady)
            },
            Ok(Async::Ready(item)) =>
                Ok(Async::Ready((item, BlenderNil { inner: maybe_inner, _marker: PhantomData, }))),
            Err(error) =>
                Err((error, BlenderNil { inner: maybe_inner, _marker: PhantomData, })),
        })
    }
}

impl<U, V, S, MO, ME> Future for BlenderNil<U, V, S, MO, ME>
where S: Stream,
      MO: Fn(Option<S::Item>) -> U,
      ME: Fn(S::Error) -> V,
{
    type Item = (U, Self);
    type Error = (V, Self);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.try_poll().expect("cannot poll BlenderNil twice")
    }
}

// impl<U, V, P, S, MO, ME> TryFuture for BlenderCons<U, V, P, S, MO, ME>
// where P: TryFuture<Item = (U, P), Error = (V, P)>,
//       S: Stream,
//       MO: Fn(Option<S::Item>) -> U,
//       ME: Fn(S::Error) -> V,
// {
//     type Item = (U, Self);
//     type Error = (V, Self);

//     fn try_poll(&mut self) -> Option<Poll<Self::Item, Self::Error>> {
//         let inner = self.inner.take()?;
//         let mut cdr = self.cdr.take()?;
//         match cdr.try_poll() {
//             None | Some(Ok(Async::NotReady)) => {
//                 unimplemented!()
//             },
//             Some(Ok(Async::Ready((item, cdr)))) => {
//                 let blender = BlenderCons {
//                     inner: Some(inner),
//                     cdr: Some(cdr),
//                     _marker: PhantomData,
//                 };
//                 Some(Ok(Async::Ready((item, blender))))
//             },
//             Some(Err((error, cdr))) => {
//                 let blender = BlenderCons {
//                     inner: Some(inner),
//                     cdr: Some(cdr),
//                     _marker: PhantomData,
//                 };
//                 Some(Err((error, blender)))
//             },
//         }
//     }
// }
