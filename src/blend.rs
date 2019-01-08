use std::mem;

use futures::{
    Poll,
    Async,
    Future,
    Stream,
};

pub struct Blender(());

impl Blender {
    pub fn new() -> Blender {
        Blender(())
    }

    pub fn add<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> BlenderNil<S, MO, ME> {
        BlenderNil {
            inner: Inner::Active { stream, map_ok, map_err, },
        }
    }
}

enum Inner<S, MO, ME> {
    Taken,
    Active {
        stream: S,
        map_ok: MO,
        map_err: ME,
    },
    Disabled {
        stream: S,
    },
}

pub struct BlenderNil<S, MO, ME> {
    inner: Inner<S, MO, ME>,
}

impl<SS, MOS, MOE> BlenderNil<SS, MOS, MOE> {
    pub fn add<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> BlenderCons<BlenderNil<SS, MOS, MOE>, S, MO, ME> {
        BlenderCons {
            inner: Inner::Active { stream, map_ok, map_err, },
            cdr: Some(self),
        }
    }
}

pub struct BlenderCons<P, S, MO, ME> {
    inner: Inner<S, MO, ME>,
    cdr: Option<P>,
}

impl<P, SS, MOS, MES> BlenderCons<P, SS, MOS, MES> {
    pub fn add<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> BlenderCons<BlenderCons<P, SS, MOS, MES>, S, MO, ME> {
        BlenderCons {
            inner: Inner::Active { stream, map_ok, map_err, },
            cdr: Some(self),
        }
    }
}

pub trait Decompose {
    type Parts;

    fn decompose(self) -> Self::Parts;
}

impl<S, MO, ME> Decompose for BlenderNil<S, MO, ME> {
    type Parts = S;

    fn decompose(self) -> Self::Parts {
        self.inner.into_stream()
    }
}

impl<P, S, MO, ME> Decompose for BlenderCons<P, S, MO, ME> where P: Decompose {
    type Parts = (S, P::Parts);

    fn decompose(self) -> Self::Parts {
        (
            self.inner.into_stream(),
            self.cdr.expect("decomposing already triggered BlendCons").decompose()
        )
    }
}

impl<S, MO, ME> Inner<S, MO, ME> {
    fn into_stream(self) -> S {
        match self {
            Inner::Active { stream, .. } | Inner::Disabled { stream, } =>
                stream,
            Inner::Taken =>
                panic!("decomposing already triggered Blend"),
        }
    }
}

impl<U, V, S, MO, ME> Inner<S, MO, ME> where S: Stream, MO: Fn(Option<S::Item>) -> U, ME: Fn(S::Error) -> V {
    fn try_poll_inner(&mut self) -> Option<Poll<(U, Self), (V, Self)>> {
        match mem::replace(self, Inner::Taken) {
            Inner::Active { mut stream, map_ok, map_err, } =>
                Some(match stream.poll() {
                    Ok(Async::NotReady) => {
                        *self = Inner::Active { stream, map_ok, map_err, };
                        Ok(Async::NotReady)
                    },
                    Ok(Async::Ready(Some(item))) => {
                        let item = map_ok(Some(item));
                        Ok(Async::Ready((item, Inner::Active { stream, map_ok, map_err, })))
                    },
                    Ok(Async::Ready(None)) => {
                        let item = map_ok(None);
                        Ok(Async::Ready((item, Inner::Disabled { stream, })))
                    },
                    Err(error) => {
                        let error = map_err(error);
                        Err((error, Inner::Disabled { stream, }))
                    },
                }),
            Inner::Taken =>
                panic!("cannot poll Blend twice"),
            inner @ Inner::Disabled { .. } => {
                *self = inner;
                None
            },
        }
    }
}

pub trait TryFuture {
    type Item;
    type Error;

    fn try_poll(&mut self) -> Option<Poll<Self::Item, Self::Error>>;
}

impl<U, V, S, MO, ME> TryFuture for BlenderNil<S, MO, ME>
where S: Stream,
      MO: Fn(Option<S::Item>) -> U,
      ME: Fn(S::Error) -> V,
{
    type Item = (U, Self);
    type Error = (V, Self);

    fn try_poll(&mut self) -> Option<Poll<Self::Item, Self::Error>> {
        Some(match self.inner.try_poll_inner()? {
            Ok(Async::NotReady) =>
                Ok(Async::NotReady),
            Ok(Async::Ready((item, inner))) =>
                Ok(Async::Ready((item, BlenderNil { inner, }))),
            Err((error, inner)) =>
                Err((error, BlenderNil { inner, })),
        })
    }
}

impl<U, V, S, MO, ME> Future for BlenderNil<S, MO, ME>
where S: Stream,
      MO: Fn(Option<S::Item>) -> U,
      ME: Fn(S::Error) -> V,
{
    type Item = Option<(U, Self)>;
    type Error = (V, Self);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.try_poll() {
            Some(Ok(Async::Ready(item))) =>
                Ok(Async::Ready(Some(item))),
            Some(Ok(Async::NotReady)) =>
                Ok(Async::NotReady),
            Some(Err(error)) =>
                Err(error),
            None =>
                Ok(Async::Ready(None)),
        }
    }
}

impl<U, V, P, S, MO, ME> TryFuture for BlenderCons<P, S, MO, ME>
where P: TryFuture<Item = (U, P), Error = (V, P)>,
      S: Stream,
      MO: Fn(Option<S::Item>) -> U,
      ME: Fn(S::Error) -> V,
{
    type Item = (U, Self);
    type Error = (V, Self);

    fn try_poll(&mut self) -> Option<Poll<Self::Item, Self::Error>> {
        let mut cdr = self.cdr.take()?;
        match cdr.try_poll() {
            None | Some(Ok(Async::NotReady)) => {
                Some(match self.inner.try_poll_inner()? {
                    Ok(Async::NotReady) => {
                        self.cdr = Some(cdr);
                        Ok(Async::NotReady)
                    },
                    Ok(Async::Ready((item, inner))) =>
                        Ok(Async::Ready((item, BlenderCons { inner, cdr: Some(cdr), }))),
                    Err((error, inner)) =>
                        Err((error, BlenderCons { inner, cdr: Some(cdr), })),
                })
            },
            Some(Ok(Async::Ready((item, cdr)))) => {
                let blender = BlenderCons {
                    inner: mem::replace(&mut self.inner, Inner::Taken),
                    cdr: Some(cdr),
                };
                Some(Ok(Async::Ready((item, blender))))
            },
            Some(Err((error, cdr))) => {
                let blender = BlenderCons {
                    inner: mem::replace(&mut self.inner, Inner::Taken),
                    cdr: Some(cdr),
                };
                Some(Err((error, blender)))
            },
        }
    }
}

impl<U, V, P, S, MO, ME> Future for BlenderCons<P, S, MO, ME>
where P: TryFuture<Item = (U, P), Error = (V, P)>,
      S: Stream,
      MO: Fn(Option<S::Item>) -> U,
      ME: Fn(S::Error) -> V,
{
    type Item = Option<(U, Self)>;
    type Error = (V, Self);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.try_poll() {
            Some(Ok(Async::Ready(item))) =>
                Ok(Async::Ready(Some(item))),
            Some(Ok(Async::NotReady)) =>
                Ok(Async::NotReady),
            Some(Err(error)) =>
                Err(error),
            None =>
                Ok(Async::Ready(None)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{
        Instant,
        Duration,
    };
    use futures::{
        Future,
        stream,
    };
    use tokio::timer::Delay;
    use super::Blender;

    #[test]
    fn blend_3() {
        let stream_a = stream::iter_ok(vec![0, 1, 2]);
        let stream_b = stream::iter_ok(vec![true, false]);
        let stream_c = stream::iter_ok(vec!["5"]);

        #[derive(PartialEq, Debug)]
        enum Var3<A, B, C> { A(A), B(B), C(C), }

        let blender = Blender::new()
            .add(stream_a, Var3::A, |()| ())
            .add(stream_b, Var3::B, |()| ())
            .add(stream_c, Var3::C, |()| ());

        let (item, blender) = blender.wait().map_err(|e| e.0).unwrap().unwrap();
        assert_eq!(item, Var3::A(Some(0)));
        let (item, blender) = blender.wait().map_err(|e| e.0).unwrap().unwrap();
        assert_eq!(item, Var3::A(Some(1)));
        let (item, blender) = blender.wait().map_err(|e| e.0).unwrap().unwrap();
        assert_eq!(item, Var3::A(Some(2)));
        let (item, blender) = blender.wait().map_err(|e| e.0).unwrap().unwrap();
        assert_eq!(item, Var3::A(None));
        let (item, blender) = blender.wait().map_err(|e| e.0).unwrap().unwrap();
        assert_eq!(item, Var3::B(Some(true)));
        let (item, blender) = blender.wait().map_err(|e| e.0).unwrap().unwrap();
        assert_eq!(item, Var3::B(Some(false)));
        let (item, blender) = blender.wait().map_err(|e| e.0).unwrap().unwrap();
        assert_eq!(item, Var3::B(None));
        let (item, blender) = blender.wait().map_err(|e| e.0).unwrap().unwrap();
        assert_eq!(item, Var3::C(Some("5")));
        let (item, blender) = blender.wait().map_err(|e| e.0).unwrap().unwrap();
        assert_eq!(item, Var3::C(None));
        let next = blender.wait().map_err(|e| e.0).unwrap();
        assert!(next.is_none());
    }

    #[test]
    fn blend_delays() {
        use futures::{
            future::Either,
            stream::futures_unordered,
        };

        let stream_ok = futures_unordered(vec![
            Either::A(Delay::new(Instant::now() + Duration::from_millis(200)).map(|_| 0)),
            Either::B(Delay::new(Instant::now() + Duration::from_millis(100)).map(|_| 1)),
        ]);
        let stream_err = futures_unordered(vec![
            Either::A(Delay::new(Instant::now() + Duration::from_millis(300)).map(|_| "a")),
            Either::B(Delay::new(Instant::now() + Duration::from_millis(50)).map(|_| "b")),
        ]);

        let blender = Blender::new()
            .add(stream_ok, Ok, |x| x)
            .add(stream_err, Err, |x| x);

        let mut runtime = tokio::runtime::Runtime::new().unwrap();

        let (item, blender) = runtime.block_on(blender.map_err(|e| e.0)).unwrap().unwrap();
        assert_eq!(item, Err(Some("b")));
        let (item, blender) = runtime.block_on(blender.map_err(|e| e.0)).unwrap().unwrap();
        assert_eq!(item, Ok(Some(1)));
        let (item, blender) = runtime.block_on(blender.map_err(|e| e.0)).unwrap().unwrap();
        assert_eq!(item, Ok(Some(0)));
        let (item, blender) = runtime.block_on(blender.map_err(|e| e.0)).unwrap().unwrap();
        assert_eq!(item, Ok(None));
        let (item, blender) = runtime.block_on(blender.map_err(|e| e.0)).unwrap().unwrap();
        assert_eq!(item, Err(Some("a")));
        let (item, blender) = runtime.block_on(blender.map_err(|e| e.0)).unwrap().unwrap();
        assert_eq!(item, Err(None));
        let next = runtime.block_on(blender.map_err(|e| e.0)).unwrap();
        assert!(next.is_none());
    }

    #[test]
    fn blend_loop() {
        let stream_a = stream::iter_ok(vec![2, 3, 4]);
        let stream_b = stream::iter_ok(vec![true, false]);
        let stream_c = stream::iter_ok(vec!["5"]);

        #[derive(PartialEq, Debug)]
        enum Var3<A, B, C> { A(A), B(B), C(C), }

        let blender = Blender::new()
            .add(stream_a, Var3::A, |()| ())
            .add(stream_b, Var3::B, |()| ())
            .add(stream_c, Var3::C, |()| ());

        use futures::future::{loop_fn, Loop};

        let future = loop_fn((blender, 0), |(blender, counter)| {
            blender
                .map_err(|((), _blender)| ())
                .map(move |next| {
                match next {
                    None =>
                        Loop::Break(counter),
                    Some((Var3::A(None), blender)) =>
                        Loop::Continue((blender, counter)),
                    Some((Var3::A(Some(value)), blender)) =>
                        Loop::Continue((blender, counter + value)),
                    Some((Var3::B(None), blender)) =>
                        Loop::Continue((blender, counter)),
                    Some((Var3::B(Some(false)), blender)) =>
                        Loop::Continue((blender, counter)),
                    Some((Var3::B(Some(true)), blender)) =>
                        Loop::Continue((blender, counter + 1)),
                    Some((Var3::C(None), blender)) =>
                        Loop::Continue((blender, counter)),
                    Some((Var3::C(Some(string)), blender)) =>
                        Loop::Continue((blender, counter + string.parse::<i32>().unwrap())),
                }
            })
        });
        assert_eq!(future.wait().unwrap(), 15);
    }
}
