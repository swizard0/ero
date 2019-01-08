use futures::{
    Poll,
    Async,
};

pub struct Blender(());

impl Blender {
    pub fn new() -> Blender {
        Blender(())
    }

    pub fn add_stream<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> stream::Nil<S, MO, ME> {
        stream::make_nil(stream, map_ok, map_err)
    }

    pub fn add_gen_future<G, N, MO, ME>(self, gen: G, next: N, map_ok: MO, map_err: ME) -> gen::Nil<G, N, MO, ME> {
        gen::make_nil(gen, next, map_ok, map_err)
    }
}

pub trait Decompose {
    type Parts;

    fn decompose(self) -> Self::Parts;
}

pub trait TryFuture {
    type Item;
    type Error;

    fn try_poll(&mut self) -> Option<Poll<Self::Item, Self::Error>>;
}

fn try_poll_to_poll<U, V, F>(
    future: &mut F
)
    -> Poll<Option<(U, F)>, (V, F)>
where F: TryFuture<Item = (U, F), Error = (V, F)>
{
    match future.try_poll() {
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

mod stream {
    use std::mem;
    use futures::{
        Poll,
        Async,
        Future,
        Stream,
    };

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

    pub struct Nil<S, MO, ME> {
        inner: Inner<S, MO, ME>,
    }

    pub fn make_nil<S, MO, ME>(stream: S, map_ok: MO, map_err: ME) -> Nil<S, MO, ME> {
        Nil { inner: Inner::Active { stream, map_ok, map_err, }, }
    }

    impl<SS, MOS, MES> Nil<SS, MOS, MES> {
        pub fn add_stream<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> Cons<Nil<SS, MOS, MES>, S, MO, ME> {
            make_cons(stream, map_ok, map_err, self)
        }

        pub fn add_gen_future<G, N, MO, ME>(self, gen: G, next: N, map_ok: MO, map_err: ME) -> super::gen::Cons<Nil<SS, MOS, MES>, G, N, MO, ME> {
            super::gen::make_cons(gen, next, map_ok, map_err, self)
        }
    }

    pub struct Cons<P, S, MO, ME> {
        inner: Inner<S, MO, ME>,
        cdr: Option<P>,
    }

    pub fn make_cons<P, S, MO, ME>(stream: S, map_ok: MO, map_err: ME, cdr: P) -> Cons<P, S, MO, ME> {
        Cons {
            inner: Inner::Active { stream, map_ok, map_err },
            cdr: Some(cdr),
        }
    }

    impl<P, SS, MOS, MES> Cons<P, SS, MOS, MES> {
        pub fn add_stream<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> Cons<Cons<P, SS, MOS, MES>, S, MO, ME> {
            make_cons(stream, map_ok, map_err, self)
        }

        pub fn add_gen_future<G, N, MO, ME>(self, gen: G, next: N, map_ok: MO, map_err: ME) -> super::gen::Cons<Cons<P, SS, MOS, MES>, G, N, MO, ME> {
            super::gen::make_cons(gen, next, map_ok, map_err, self)
        }
    }

    impl<S, MO, ME> super::Decompose for Nil<S, MO, ME> {
        type Parts = S;

        fn decompose(self) -> Self::Parts {
            self.inner.into_stream()
        }
    }

    impl<P, S, MO, ME> super::Decompose for Cons<P, S, MO, ME> where P: super::Decompose {
        type Parts = (S, P::Parts);

        fn decompose(self) -> Self::Parts {
            (
                self.inner.into_stream(),
                self.cdr.expect("decomposing already triggered blend::stream::Cons").decompose()
            )
        }
    }

    impl<S, MO, ME> Inner<S, MO, ME> {
        fn into_stream(self) -> S {
            match self {
                Inner::Active { stream, .. } | Inner::Disabled { stream, } =>
                    stream,
                Inner::Taken =>
                    panic!("decomposing already triggered blend::stream::Inner"),
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
                    panic!("cannot poll blend::stream::Inner twice"),
                inner @ Inner::Disabled { .. } => {
                    *self = inner;
                    None
                },
            }
        }
    }

    impl<U, V, S, MO, ME> super::TryFuture for Nil<S, MO, ME>
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
                    Ok(Async::Ready((item, Nil { inner, }))),
                Err((error, inner)) =>
                    Err((error, Nil { inner, })),
            })
        }
    }

    impl<U, V, S, MO, ME> Future for Nil<S, MO, ME>
    where S: Stream,
          MO: Fn(Option<S::Item>) -> U,
          ME: Fn(S::Error) -> V,
    {
        type Item = Option<(U, Self)>;
        type Error = (V, Self);

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            super::try_poll_to_poll(self)
        }
    }

    impl<U, V, P, S, MO, ME> super::TryFuture for Cons<P, S, MO, ME>
    where P: super::TryFuture<Item = (U, P), Error = (V, P)>,
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
                            Ok(Async::Ready((item, Cons { inner, cdr: Some(cdr), }))),
                        Err((error, inner)) =>
                            Err((error, Cons { inner, cdr: Some(cdr), })),
                    })
                },
                Some(Ok(Async::Ready((item, cdr)))) => {
                    let blender = Cons {
                        inner: mem::replace(&mut self.inner, Inner::Taken),
                        cdr: Some(cdr),
                    };
                    Some(Ok(Async::Ready((item, blender))))
                },
                Some(Err((error, cdr))) => {
                    let blender = Cons {
                        inner: mem::replace(&mut self.inner, Inner::Taken),
                        cdr: Some(cdr),
                    };
                    Some(Err((error, blender)))
                },
            }
        }
    }

    impl<U, V, P, S, MO, ME> Future for Cons<P, S, MO, ME>
    where P: super::TryFuture<Item = (U, P), Error = (V, P)>,
          S: Stream,
          MO: Fn(Option<S::Item>) -> U,
          ME: Fn(S::Error) -> V,
    {
        type Item = Option<(U, Self)>;
        type Error = (V, Self);

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            super::try_poll_to_poll(self)
        }
    }
}

mod gen {
    use std::mem;
    use futures::{
        Poll,
        Async,
        Future,
    };

    enum Inner<G, N, MO, ME> {
        Taken,
        Active {
            gen: G,
            next: N,
            map_ok: MO,
            map_err: ME,
        },
        Disabled {
            gen: G,
            next: N,
        },
    }

    pub struct Nil<G, N, MO, ME> {
        inner: Inner<G, N, MO, ME>,
    }

    pub fn make_nil<G, N, MO, ME>(gen: G, next: N, map_ok: MO, map_err: ME) -> Nil<G, N, MO, ME> {
        Nil { inner: Inner::Active { gen, next, map_ok, map_err, }, }
    }

    impl<GG, NG, MOG, MEG> Nil<GG, NG, MOG, MEG> {
        pub fn add_stream<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> super::stream::Cons<Nil<GG, NG, MOG, MEG>, S, MO, ME> {
            super::stream::make_cons(stream, map_ok, map_err, self)
        }

        pub fn add_gen_future<G, N, MO, ME>(self, gen: G, next: N, map_ok: MO, map_err: ME) -> Cons<Nil<GG, NG, MOG, MEG>, G, N, MO, ME> {
            make_cons(gen, next, map_ok, map_err, self)
        }
    }

    pub struct Cons<P, G, N, MO, ME> {
        inner: Inner<G, N, MO, ME>,
        cdr: Option<P>,
    }

    pub fn make_cons<P, G, N, MO, ME>(gen: G, next: N, map_ok: MO, map_err: ME, cdr: P) -> Cons<P, G, N, MO, ME> {
        Cons {
            inner: Inner::Active { gen, next, map_ok, map_err, },
            cdr: Some(cdr),
        }
    }

    impl<P, GG, NG, MOG, MEG> Cons<P, GG, NG, MOG, MEG> {
        pub fn add_stream<S, MO, ME>(self, stream: S, map_ok: MO, map_err: ME) -> super::stream::Cons<Cons<P, GG, NG, MOG, MEG>, S, MO, ME> {
            super::stream::make_cons(stream, map_ok, map_err, self)
        }

        pub fn add_gen_future<G, N, MO, ME>(self, gen: G, next: N, map_ok: MO, map_err: ME) -> Cons<Cons<P, GG, NG, MOG, MEG>, G, N, MO, ME> {
            make_cons(gen, next, map_ok, map_err, self)
        }
    }

    impl<G, N, MO, ME> super::Decompose for Nil<G, N, MO, ME> {
        type Parts = (G, N);

        fn decompose(self) -> Self::Parts {
            self.inner.into_gen_future()
        }
    }

    impl<P, G, N, MO, ME> super::Decompose for Cons<P, G, N, MO, ME> where P: super::Decompose {
        type Parts = ((G, N), P::Parts);

        fn decompose(self) -> Self::Parts {
            (
                self.inner.into_gen_future(),
                self.cdr.expect("decomposing already triggered blend::gen::Cons").decompose()
            )
        }
    }

    impl<G, N, MO, ME> Inner<G, N, MO, ME> {
        fn into_gen_future(self) -> (G, N) {
            match self {
                Inner::Active { gen, next, .. } | Inner::Disabled { gen, next, } =>
                    (gen, next),
                Inner::Taken =>
                    panic!("decomposing already triggered blend::gen::Inner"),
            }
        }
    }

    impl<U, V, W, T, E, G, N, MO, ME> Inner<G, N, MO, ME>
    where G: Future<Item = (Option<T>, W), Error = (E, W)>,
          N: Fn(W) -> G,
          MO: Fn(Option<T>) -> U,
          ME: Fn(E) -> V,
    {
        fn try_poll_inner(&mut self) -> Option<Poll<(U, Self), (V, Self)>> {
            match mem::replace(self, Inner::Taken) {
                Inner::Active { mut gen, next, map_ok, map_err, } =>
                    Some(match gen.poll() {
                        Ok(Async::NotReady) => {
                            *self = Inner::Active { gen, next, map_ok, map_err, };
                            Ok(Async::NotReady)
                        },
                        Ok(Async::Ready((Some(item), kont))) => {
                            let item = map_ok(Some(item));
                            mem::replace(&mut gen, next(kont));
                            Ok(Async::Ready((item, Inner::Active { gen, next, map_ok, map_err, })))
                        },
                        Ok(Async::Ready((None, kont))) => {
                            let item = map_ok(None);
                            mem::replace(&mut gen, next(kont));
                            Ok(Async::Ready((item, Inner::Disabled { gen, next, })))
                        },
                        Err((error, kont)) => {
                            let error = map_err(error);
                            mem::replace(&mut gen, next(kont));
                            Err((error, Inner::Disabled { gen, next, }))
                        },
                    }),
                Inner::Taken =>
                    panic!("cannot poll blend::gen::Inner twice"),
                inner @ Inner::Disabled { .. } => {
                    *self = inner;
                    None
                },
            }
        }
    }

    impl<U, V, W, T, E, G, N, MO, ME> super::TryFuture for Nil<G, N, MO, ME>
    where G: Future<Item = (Option<T>, W), Error = (E, W)>,
          N: Fn(W) -> G,
          MO: Fn(Option<T>) -> U,
          ME: Fn(E) -> V,
    {
        type Item = (U, Self);
        type Error = (V, Self);

        fn try_poll(&mut self) -> Option<Poll<Self::Item, Self::Error>> {
            Some(match self.inner.try_poll_inner()? {
                Ok(Async::NotReady) =>
                    Ok(Async::NotReady),
                Ok(Async::Ready((item, inner))) =>
                    Ok(Async::Ready((item, Nil { inner, }))),
                Err((error, inner)) =>
                    Err((error, Nil { inner, })),
            })
        }
    }

    impl<U, V, W, T, E, G, N, MO, ME> Future for Nil<G, N, MO, ME>
    where G: Future<Item = (Option<T>, W), Error = (E, W)>,
          N: Fn(W) -> G,
          MO: Fn(Option<T>) -> U,
          ME: Fn(E) -> V,
    {
        type Item = Option<(U, Self)>;
        type Error = (V, Self);

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            super::try_poll_to_poll(self)
        }
    }

    impl<U, V, W, P, T, E, G, N, MO, ME> super::TryFuture for Cons<P, G, N, MO, ME>
    where P: super::TryFuture<Item = (U, P), Error = (V, P)>,
          G: Future<Item = (Option<T>, W), Error = (E, W)>,
          N: Fn(W) -> G,
          MO: Fn(Option<T>) -> U,
          ME: Fn(E) -> V,
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
                            Ok(Async::Ready((item, Cons { inner, cdr: Some(cdr), }))),
                        Err((error, inner)) =>
                            Err((error, Cons { inner, cdr: Some(cdr), })),
                    })
                },
                Some(Ok(Async::Ready((item, cdr)))) => {
                    let blender = Cons {
                        inner: mem::replace(&mut self.inner, Inner::Taken),
                        cdr: Some(cdr),
                    };
                    Some(Ok(Async::Ready((item, blender))))
                },
                Some(Err((error, cdr))) => {
                    let blender = Cons {
                        inner: mem::replace(&mut self.inner, Inner::Taken),
                        cdr: Some(cdr),
                    };
                    Some(Err((error, blender)))
                },
            }
        }
    }

    impl<U, V, W, P, T, E, G, N, MO, ME> Future for Cons<P, G, N, MO, ME>
    where P: super::TryFuture<Item = (U, P), Error = (V, P)>,
          G: Future<Item = (Option<T>, W), Error = (E, W)>,
          N: Fn(W) -> G,
          MO: Fn(Option<T>) -> U,
          ME: Fn(E) -> V,
    {
        type Item = Option<(U, Self)>;
        type Error = (V, Self);

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            super::try_poll_to_poll(self)
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
    fn blend_3_stream() {
        let stream_a = stream::iter_ok(vec![0, 1, 2]);
        let stream_b = stream::iter_ok(vec![true, false]);
        let stream_c = stream::iter_ok(vec!["5"]);

        #[derive(PartialEq, Debug)]
        enum Var3<A, B, C> { A(A), B(B), C(C), }

        let blender = Blender::new()
            .add_stream(stream_a, Var3::A, |()| ())
            .add_stream(stream_b, Var3::B, |()| ())
            .add_stream(stream_c, Var3::C, |()| ());

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
    fn blend_3_gen() {
        let stream_a = stream::iter_ok(vec![0, 1, 2]);
        let stream_b = stream::iter_ok(vec![true, false]);
        let stream_c = stream::iter_ok(vec!["5"]);

        #[derive(PartialEq, Debug)]
        enum Var3<A, B, C> { A(A), B(B), C(C), }

        use futures::Stream;
        let blender = Blender::new()
            .add_gen_future(stream_a.into_future(), Stream::into_future, Var3::A, |()| ())
            .add_stream(stream_b, Var3::B, |()| ())
            .add_gen_future(stream_c.into_future(), Stream::into_future, Var3::C, |()| ());

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
            .add_stream(stream_ok, Ok, |x| x)
            .add_stream(stream_err, Err, |x| x);

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
            .add_stream(stream_a, Var3::A, |()| ())
            .add_stream(stream_b, Var3::B, |()| ())
            .add_stream(stream_c, Var3::C, |()| ());

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
