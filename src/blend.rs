use futures::{
    Poll,
    Async,
};

use std::marker::PhantomData;

#[derive(Debug)]
struct Nil;

#[derive(Debug)]
struct Cons<A, D> {
    car: A,
    cdr: D,
}

struct Node<S, O> {
    source: S,
    _parts: PhantomData<O>,
}

struct Blender<B> {
    chain: B,
}

impl Blender<Nil> {
    fn add<S>(self, source: S) -> Blender<Cons<Node<S, (S, ())>, Nil>> where S: Source {
        Blender {
            chain: Cons {
                car: Node {
                    source,
                    _parts: PhantomData,
                },
                cdr: self.chain,
            },
        }
    }
}

impl<R, Q, P> Blender<Cons<Node<Q, P>, R>> {
    fn add<S>(self, source: S) -> Blender<Cons<Node<S, (S, P)>, Cons<Node<Q, P>, R>>> where S: Source {
        Blender {
            chain: Cons {
                car: Node {
                    source,
                    _parts: PhantomData,
                },
                cdr: self.chain,
            },
        }
    }

    fn finish_sources(self) -> FolderStart<Cons<Node<Q, P>, R>> {
        FolderStart {
            unfolded: self.chain,
        }
    }
}

struct FolderStart<N> {
    unfolded: N,
}

impl<S, P, R> FolderStart<Cons<Node<S, (S, P)>, R>> {
    fn fold<FO, FE, T, E, U, V>(self, fold_ok: FO, fold_err: FE) -> Folder<R, Cons<FNode<S, FO, FE, (), P>, Nil>, U, V>
    where FO: Fn(T) -> U,
          FE: Fn(E) -> V,
    {
        Folder {
            unfolded: self.unfolded.cdr,
            folded: Cons {
                car: FNode {
                    source: self.unfolded.car.source,
                    fold_ok,
                    fold_err,
                    _zipper: PhantomData,
                },
                cdr: Nil,
            },
            _marker: PhantomData,
        }
    }
}

struct Folder<N, F, U, V> {
    unfolded: N,
    folded: F,
    _marker: PhantomData<(U, V)>,
}

struct FNode<S, FO, FE, L, R> {
    source: S,
    fold_ok: FO,
    fold_err: FE,
    _zipper: PhantomData<(L, R)>,
}

impl<S, P, R, F, Q, GO, GE, L, RA, RD, U, V> Folder<Cons<Node<S, (S, P)>, R>, Cons<FNode<Q, GO, GE, L, (RA, RD)>, F>, U, V> {
    fn fold<FO, FE, T, E>(
        self,
        fold_ok: FO,
        fold_err: FE,
    )
        -> Folder<R, Cons<FNode<S, FO, FE, (Q, L), RD>, Cons<FNode<Q, GO, GE, L, (RA, RD)>, F>>, U, V>
    where FO: Fn(T) -> U,
          FE: Fn(E) -> V,
    {
        Folder {
            unfolded: self.unfolded.cdr,
            folded: Cons {
                car: FNode {
                    source: self.unfolded.car.source,
                    fold_ok,
                    fold_err,
                    _zipper: PhantomData,
                },
                cdr: self.folded,
            },
            _marker: PhantomData,
        }
    }
}

impl<B, U, V> Folder<Nil, B, U, V> {
    fn finish(self) -> Ready<B, U, V> {
        Ready {
            blender: Some(self.folded),
            _marker: PhantomData,
        }
    }
}

struct Ready<B, U, V> {
    blender: Option<B>,
    _marker: PhantomData<(U, V)>,
}

trait Decompose {
    type Parts;

    fn decompose(self) -> Self::Parts;
}

impl Decompose for Nil {
    type Parts = ();

    fn decompose(self) -> Self::Parts {
        ()
    }
}

impl<S, FO, FE, L, R, P> Decompose for Cons<FNode<S, FO, FE, L, R>, P> where P: Decompose {
    type Parts = (S, P::Parts);

    fn decompose(self) -> Self::Parts {
        (self.car.source, self.cdr.decompose())
    }
}

enum SourcePoll<T, N, D, E> {
    NotReady(N),
    Ready { item: T, next: N, },
    Depleted(D),
    Error(E),
}

trait Source: Sized {
    type Item;
    type Depleted;
    type Error;

    fn source_poll(self) -> SourcePoll<Self::Item, Self, Self::Depleted, Self::Error>;
}

struct Gone;

impl<S> Source for S where S: Stream {
    type Item = S::Item;
    type Depleted = Gone;
    type Error = S::Error;

    fn source_poll(mut self) -> SourcePoll<Self::Item, Self, Self::Depleted, Self::Error> {
        match self.poll() {
            Ok(Async::NotReady) =>
                SourcePoll::NotReady(self),
            Ok(Async::Ready(Some(item))) =>
                SourcePoll::Ready { item, next: self, },
            Ok(Async::Ready(None)) =>
                SourcePoll::Depleted(Gone),
            Err(error) =>
                SourcePoll::Error(error),
        }
    }
}

struct FutureGenerator<F, N> {
    future: F,
    next: N,
}

impl<F, N, T, K> Source for FutureGenerator<F, N> where F: Future<Item = (Option<T>, K)>, N: Fn(K) -> F {
    type Item = T;
    type Depleted = K;
    type Error = F::Error;

    fn source_poll(mut self) -> SourcePoll<Self::Item, Self, Self::Depleted, Self::Error> {
        match self.future.poll() {
            Ok(Async::NotReady) =>
                SourcePoll::NotReady(self),
            Ok(Async::Ready((Some(item), kont))) => {
                let next_future = (self.next)(kont);
                SourcePoll::Ready { item, next: FutureGenerator { future: next_future, next: self.next, }, }
            },
            Ok(Async::Ready((None, kont))) =>
                SourcePoll::Depleted(kont),
            Err(error) =>
                SourcePoll::Error(error),
        }
    }
}

enum OwnedPoll<U, V, S, R> {
    NotReady { me: S, tail: R, },
    Ready { folded_ok: U, me: S, tail: R, },
    Depleted { folded_err: V, },
    Error { folded_err: V, },
}

#[derive(Debug)]
struct DecomposeZip<DL, D, DR> {
    left_dir: DL,
    myself: D,
    right_rev: DR,
}

enum ErrorEvent<DL, D, DR, E> {
    Depleted { decomposed: DecomposeZip<DL, D, DR>, },
    Error { error: E, decomposed: DecomposeZip<DL, Gone, DR>, },
}

trait Probe<U, V, A>: Sized {
    fn probe(self, tail: A) -> OwnedPoll<U, V, Self, A>;
}

impl<U, V, A> Probe<U, V, A> for Nil {
    fn probe(self, tail: A) -> OwnedPoll<U, V, Self, A> {
        OwnedPoll::NotReady { me: self, tail, }
    }
}

impl<U, V, S, FO, FE, L, R, P> Probe<U, V, R> for Cons<FNode<S, FO, FE, L, R>, P>
where P: Probe<U, V, (S, R)> + Decompose<Parts = L>,
      S: Source,
      FO: Fn(S::Item) -> U,
      FE: Fn(ErrorEvent<L, S::Depleted, R, S::Error>) -> V,
{
    fn probe(self, tail: R) -> OwnedPoll<U, V, Self, R> {
        let Cons { car: FNode { source, fold_ok, fold_err, .. }, cdr, } = self;
        match cdr.probe((source, tail)) {
            OwnedPoll::NotReady { me: cdr, tail: (source, tail), } => {
                match source.source_poll() {
                    SourcePoll::NotReady(source) =>
                        OwnedPoll::NotReady {
                            me: Cons {
                                car: FNode { source, fold_ok, fold_err, _zipper: PhantomData, },
                                cdr,
                            },
                            tail,
                        },
                    SourcePoll::Ready { item, next: source, } => {
                        let folded_ok = fold_ok(item);
                        OwnedPoll::Ready {
                            folded_ok,
                            me: Cons { car: FNode { source, fold_ok, fold_err, _zipper: PhantomData, }, cdr, },
                            tail,
                        }
                    },
                    SourcePoll::Depleted(depleted_token) => {
                        let decomposed_left = cdr.decompose();
                        let decomposed_right = tail;
                        let folded_err = fold_err(ErrorEvent::Depleted {
                            decomposed: DecomposeZip {
                                left_dir: decomposed_left,
                                myself: depleted_token,
                                right_rev: decomposed_right,
                            },
                        });
                        OwnedPoll::Depleted { folded_err, }
                    },
                    SourcePoll::Error(error) => {
                        let decomposed_left = cdr.decompose();
                        let decomposed_right = tail;
                        let folded_err = fold_err(ErrorEvent::Error {
                            decomposed: DecomposeZip {
                                left_dir: decomposed_left,
                                myself: Gone,
                                right_rev: decomposed_right,
                            },
                            error,
                        });
                        OwnedPoll::Error { folded_err, }
                    },
                }
            },
            OwnedPoll::Ready { folded_ok, me: cdr, tail: (source, tail), } =>
                OwnedPoll::Ready { folded_ok, me: Cons { car: FNode { source, fold_ok, fold_err, _zipper: PhantomData, }, cdr, }, tail, },
            OwnedPoll::Depleted { folded_err, } =>
                OwnedPoll::Depleted { folded_err, },
            OwnedPoll::Error { folded_err, } =>
                OwnedPoll::Error { folded_err, },
        }
    }
}

impl<U, V, B> Future for Ready<B, U, V> where B: Probe<U, V, ()> {
    type Item = (U, Self);
    type Error = V;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let blender = self.blender.take().expect("cannot poll Ready twice");
        match blender.probe(()) {
            OwnedPoll::NotReady { me, tail: (), } => {
                self.blender = Some(me);
                Ok(Async::NotReady)
            },
            OwnedPoll::Ready { folded_ok, me, tail: (), } => {
                Ok(Async::Ready((
                    folded_ok,
                    Ready {
                        blender: Some(me),
                        _marker: PhantomData,
                    },
                )))
            },
            OwnedPoll::Depleted { folded_err, } =>
                Err(folded_err),
            OwnedPoll::Error { folded_err, } =>
                Err(folded_err),
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
