use futures::{
    Poll,
    Async,
    Stream,
    Future,
};

use std::marker::PhantomData;

#[derive(Debug)]
struct Nil;

#[derive(Debug)]
struct Cons<A, D> {
    car: A,
    cdr: D,
}

struct SourceNode<S, O> {
    source: S,
    _parts: PhantomData<O>,
}

struct Blender<B> {
    chain: B,
}

impl Blender<Nil> {
    fn new() -> Blender<Nil> {
        Blender {
            chain: Nil,
        }
    }

    fn add<S>(self, source: S) -> Blender<Cons<SourceNode<S, (S, ())>, Nil>> where S: Source {
        Blender {
            chain: Cons {
                car: SourceNode {
                    source,
                    _parts: PhantomData,
                },
                cdr: self.chain,
            },
        }
    }
}

impl<R, Q, P> Blender<Cons<SourceNode<Q, P>, R>> {
    fn add<S>(self, source: S) -> Blender<Cons<SourceNode<S, (S, P)>, Cons<SourceNode<Q, P>, R>>> where S: Source {
        Blender {
            chain: Cons {
                car: SourceNode {
                    source,
                    _parts: PhantomData,
                },
                cdr: self.chain,
            },
        }
    }

    fn finish_sources(self) -> FolderInit<Cons<SourceNode<Q, P>, R>> {
        FolderInit {
            unfolded: self.chain,
        }
    }
}

struct FolderInit<N> {
    unfolded: N,
}

fn id<T>(x: T) -> T { x }

impl<S, P, R> FolderInit<Cons<SourceNode<S, (S, P)>, R>> {
    fn fold<FO, FE, T, E, U, V>(self, fold_ok: FO, fold_err: FE) -> Folder<R, Cons<FoldNode<S, FO, FE, (), P>, Nil>, U, V>
    where FO: Fn(T) -> U,
          FE: Fn(E) -> V,
    {
        Folder {
            unfolded: self.unfolded.cdr,
            folded: Cons {
                car: FoldNode {
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

    fn fold_id<U, V>(self) -> Folder<R, Cons<FoldNode<S, fn(U) -> U, fn(V) -> V, (), P>, Nil>, U, V> {
        self.fold(id, id)
    }
}

struct Folder<N, F, U, V> {
    unfolded: N,
    folded: F,
    _marker: PhantomData<(U, V)>,
}

struct FoldNode<S, FO, FE, L, R> {
    source: S,
    fold_ok: FO,
    fold_err: FE,
    _zipper: PhantomData<(L, R)>,
}

impl<S, P, R, F, Q, GO, GE, L, RA, RD, U, V> Folder<Cons<SourceNode<S, (S, P)>, R>, Cons<FoldNode<Q, GO, GE, L, (RA, RD)>, F>, U, V> {
    fn fold<FO, FE, T, E>(
        self,
        fold_ok: FO,
        fold_err: FE,
    )
        -> Folder<R, Cons<FoldNode<S, FO, FE, (Q, L), RD>, Cons<FoldNode<Q, GO, GE, L, (RA, RD)>, F>>, U, V>
    where FO: Fn(T) -> U,
          FE: Fn(E) -> V,
    {
        Folder {
            unfolded: self.unfolded.cdr,
            folded: Cons {
                car: FoldNode {
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

    fn fold_id(self) -> Folder<R, Cons<FoldNode<S, fn(U) -> U, fn(V) -> V, (Q, L), RD>, Cons<FoldNode<Q, GO, GE, L, (RA, RD)>, F>>, U, V> {
        self.fold(id, id)
    }
}

impl<B, U, V> Folder<Nil, B, U, V> {
    fn finish(self) -> BlenderReady<B, U, V> {
        BlenderReady {
            blender: Some(self.folded),
            _marker: PhantomData,
        }
    }
}

struct BlenderReady<B, U, V> {
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

impl<S, FO, FE, L, R, P> Decompose for Cons<FoldNode<S, FO, FE, L, R>, P> where P: Decompose {
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

impl<U, V, S, FO, FE, L, R, P> Probe<U, V, R> for Cons<FoldNode<S, FO, FE, L, R>, P>
where P: Probe<U, V, (S, R)> + Decompose<Parts = L>,
      S: Source,
      FO: Fn(S::Item) -> U,
      FE: Fn(ErrorEvent<L, S::Depleted, R, S::Error>) -> V,
{
    fn probe(self, tail: R) -> OwnedPoll<U, V, Self, R> {
        let Cons { car: FoldNode { source, fold_ok, fold_err, .. }, cdr, } = self;
        match cdr.probe((source, tail)) {
            OwnedPoll::NotReady { me: cdr, tail: (source, tail), } => {
                match source.source_poll() {
                    SourcePoll::NotReady(source) =>
                        OwnedPoll::NotReady {
                            me: Cons {
                                car: FoldNode { source, fold_ok, fold_err, _zipper: PhantomData, },
                                cdr,
                            },
                            tail,
                        },
                    SourcePoll::Ready { item, next: source, } => {
                        let folded_ok = fold_ok(item);
                        OwnedPoll::Ready {
                            folded_ok,
                            me: Cons { car: FoldNode { source, fold_ok, fold_err, _zipper: PhantomData, }, cdr, },
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
                OwnedPoll::Ready { folded_ok, me: Cons { car: FoldNode { source, fold_ok, fold_err, _zipper: PhantomData, }, cdr, }, tail, },
            OwnedPoll::Depleted { folded_err, } =>
                OwnedPoll::Depleted { folded_err, },
            OwnedPoll::Error { folded_err, } =>
                OwnedPoll::Error { folded_err, },
        }
    }
}

impl<U, V, B> Future for BlenderReady<B, U, V> where B: Probe<U, V, ()> {
    type Item = (U, Self);
    type Error = V;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let blender = self.blender.take().expect("cannot poll BlenderReady twice");
        match blender.probe(()) {
            OwnedPoll::NotReady { me, tail: (), } => {
                self.blender = Some(me);
                Ok(Async::NotReady)
            },
            OwnedPoll::Ready { folded_ok, me, tail: (), } => {
                Ok(Async::Ready((
                    folded_ok,
                    BlenderReady {
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
        future::Either,
        stream::{
            self,
            futures_unordered,
        },
    };
    use tokio::timer::Delay;
    use super::{
        Gone,
        Blender,
        ErrorEvent,
        DecomposeZip,
    };

    #[test]
    fn blend_3_stream() {
        let stream_a = stream::iter_ok::<_, ()>(vec![0, 1, 2]);
        let stream_b = stream::iter_ok::<_, ()>(vec![true, false]);
        let stream_c = stream::iter_ok::<_, ()>(vec!["5"]);

        #[derive(PartialEq, Debug)]
        enum Var3<A, B, C> { A(A), B(B), C(C), }

        let blender = Blender::new()
            .add(stream_a)
            .add(stream_b)
            .add(stream_c)
            .finish_sources()
            .fold(Var3::C, Var3::C)
            .fold(Var3::B, Var3::B)
            .fold(Var3::A, Var3::A)
            .finish();
        let blender = match blender.wait() {
            Ok((Var3::C("5"), blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        let (stream_a, stream_b) = match blender.wait() {
            Err(Var3::C(ErrorEvent::Depleted { decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (stream_b, (stream_a, ())), }, })) =>
                (stream_a, stream_b),
            _other => panic!("unexpected wait result"),
        };
        let blender = Blender::new()
            .add(stream_a)
            .add(stream_b)
            .finish_sources()
            .fold(Either::B, Either::B)
            .fold(Either::A, Either::A)
            .finish();
        let blender = match blender.wait() {
            Ok((Either::B(true), blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        let blender = match blender.wait() {
            Ok((Either::B(false), blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        let stream_a = match blender.wait() {
            Err(Either::B(ErrorEvent::Depleted { decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (stream_a, ()), }, })) =>
                stream_a,
            _other => panic!("unexpected wait result"),
        };
        let blender = Blender::new()
            .add(stream_a)
            .finish_sources()
            .fold_id()
            .finish();
        let blender = match blender.wait() {
            Ok((0, blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        let blender = match blender.wait() {
            Ok((1, blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        let blender = match blender.wait() {
            Ok((2, blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        match blender.wait() {
            Err(ErrorEvent::Depleted { decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (), }, }) => (),
            _other => panic!("unexpected wait result"),
        }
    }

    fn blend_3_mix() {
        let stream_a = stream::iter_ok::<_, ()>(vec![0u8, 1, 2]);
        let stream_b = stream::iter_ok::<_, ()>(vec![true, false]);
        let stream_c = stream::iter_ok::<_, ()>(vec!["5"]);

        // let blender_a = Blender::new()
        //     .add(stream_a)
        //     .finish_sources()
        //     .fold_id()
        //     .finish();

        // #[derive(PartialEq, Debug)]
        // enum Var3<A, B, C> { A(A), B(B), C(C), }

        // let blender = Blender::new()
        //     .add(stream_a)
        //     .add(stream_b)
        //     .add(stream_c)
        //     .finish_sources()
        //     .fold(Var3::C, Var3::C)
        //     .fold(Var3::B, Var3::B)
        //     .fold(Var3::A, Var3::A)
        //     .finish();
        // let blender = match blender.wait() {
        //     Ok((Var3::C("5"), blender)) => blender,
        //     _other => panic!("unexpected wait result"),
        // };
        // let (stream_a, stream_b) = match blender.wait() {
        //     Err(Var3::C(ErrorEvent::Depleted { decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (stream_b, (stream_a, ())), }, })) =>
        //         (stream_a, stream_b),
        //     _other => panic!("unexpected wait result"),
        // };
        // let blender = Blender::new()
        //     .add(stream_a)
        //     .add(stream_b)
        //     .finish_sources()
        //     .fold(Either::B, Either::B)
        //     .fold(Either::A, Either::A)
        //     .finish();
        // let blender = match blender.wait() {
        //     Ok((Either::B(true), blender)) => blender,
        //     _other => panic!("unexpected wait result"),
        // };
        // let blender = match blender.wait() {
        //     Ok((Either::B(false), blender)) => blender,
        //     _other => panic!("unexpected wait result"),
        // };
        // let stream_a = match blender.wait() {
        //     Err(Either::B(ErrorEvent::Depleted { decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (stream_a, ()), }, })) =>
        //         stream_a,
        //     _other => panic!("unexpected wait result"),
        // };
        // let blender = Blender::new()
        //     .add(stream_a)
        //     .finish_sources()
        //     .fold(|x| x, |x| x)
        //     .finish();
        // let blender = match blender.wait() {
        //     Ok((0, blender)) => blender,
        //     _other => panic!("unexpected wait result"),
        // };
        // let blender = match blender.wait() {
        //     Ok((1, blender)) => blender,
        //     _other => panic!("unexpected wait result"),
        // };
        // let blender = match blender.wait() {
        //     Ok((2, blender)) => blender,
        //     _other => panic!("unexpected wait result"),
        // };
        // match blender.wait() {
        //     Err(ErrorEvent::Depleted { decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (), }, }) => (),
        //     _other => panic!("unexpected wait result"),
        // }
    }

    #[test]
    fn blend_delays() {
        let stream_a = futures_unordered(vec![
            Either::A(Delay::new(Instant::now() + Duration::from_millis(200)).map(|_| 0)),
            Either::B(Delay::new(Instant::now() + Duration::from_millis(100)).map(|_| 1)),
        ]);
        let stream_b = futures_unordered(vec![
            Either::A(Delay::new(Instant::now() + Duration::from_millis(300)).map(|_| "a")),
            Either::B(Delay::new(Instant::now() + Duration::from_millis(50)).map(|_| "b")),
        ]);

        let mut runtime = tokio::runtime::Runtime::new().unwrap();

        let blender = Blender::new()
            .add(stream_a)
            .add(stream_b)
            .finish_sources()
            .fold(Either::B, Either::B)
            .fold(Either::A, Either::A)
            .finish();
        let blender = match runtime.block_on(blender) {
            Ok((Either::B("b"), blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        let blender = match runtime.block_on(blender) {
            Ok((Either::A(1), blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        let blender = match runtime.block_on(blender) {
            Ok((Either::A(0), blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        let stream_b = match runtime.block_on(blender) {
            Err(Either::A(ErrorEvent::Depleted { decomposed: DecomposeZip { left_dir: (stream_b, ()), myself: Gone, right_rev: (), }, })) =>
                stream_b,
            _other => panic!("unexpected wait result"),
        };
        let blender = Blender::new()
            .add(stream_b)
            .finish_sources()
            .fold_id()
            .finish();
        let blender = match runtime.block_on(blender) {
            Ok(("a", blender)) => blender,
            _other => panic!("unexpected wait result"),
        };
        match runtime.block_on(blender) {
            Err(ErrorEvent::Depleted { decomposed: DecomposeZip { left_dir: (), myself: Gone, right_rev: (), }, }) => (),
            _other => panic!("unexpected wait result"),
        }
    }
}
