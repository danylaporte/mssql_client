use futures::{Async, Poll};
use futures_state_stream::{StateStream, StreamEvent};

pub(crate) trait StateStreamExt: StateStream {
    fn map_result_exhaust<F, U, E>(self, f: F) -> MapResultExhaust<Self, F, Self::Error>
    where
        F: FnMut(Self::Item) -> Result<U, E>,
        E: Into<Self::Error>,
        Self: Sized;
}

impl<T> StateStreamExt for T
where
    T: StateStream,
{
    fn map_result_exhaust<F, U, E>(self, f: F) -> MapResultExhaust<Self, F, Self::Error>
    where
        F: FnMut(Self::Item) -> Result<U, E>,
        E: Into<Self::Error>,
        Self: Sized,
    {
        MapResultExhaust {
            stream: self,
            f,
            err: None,
        }
    }
}

pub(crate) struct MapResultExhaust<S, F, E> {
    err: Option<E>,
    f: F,
    stream: S,
}

impl<S, F, E, U> StateStream for MapResultExhaust<S, F, S::Error>
where
    S: StateStream,
    F: FnMut(S::Item) -> Result<U, E>,
    E: Into<S::Error>,
{
    type Item = U;
    type State = S::State;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<StreamEvent<Self::Item, S::State>, S::Error> {
        loop {
            return match self.stream.poll()? {
                Async::Ready(StreamEvent::Next(i)) => {
                    if self.err.is_none() {
                        match (self.f)(i) {
                            Ok(v) => return Ok(Async::Ready(StreamEvent::Next(v))),
                            Err(e) => self.err = Some(e.into()),
                        }
                    }
                    continue;
                }
                Async::Ready(StreamEvent::Done(s)) => match self.err.take() {
                    Some(e) => Err(e),
                    None => Ok(Async::Ready(StreamEvent::Done(s))),
                },
                Async::NotReady => Ok(Async::NotReady),
            };
        }
    }
}
