use crate::{FromRow, Query, Row};
use failure::Error;
use futures::Future;

pub trait Command {
    fn execute<'a, Q>(self, query: Q) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        Q: Into<Query<'a>>,
        Self: Sized;

    fn query_with<'a, Q, F: 'static, T: 'static>(
        self,
        query: Q,
        f: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        Q: Into<Query<'a>>,
        F: FnMut(&Row) -> Result<T, Error>,
        Self: Sized;

    fn query<'a, Q, T: 'static>(
        self,
        query: Q,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        Q: Into<Query<'a>>,
        T: FromRow,
        Self: Sized,
    {
        self.query_with(query, FromRow::from_row)
    }
}
