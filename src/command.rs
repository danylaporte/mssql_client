use crate::{FromRow, Params, Row};
use failure::Error;
use futures::Future;
use std::borrow::Cow;

pub trait Command {
    /// Execute an sql command that does not returns rows.
    ///
    /// # Example
    /// ```
    /// use futures::Future;
    /// use mssql_client::{Connection, Command};
    /// use tokio::executor::current_thread::block_on_all;
    ///
    /// let f = Connection::from_env("MSSQL_DB");
    /// let f = f.and_then(|conn| Command::execute(conn, "DECLARE @i INT = @p1", 10));
    /// let _ = block_on_all(f).unwrap();
    /// ```
    fn execute<'a, S, P>(self, sql: S, params: P) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        P: Params<'a>,
        S: Into<Cow<'static, str>>;

    /// Query the database and reads all rows.
    ///
    /// # Example
    /// ```
    /// use futures::Future;
    /// use mssql_client::{Connection, Command};
    /// use tokio::executor::current_thread::block_on_all;
    ///
    /// let f = Connection::from_env("MSSQL_DB");
    /// let f = f.and_then(|conn| Command::query(conn, "SELECT @p1 + 2", 10));
    /// let (_, rows) = block_on_all(f).unwrap();
    ///
    /// assert_eq!(12, rows[0]);
    /// ```
    fn query<'a, T, S, P>(
        self,
        sql: S,
        params: P,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        S: Into<Cow<'static, str>>,
        T: FromRow + 'static,
        P: Params<'a>,
        Self: Sized,
    {
        self.query_fold(sql, params, Vec::new(), |mut vec, r| {
            vec.push(T::from_row(r)?);
            Ok(vec)
        })
    }

    /// Query the database and reads all rows using a function to transform them.
    ///
    /// # Example
    /// ```
    /// use futures::Future;
    /// use mssql_client::{Connection, Command};
    /// use tokio::executor::current_thread::block_on_all;
    ///
    /// let f = Connection::from_env("MSSQL_DB");
    ///
    /// let f = f.and_then(|conn| Command::query_map(
    ///     conn,
    ///     "SELECT @p1 + 2",
    ///     10,
    ///     |row| row.get(0),
    /// ));
    ///
    /// let (_, rows) = block_on_all(f).unwrap();
    ///
    /// assert_eq!(12, rows[0]);
    /// ```
    fn query_map<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        mut func: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        F: FnMut(&Row) -> Result<T, Error> + 'static,
        S: Into<Cow<'static, str>>,
        P: Params<'a>,
        T: 'static,
        Self: Sized,
    {
        self.query_fold(sql, params, Vec::new(), move |mut vec, r| {
            vec.push(func(r)?);
            Ok(vec)
        })
    }

    fn query_fold<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        init: T,
        func: F,
    ) -> Box<dyn Future<Item = (Self, T), Error = Error>>
    where
        F: FnMut(T, &Row) -> Result<T, Error> + 'static,
        P: Params<'a>,
        S: Into<Cow<'static, str>>,
        Self: Sized,
        T: 'static;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;
    use tokio::executor::current_thread::block_on_all;
    use uuid::Uuid;

    #[test]
    fn execute_params() {
        fn exec<'a, C, S, P>(
            c: C,
            sql: S,
            params: P,
        ) -> Box<dyn Future<Item = C, Error = Error> + 'a>
        where
            C: Command + 'a,
            S: Into<Cow<'static, str>>,
            P: Params<'a>,
        {
            c.execute(sql, params)
        }

        let s = "DECLARE @a UNIQUEIDENTIFIER = @p1".to_owned();
        let id = &Uuid::nil();

        block_on_all(Connection::from_env("MSSQL_DB").and_then(|conn| exec(conn, s, id))).unwrap();
    }
}
