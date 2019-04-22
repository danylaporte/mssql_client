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
    /// let f = f.and_then(|conn| Command::execute(conn, "DECLARE @i INT = NULL"));
    /// let _ = block_on_all(f).unwrap();
    /// ```
    fn execute<S>(self, sql: S) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        S: Into<Cow<'static, str>>;

    /// Execute an sql command that does not returns rows.
    ///
    /// # Example
    /// ```
    /// use futures::Future;
    /// use mssql_client::{Connection, Command};
    /// use tokio::executor::current_thread::block_on_all;
    ///
    /// let f = Connection::from_env("MSSQL_DB");
    /// let f = f.and_then(|conn| Command::execute_params(conn, "DECLARE @i INT = @p1", 10));
    /// let _ = block_on_all(f).unwrap();
    /// ```
    fn execute_params<'a, S, P>(
        self,
        sql: S,
        params: P,
    ) -> Box<dyn Future<Item = Self, Error = Error>>
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
    /// let f = f.and_then(|conn| Command::query(conn, "SELECT CAST(10 as INT)"));
    /// let (_, rows) = block_on_all(f).unwrap();
    ///
    /// assert_eq!(10, rows[0]);
    /// ```
    fn query<T, S>(self, sql: S) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        S: Into<Cow<'static, str>>,
        T: FromRow + 'static,
        Self: Sized;

    /// Query the database and reads all rows.
    ///
    /// # Example
    /// ```
    /// use futures::Future;
    /// use mssql_client::{Connection, Command};
    /// use tokio::executor::current_thread::block_on_all;
    ///
    /// let f = Connection::from_env("MSSQL_DB");
    /// let f = f.and_then(|conn| Command::query_params(conn, "SELECT @p1 + 2", 10));
    /// let (_, rows) = block_on_all(f).unwrap();
    ///
    /// assert_eq!(12, rows[0]);
    /// ```
    fn query_params<'a, T, S, P>(
        self,
        sql: S,
        params: P,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        P: Params<'a>,
        S: Into<Cow<'static, str>>,
        T: FromRow + 'static,
        Self: Sized;

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
    /// let f = f.and_then(|conn| Command::query_params_with(
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
    fn query_params_with<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        func: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        F: FnMut(&Row) -> Result<T, Error> + 'static,
        S: Into<Cow<'static, str>>,
        P: Params<'a> + 'a,
        Self: Sized;

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
    /// let f = f.and_then(|conn| Command::query_with(
    ///     conn,
    ///     "SELECT CAST(10 as INT)",
    ///     |row| Ok(row.get::<i32>(0)? + 2),
    /// ));
    ///
    /// let (_, rows) = block_on_all(f).unwrap();
    ///
    /// assert_eq!(12, rows[0]);
    /// ```
    fn query_with<T, S, F>(
        self,
        sql: S,
        func: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        F: FnMut(&Row) -> Result<T, Error> + 'static,
        S: Into<Cow<'static, str>>,
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;
    use tokio::executor::current_thread::block_on_all;
    use uuid::Uuid;

    #[test]
    fn execute_params() {
        fn exec<'a, C, S, P>(c: C, sql: S, params: P) -> Box<dyn Future<Item = C, Error = Error>>
        where
            C: Command,
            S: Into<Cow<'static, str>> + 'static,
            P: Params<'a>,
        {
            c.execute_params(sql, params)
        }

        let s = "DECLARE @a UNIQUEIDENTIFIER = @p1".to_owned();
        let id = &Uuid::nil();

        block_on_all(Connection::from_env("MSSQL_DB").and_then(|conn| exec(conn, s, id))).unwrap();
    }
}
