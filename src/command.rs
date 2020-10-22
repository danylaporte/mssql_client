use crate::{FromRow, Params, Result, Row};
use futures03::future::LocalBoxFuture;
use std::{borrow::Cow, fmt::Debug};

pub trait Command {
    /// Execute an sql command that does not returns rows.
    ///
    /// # Example
    /// ```
    /// use futures::Future;
    /// use mssql_client::{Connection, Command, Result};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let conn = Connection::from_env("MSSQL_DB").await?;
    ///     Command::execute(conn, "DECLARE @i INT = @p1", 10).await?;
    ///     Ok(())
    /// }
    /// ```
    fn execute<'a, S, P>(self, sql: S, params: P) -> LocalBoxFuture<'a, Result<Self>>
    where
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
        Self: Sized;

    /// Query the database and reads all rows.
    ///
    /// # Example
    /// ```
    /// use mssql_client::{Connection, Command, Result};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let conn = Connection::from_env("MSSQL_DB").await?;
    ///     let (_, rows) = Command::query(conn, "SELECT @p1 + 2", 10).await?;
    ///
    ///     assert_eq!(12, rows[0]);
    ///     Ok(())
    /// }
    /// ```
    fn query<'a, T, S, P>(self, sql: S, params: P) -> LocalBoxFuture<'a, Result<(Self, Vec<T>)>>
    where
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
        Self: Sized,
        T: FromRow + 'a,
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
    /// use mssql_client::{Connection, Command, Result};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let conn = Connection::from_env("MSSQL_DB").await?;
    ///
    ///     let (_, rows) = Command::query_map(
    ///         conn,
    ///         "SELECT @p1 + 2",
    ///         10,
    ///         |row| row.get(0),
    ///     ).await?;
    ///
    ///     assert_eq!(12, rows[0]);
    ///     Ok(())
    /// }
    /// ```
    fn query_map<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        mut func: F,
    ) -> LocalBoxFuture<'a, Result<(Self, Vec<T>)>>
    where
        F: FnMut(&Row) -> Result<T> + 'a,
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
        Self: Sized,
        T: 'a,
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
    ) -> LocalBoxFuture<'a, Result<(Self, T)>>
    where
        F: FnMut(T, &Row) -> Result<T> + 'a,
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
        Self: Sized,
        T: 'a;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Connection;
    use uuid::Uuid;

    #[tokio::test]
    async fn execute_params() -> Result<()> {
        fn exec<'a, C, S, P>(c: C, sql: S, params: P) -> LocalBoxFuture<'a, Result<C>>
        where
            C: Command + 'a,
            S: Debug + Into<Cow<'static, str>> + 'a,
            P: Debug + Params<'a> + 'a,
        {
            c.execute(sql, params)
        }

        let s = "DECLARE @a UNIQUEIDENTIFIER = @p1".to_owned();
        let id = &Uuid::nil();

        let conn = Connection::from_env("MSSQL_DB").await?;
        exec(conn, s, id).await?;

        Ok(())
    }
}
