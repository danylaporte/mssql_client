use crate::{
    utils::{adjust_conn_str, params_to_vec, reduce},
    Command, FromRow, Params, Result, Row, Transaction,
};
use futures03::{compat::Future01CompatExt, future::LocalBoxFuture};
use futures_state_stream::StateStream;
use std::{borrow::Cow, env::var, ffi::OsStr, fmt::Debug};
use tiberius::{query::QueryRow, BoxableIo, SqlConnection};
use tracing::instrument;

/// A database connection.
///
/// When created, a connection is not immediately made to the database.
/// It requires to issue a command or to explicitly call the connect fn.
///
/// # Example
/// ```
/// use mssql_client::{Connection, Result};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let conn_str = "server=tcp:localhost\\SQL2017;database=master;integratedsecurity=sspi;trustservercertificate=true";
///     let connection = Connection::connect(conn_str).await?;
///     Ok(())
/// }
/// ```
pub struct Connection(pub(super) SqlConnection<Box<dyn BoxableIo>>);

impl Command for Connection {
    fn execute<'a, S, P>(self, sql: S, params: P) -> LocalBoxFuture<'a, Result<Self>>
    where
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
    {
        self.execute(sql, params)
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
        T: 'a,
        Self: Sized,
    {
        self.query_fold(sql, params, init, func)
    }
}

impl Connection {
    /// Creates a connection future that will resolve to a Connection once connected.
    /// # Example
    /// ```
    /// use mssql_client::{Connection, Result};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let conn_str = "server=tcp:localhost\\SQL2017;database=master;integratedsecurity=sspi;trustservercertificate=true";
    ///     let connection = Connection::connect(conn_str).await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn connect<'a, S>(conn_str: S) -> LocalBoxFuture<'a, Result<Self>>
    where
        S: Debug + Into<String> + 'a,
    {
        Box::pin(Self::connect_imp(conn_str))
    }

    #[instrument(level = "debug", name = "Connection::connect", err)]
    async fn connect_imp<S>(conn_str: S) -> Result<Self>
    where
        S: Debug + Into<String>,
    {
        let conn_str = adjust_conn_str(&conn_str.into())?;
        let c = SqlConnection::connect(&conn_str).compat().await?;
        Ok(Connection(c))
    }

    /// Creates a connection that will connect to the database specified in the environment variable.
    ///
    /// An error is returned if the environment variable could not be read.
    ///
    /// # Example
    /// ```
    /// use mssql_client::{Connection, Result};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let connection = Connection::from_env("MSSQL_DB").await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn from_env<K>(key: K) -> Result<Self>
    where
        K: AsRef<OsStr>,
    {
        let key = key.as_ref();

        let conn_str = var(key)?;
        Ok(Connection::connect(conn_str).await?)
    }

    /// Execute sql statements that don't return rows.
    ///
    /// # Example
    /// ```
    /// use mssql_client::{Connection, Result};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let connection = Connection::from_env("MSSQL_DB").await?;
    ///     let connection = connection.execute("DECLARE @a INT = 0", ()).await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn execute<'a, S, P>(self, sql: S, params: P) -> LocalBoxFuture<'a, Result<Self>>
    where
        S: Debug + Into<Cow<'static, str>> + 'a,
        P: Debug + Params<'a> + 'a,
    {
        Box::pin(self.execute_imp(sql, params))
    }

    #[instrument(level = "debug", name = "Connection::execute", skip(self), err)]
    async fn execute_imp<'a, S, P>(self, sql: S, params: P) -> Result<Self>
    where
        S: Debug + Into<Cow<'static, str>> + 'a,
        P: Debug + Params<'a> + 'a,
    {
        let mut p = Vec::new();
        params.params(&mut p);

        let sql = sql.into();

        let (_affected_rows, conn) = if p.is_empty() {
            self.0.simple_exec(sql).compat().await
        } else {
            let params = params_to_vec(&p);
            self.0.exec(sql, &params).compat().await
        }?;

        Ok(Self(conn))
    }

    /// Execute sql query and returns all the rows.
    ///
    /// # Example
    /// ```
    /// #[macro_use]
    /// use mssql_client::{Connection, Result};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let (connection, rows): (_, Vec<i32>) = Connection::from_env("MSSQL_DB")
    ///         .await?
    ///         .query("SELECT 1", ())
    ///         .await?;
    ///
    ///     assert_eq!(rows[0], 1);
    ///     Ok(())
    /// }
    /// ```
    pub async fn query<'a, T, S, P>(self, sql: S, params: P) -> Result<(Self, Vec<T>)>
    where
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
        T: FromRow + 'a,
    {
        self.query_map(sql, params, FromRow::from_row).await
    }

    pub fn query_fold<'a, T, S, P, F>(
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
        T: 'a,
    {
        Box::pin(self.query_fold_imp(sql, params, init, func))
    }

    #[instrument(
        level = "debug",
        name = "Connection::query_fold",
        skip(self, init, func),
        err
    )]
    pub async fn query_fold_imp<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        init: T,
        mut func: F,
    ) -> Result<(Self, T)>
    where
        F: FnMut(T, &Row) -> Result<T>,
        P: Debug + Params<'a>,
        S: Debug + Into<Cow<'static, str>>,
    {
        let mut p = Vec::new();
        params.params(&mut p);

        let sql = sql.into();
        let next = move |r, row| func(r, &Row(row));

        let stream: Box<
            dyn StateStream<
                Item = QueryRow,
                State = SqlConnection<Box<dyn BoxableIo>>,
                Error = tiberius::Error,
            >,
        > = if p.is_empty() {
            Box::new(self.0.simple_query(sql))
        } else {
            Box::new(self.0.query(sql, &params_to_vec(&p)))
        };

        let (conn, rows) = reduce(stream, init, next).await?;

        Ok((Self(conn), rows))
    }

    pub fn query_map<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        mut func: F,
    ) -> LocalBoxFuture<'a, Result<(Self, Vec<T>)>>
    where
        F: FnMut(&Row) -> Result<T> + 'a,
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
        T: 'a,
    {
        self.query_fold(sql, params, Vec::new(), move |mut vec, row| {
            vec.push(func(row)?);
            Ok(vec)
        })
    }

    pub fn transaction(self) -> LocalBoxFuture<'static, Result<Transaction>> {
        Box::pin(self.transaction_imp())
    }

    #[instrument(level = "debug", name = "Connection::transaction", skip(self), err)]
    async fn transaction_imp(self) -> Result<Transaction> {
        use futures::future::Future;

        let (_, t) = self
            .0
            .transaction()
            .and_then(|t| t.simple_exec("set implicit_transactions off"))
            .and_then(|(_, t)| t.simple_exec("BEGIN TRANSACTION"))
            .compat()
            .await?;

        Ok(Transaction(t))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect() -> Result<()> {
        Connection::from_env("MSSQL_DB").await?;
        Ok(())
    }

    #[tokio::test]
    async fn execute() -> Result<()> {
        Connection::from_env("MSSQL_DB")
            .await?
            .execute("DECLARE @a INT = 0", ())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn execute_params() -> Result<()> {
        Connection::from_env("MSSQL_DB")
            .await?
            .execute("DECLARE @a INT = @p1", 10)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn query() -> Result<()> {
        let (_connection, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .query("SELECT 2", ())
            .await?;

        assert_eq!(2, rows[0]);
        Ok(())
    }

    #[tokio::test]
    async fn query_params() -> Result<()> {
        let (_connection, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .query::<(String, i32), _, _>("SELECT @P1, @P2", ("Foo", 3))
            .await?;

        assert_eq!("Foo", &rows[0].0);
        assert_eq!(3, rows[0].1);
        Ok(())
    }

    #[tokio::test]
    async fn query_params_nulls() -> Result<()> {
        use uuid::Uuid;
        let sql = r#"
            DECLARE @V1 NVARCHAR(100) = @p1;
            DECLARE @V2 INT = @p2;
            DECLARE @V3 UNIQUEIDENTIFIER = @p3;

            SELECT @V1, @V2, @V3
        "#;

        let (_connection, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .query::<(Option<String>, Option<i32>, Option<Uuid>), _, _>(
                sql,
                (None::<&str>, None::<i32>, None::<Uuid>),
            )
            .await?;

        assert_eq!(None, rows[0].0);
        assert_eq!(None, rows[0].1);
        assert_eq!(None, rows[0].2);
        Ok(())
    }

    #[tokio::test]
    async fn query_decimal() -> Result<()> {
        let (_connection, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .query("SELECT CAST(15337032 as DECIMAL(28, 12))", ())
            .await?;

        assert_eq!(decimal::Decimal::from(15337032), rows[0]);
        Ok(())
    }

    #[tokio::test]
    async fn query_f64() -> Result<()> {
        let (_connection, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .query("SELECT CAST(15337032 as DECIMAL(28, 12))", ())
            .await?;

        assert_eq!(15337032f64, rows[0]);
        Ok(())
    }
}
