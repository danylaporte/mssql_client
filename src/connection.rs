use crate::{
    utils::{adjust_conn_str, reduce},
    Command, FromRow, Params, Row, Transaction,
};
use failure::{format_err, Error, ResultExt};
use futures03::{compat::Future01CompatExt, future::LocalBoxFuture};
use log::{debug, error, trace};
use std::{borrow::Cow, env::var, ffi::OsStr, time::Instant};
use tiberius::{BoxableIo, SqlConnection};

/// A database connection.
///
/// When created, a connection is not immediately made to the database.
/// It requires to issue a command or to explicitly call the connect fn.
///
/// # Example
/// ```
/// use mssql_client::Connection;
///
/// #[tokio::main]
/// async fn main() -> Result<(), failure::Error> {
///     let conn_str = "server=tcp:localhost\\SQL2017;database=master;integratedsecurity=sspi;trustservercertificate=true";
///     let connection = Connection::connect(conn_str).await?;
///     Ok(())
/// }
/// ```
pub struct Connection(pub(super) SqlConnection<Box<dyn BoxableIo>>);

impl Command for Connection {
    fn execute<'a, S, P>(self, sql: S, params: P) -> LocalBoxFuture<'a, Result<Self, Error>>
    where
        P: Params<'a> + 'a,
        S: Into<Cow<'static, str>> + 'a,
    {
        self.execute(sql, params)
    }

    fn query_fold<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        init: T,
        func: F,
    ) -> LocalBoxFuture<'a, Result<(Self, T), Error>>
    where
        F: FnMut(T, &Row) -> Result<T, Error> + 'a,
        P: Params<'a> + 'a,
        S: Into<Cow<'static, str>> + 'a,
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
    /// use mssql_client::Connection;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), failure::Error> {
    ///     let conn_str = "server=tcp:localhost\\SQL2017;database=master;integratedsecurity=sspi;trustservercertificate=true";
    ///     let connection = Connection::connect(conn_str).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn connect<S>(conn_str: S) -> Result<Self, Error>
    where
        S: Into<String>,
    {
        let conn_str = adjust_conn_str(&conn_str.into())?;

        trace!("Connecting to db...");

        let start = Instant::now();

        let c = SqlConnection::connect(&conn_str)
            .compat()
            .await
            .map_err(|e| {
                let e = format_err!("Failed connecting to db. {:?}", e);
                error!("{}", e);
                debug!("Failed connection string: {}", conn_str);
                e
            })?;

        trace!(
            "Connected to db in {}ms.",
            (Instant::now() - start).as_millis(),
        );

        Ok(Connection(c))
    }

    /// Creates a connection that will connect to the database specified in the environment variable.
    ///
    /// An error is returned if the environment variable could not be read.
    ///
    /// # Example
    /// ```
    /// use mssql_client::Connection;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), failure::Error> {
    ///     let connection = Connection::from_env("MSSQL_DB").await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn from_env<K>(key: K) -> Result<Self, Error>
    where
        K: AsRef<OsStr>,
    {
        let key = key.as_ref();

        let conn_str = var(key)
            .map_err(|e| format_err!("Connection from env variable {:#?} failed. {}", key, e))?;

        Ok(Connection::connect(conn_str).await?)
    }

    /// Execute sql statements that don't return rows.
    ///
    /// # Example
    /// ```
    /// use mssql_client::Connection;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), failure::Error> {
    ///     let connection = Connection::from_env("MSSQL_DB").await?;
    ///     let connection = connection.execute("DECLARE @a INT = 0", ()).await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn execute<'a, S, P>(self, sql: S, params: P) -> LocalBoxFuture<'a, Result<Self, Error>>
    where
        S: Into<Cow<'static, str>> + 'a,
        P: Params<'a> + 'a,
    {
        Box::pin(async {
            let mut p = Vec::new();
            params.params(&mut p);

            let sql = sql.into();
            let log_sql = format!("{:?}\nParams: {:#?}", sql, p);
            let start = Instant::now();

            trace!("Executing {}", log_sql);

            let (_affected_rows, conn) = if p.is_empty() {
                self.0.simple_exec(sql).compat().await
            } else {
                let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();
                self.0.exec(sql, &params).compat().await
            }
            .map_err(move |e| {
                debug!("Execute failed {}.", log_sql);
                format_err!("Execute failed. {:?}", e)
            })?;

            debug!(
                "Execute executed in {}ms.",
                (Instant::now() - start).as_millis(),
            );

            Ok(Self(conn))
        })
    }

    /// Execute sql query and returns all the rows.
    ///
    /// # Example
    /// ```
    /// #[macro_use]
    /// use mssql_client::Connection;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), failure::Error> {
    ///     let (connection, rows): (_, Vec<i32>) = Connection::from_env("MSSQL_DB")
    ///         .await?
    ///         .query("SELECT 1", ())
    ///         .await?;
    ///
    ///     assert_eq!(rows[0], 1);
    ///     Ok(())
    /// }
    /// ```
    pub fn query<'a, T, S, P>(
        self,
        sql: S,
        params: P,
    ) -> LocalBoxFuture<'a, Result<(Self, Vec<T>), Error>>
    where
        P: Params<'a> + 'a,
        S: Into<Cow<'static, str>> + 'a,
        T: FromRow + 'a,
    {
        self.query_map(sql, params, FromRow::from_row)
    }

    pub fn query_fold<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        init: T,
        mut func: F,
    ) -> LocalBoxFuture<'a, Result<(Self, T), Error>>
    where
        F: FnMut(T, &Row) -> Result<T, Error> + 'a,
        P: Params<'a> + 'a,
        S: Into<Cow<'static, str>> + 'a,
        T: 'a,
    {
        Box::pin(async {
            let mut p = Vec::new();
            params.params(&mut p);

            let sql = sql.into();
            let log_sql = format!("{:?}\nParams: {:#?}", sql, p);
            let start = Instant::now();

            trace!("Querying {}", log_sql);

            let next = move |r, row| Ok(func(r, &Row(row)).context("Row conversion failed")?);

            let (conn, rows) = if p.is_empty() {
                let stream = self.0.simple_query(sql);
                reduce(stream, init, next, log_sql).await?
            } else {
                let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();
                let stream = self.0.query(sql, &params);
                reduce(stream, init, next, log_sql).await?
            };

            trace!(
                "Query executed in {}ms.",
                (Instant::now() - start).as_millis(),
            );

            Ok((Self(conn), rows))
        })
    }

    pub fn query_map<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        mut func: F,
    ) -> LocalBoxFuture<'a, Result<(Self, Vec<T>), Error>>
    where
        F: FnMut(&Row) -> Result<T, Error> + 'a,
        P: Params<'a> + 'a,
        S: Into<Cow<'static, str>> + 'a,
        T: 'a,
    {
        self.query_fold(sql, params, Vec::new(), move |mut vec, row| {
            vec.push(func(row)?);
            Ok(vec)
        })
    }

    pub async fn transaction(self) -> Result<Transaction, Error> {
        trace!("starting transaction...");
        let start = Instant::now();

        use futures::future::Future;

        let (_, t) = self
            .0
            .transaction()
            .and_then(|t| t.simple_exec("BEGIN TRANSACTION"))
            .compat()
            .await
            .map_err(|e| {
                let e = format_err!("Start transaction failed. {:?}", e);
                error!("{}", e);
                e
            })?;

        trace!(
            "transaction started in {}ms.",
            (Instant::now() - start).as_millis(),
        );

        Ok(Transaction(t))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect() -> Result<(), Error> {
        Connection::from_env("MSSQL_DB").await?;
        Ok(())
    }

    #[tokio::test]
    async fn execute() -> Result<(), Error> {
        Connection::from_env("MSSQL_DB")
            .await?
            .execute("DECLARE @a INT = 0", ())
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn execute_params() -> Result<(), Error> {
        Connection::from_env("MSSQL_DB")
            .await?
            .execute("DECLARE @a INT = @p1", 10)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn query() -> Result<(), Error> {
        let (_connection, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .query("SELECT 2", ())
            .await?;

        assert_eq!(2, rows[0]);
        Ok(())
    }

    #[tokio::test]
    async fn query_params() -> Result<(), Error> {
        let (_connection, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .query::<(String, i32), _, _>("SELECT @P1, @P2", ("Foo", 3))
            .await?;

        assert_eq!("Foo", &rows[0].0);
        assert_eq!(3, rows[0].1);
        Ok(())
    }

    #[tokio::test]
    async fn query_params_nulls() -> Result<(), Error> {
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
    async fn query_decimal() -> Result<(), Error> {
        let (_connection, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .query("SELECT CAST(15337032 as DECIMAL(28, 12))", ())
            .await?;

        assert_eq!(decimal::Decimal::from(15337032), rows[0]);
        Ok(())
    }

    #[tokio::test]
    async fn query_f64() -> Result<(), Error> {
        let (_connection, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .query("SELECT CAST(15337032 as DECIMAL(28, 12))", ())
            .await?;

        assert_eq!(15337032f64, rows[0]);
        Ok(())
    }
}
