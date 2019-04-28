use crate::{utils, Command, FromRow, Params, Row, StateStreamExt, Transaction};
use failure::{format_err, Error, ResultExt};
use futures::future::{err, Either, Future};
use futures_state_stream::StateStream;
use log::{debug, error};
use std::borrow::Cow;
use std::ffi::OsStr;
use std::time::Instant;
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
/// let conn_str = "server=tcp:localhost\\SQL2017;database=Database1;integratedsecurity=sspi;trustservercertificate=true";
/// let connection = Connection::connect(conn_str);
/// ```
pub struct Connection(pub(super) SqlConnection<Box<BoxableIo>>);

impl Command for Connection {
    fn execute<S>(self, sql: S) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        S: Into<Cow<'static, str>>,
    {
        self.execute(sql)
    }

    fn execute_params<'a, S, P>(
        self,
        sql: S,
        params: P,
    ) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        P: Params<'a>,
        S: Into<Cow<'static, str>>,
    {
        self.execute_params(sql, params)
    }

    fn query<T, S>(self, sql: S) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        S: Into<Cow<'static, str>>,
        T: FromRow + 'static,
        Self: Sized,
    {
        self.query(sql)
    }

    fn query_params<'a, T, S, P>(
        self,
        sql: S,
        params: P,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        P: Params<'a>,
        S: Into<Cow<'static, str>>,
        T: FromRow + 'static,
        Self: Sized,
    {
        self.query_params(sql, params)
    }

    fn query_params_with<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        func: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        F: FnMut(&Row) -> Result<T, Error> + 'static,
        S: Into<Cow<'static, str>>,
        P: Params<'a>,
        Self: Sized,
    {
        self.query_params_with(sql, params, func)
    }

    fn query_with<T, S, F>(
        self,
        sql: S,
        func: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        F: FnMut(&Row) -> Result<T, Error> + 'static,
        S: Into<Cow<'static, str>>,
        Self: Sized,
    {
        self.query_with(sql, func)
    }
}

impl Connection {
    /// Creates a connection future that will resolve to a Connection once connected.
    /// # Example
    /// ```
    /// use mssql_client::Connection;
    ///
    /// let conn_str = "server=tcp:localhost\\SQL2017;database=Database1;integratedsecurity=sspi;trustservercertificate=true";
    /// let connection = Connection::connect(conn_str);
    /// ```

    pub fn connect<S>(conn_str: S) -> impl Future<Item = Connection, Error = Error>
    where
        S: Into<String>,
    {
        match utils::adjust_conn_str(&conn_str.into()) {
            Ok(conn_str) => Either::A(conn_arch(conn_str)),
            Err(e) => Either::B(err(e)),
        }
    }

    /// Creates a connection that will connect to the database specified in the environment variable.
    ///
    /// An error is returned if the environment variable could not be read.
    ///
    /// # Example
    /// ```
    /// use mssql_client::Connection;
    ///
    /// let connection = Connection::from_env("MSSQL_DB");
    /// ```
    pub fn from_env<K>(key: K) -> impl Future<Item = Connection, Error = Error>
    where
        K: AsRef<OsStr>,
    {
        let key = key.as_ref();

        match ::std::env::var(key) {
            Ok(conn_str) => Either::A(Connection::connect(conn_str)),
            Err(e) => Either::B(err(format_err!(
                "Connection from env variable {:#?} failed. {}",
                key,
                e
            ))),
        }
    }

    /// Execute sql statements that dont return rows.
    ///
    /// # Example
    /// ```
    /// #[macro_use]
    /// extern crate mssql_client;
    /// extern crate tokio;
    ///
    /// use mssql_client::Connection;
    /// use tokio::executor::current_thread::block_on_all;
    ///
    /// fn main() {
    ///     let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
    ///     let query = connection.execute("DECLARE @a INT = 0");
    ///     let connection = block_on_all(query).unwrap();
    /// }
    /// ```
    pub fn execute<S>(self, sql: S) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        S: Into<Cow<'static, str>>,
    {
        self.execute_params(sql, ())
    }

    pub fn execute_params<'a, S, P>(
        self,
        sql: S,
        params: P,
    ) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        S: Into<Cow<'static, str>>,
        P: Params<'a>,
    {
        let mut p = Vec::new();
        params.params(&mut p);

        let sql = sql.into();
        let log_sql = format!("{:?}\nParams: {:#?}", sql, p);
        let start = Instant::now();

        log::debug!("Executing {}", log_sql);

        let map_err = move |e| {
            debug!("Execute failed {}.", log_sql);
            format_err!("Execute failed. {:?}", e)
        };

        let map_ok = move |(_, c)| {
            debug!(
                "Execute executed in {}ms.",
                (Instant::now() - start).as_millis(),
            );
            Connection(c)
        };

        if p.is_empty() {
            Box::new(self.0.simple_exec(sql).map_err(map_err).map(map_ok))
        } else {
            let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();

            Box::new(self.0.exec(sql, &params).map_err(map_err).map(map_ok))
        }
    }

    /// Execute sql query and returns all the rows.
    ///
    /// # Example
    /// ```
    /// #[macro_use]
    /// extern crate mssql_client;
    /// extern crate tokio;
    ///
    /// use mssql_client::Connection;
    /// use tokio::executor::current_thread::block_on_all;
    ///
    /// fn main() {
    ///     let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
    ///     let query = connection.query("SELECT 1");
    ///     let (connection, rows): (_, Vec<i32>) = block_on_all(query).unwrap();
    ///     assert_eq!(rows[0], 1);
    /// }
    /// ```
    pub fn query<T, S>(self, sql: S) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        S: Into<Cow<'static, str>>,
        T: FromRow + 'static,
    {
        self.query_params_with(sql, (), FromRow::from_row)
    }

    pub fn query_params<'a, T, S, P>(
        self,
        sql: S,
        params: P,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        P: Params<'a>,
        S: Into<Cow<'static, str>>,
        T: FromRow + 'static,
    {
        self.query_params_with(sql, params, FromRow::from_row)
    }

    pub fn query_params_with<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        mut func: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        F: FnMut(&Row) -> Result<T, Error> + 'static,
        S: Into<Cow<'static, str>>,
        P: Params<'a>,
    {
        let mut p = Vec::new();
        params.params(&mut p);

        let sql = sql.into();
        let log_sql = format!("{:?}\nParams: {:#?}", sql, p);
        let start = Instant::now();

        log::debug!("Querying {}", log_sql);

        let map_err = move |e| {
            debug!("Query failed {}.", log_sql);
            format_err!("Query failed. {:?}", e)
        };

        let map_ok = move |(rows, c)| {
            debug!(
                "Query executed in {}ms.",
                (Instant::now() - start).as_millis(),
            );
            (Connection(c), rows)
        };

        let map_rows = move |row| func(&Row(row)).context("row conversion failed.");

        if p.is_empty() {
            Box::new(
                self.0
                    .simple_query(sql)
                    .map_err(map_err)
                    .map_result_exhaust(map_rows)
                    .collect()
                    .map(map_ok),
            )
        } else {
            let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();

            Box::new(
                self.0
                    .query(sql, &params)
                    .map_err(map_err)
                    .map_result_exhaust(map_rows)
                    .collect()
                    .map(map_ok),
            )
        }
    }

    pub fn query_with<T, S, F>(
        self,
        sql: S,
        func: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        F: FnMut(&Row) -> Result<T, Error> + 'static,
        S: Into<Cow<'static, str>>,
    {
        self.query_params_with(sql, (), func)
    }

    pub fn transaction(self) -> Box<dyn Future<Item = Transaction, Error = Error>> {
        debug!("starting transaction...");
        let start = Instant::now();

        let q = self
            .0
            .transaction()
            .and_then(|t| t.simple_exec("BEGIN TRANSACTION"))
            .map(move |(_, t)| {
                debug!(
                    "transaction started in {}ms.",
                    (Instant::now() - start).as_millis(),
                );
                Transaction(t)
            })
            .map_err(|e| {
                let e = format_err!("Start transaction failed. {:?}", e);
                error!("{}", e);
                e
            });

        Box::new(q)
    }
}

#[cfg(windows)]
fn conn_arch(conn_str: String) -> Box<dyn Future<Item = Connection, Error = Error>> {
    use failure::err_msg;
    use futures_locks::Mutex;
    use lazy_static::lazy_static;

    lazy_static! {
        static ref GATE: Mutex<()> = Mutex::new(());
    }

    // Lock while establishing a sql connection (only one call at a time)
    // This prevent a dead lock / timeout in the windows CertGetCertificateChain function.

    log::trace!("acquiring connection lock.");

    let q = GATE
        .lock()
        .map_err(|_| err_msg("Lock error."))
        .and_then(move |l| {
            log::trace!("connection lock acquired.");
            debug!("Connecting to db...");

            let start = Instant::now();

            SqlConnection::connect(&conn_str)
                .map(move |c| {
                    std::mem::drop(l); // drop the lock
                    debug!(
                        "Connected to db in {}ms.",
                        (Instant::now() - start).as_millis(),
                    );
                    Connection(c)
                })
                .map_err(move |e| {
                    let e = format_err!("Failed connecting to db. {:?}", e);
                    error!("{}", e);
                    debug!("Failed connection string: {}", conn_str);
                    e
                })
        });

    Box::new(q)
}

#[cfg(not(windows))]
fn conn_arch(conn_str: String) -> Box<dyn Future<Item = Connection, Error = Error>> {
    debug!("Connecting to db...");
    let start = Instant::now();

    let q = SqlConnection::connect(&conn_str)
        .map(move |c| {
            debug!(
                "Connected to db in {}ms.",
                (Instant::now() - start).as_millis(),
            );
            Connection(c)
        })
        .map_err(move |e| {
            let e = format_err!("Failed connecting to db. {:?}", e);
            error!("{}", e);
            debug!("Failed connection string: {}", conn_str);
            e
        });

    Box::new(q)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::executor::current_thread::block_on_all;

    #[test]
    fn connect() {
        let _connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
    }

    #[test]
    fn execute() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let _connection = block_on_all(connection.execute("DECLARE @a INT = 0")).unwrap();
    }

    #[test]
    fn execute_params() {
        block_on_all(
            Connection::from_env("MSSQL_DB")
                .and_then(|t| t.execute_params("DECLARE @a INT = @p1", 10)),
        )
        .unwrap();
    }

    #[test]
    fn query() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let (_connection, rows) = block_on_all(connection.query("SELECT 2")).unwrap();
        assert_eq!(2, rows[0]);
    }

    #[test]
    fn query_params() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let (_connection, rows) = block_on_all(
            connection.query_params::<(String, i32), _, _>("SELECT @P1, @P2", ("Foo", 3)),
        )
        .unwrap();
        assert_eq!("Foo", &rows[0].0);
        assert_eq!(3, rows[0].1);
    }

    #[test]
    fn query_decimal() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let (_connection, rows) =
            block_on_all(connection.query("SELECT CAST(15337032 as DECIMAL(28, 12))")).unwrap();
        assert_eq!(decimal::Decimal::from(15337032), rows[0]);
    }

    #[test]
    fn query_f64() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let (_connection, rows) =
            block_on_all(connection.query("SELECT CAST(15337032 as DECIMAL(28, 12))")).unwrap();
        assert_eq!(15337032f64, rows[0]);
    }
}
