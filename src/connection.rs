use crate::{utils, Command, FromRow, Query, Row, StateStreamExt, Transaction};
use failure::{format_err, Error, ResultExt};
use futures::{self, Future, IntoFuture};
use futures_state_stream::StateStream;
use humantime::format_duration;
use log::{debug, error, trace};
use std::ffi::OsStr;
use std::time::Instant;
use tiberius::ty::ToSql;
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
    fn execute<'a, Q>(self, query: Q) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        Q: Into<Query<'a>>,
        Self: Sized,
    {
        self.execute(query)
    }

    fn query_with<'a, Q, F: 'static, T: 'static>(
        self,
        query: Q,
        f: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        Q: Into<Query<'a>>,
        F: FnMut(&Row) -> Result<T, Error>,
    {
        self.query_with(query, f)
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

    pub fn connect<S>(conn_str: S) -> Box<dyn Future<Item = Connection, Error = Error>>
    where
        S: Into<String>,
    {
        match utils::replace_conn_str_machine_with_ip(&conn_str.into()) {
            Ok(conn_str) => conn_arch(conn_str),
            Err(e) => Box::new(Err(e).into_future()),
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
    pub fn from_env<K>(key: K) -> Box<dyn Future<Item = Connection, Error = Error>>
    where
        K: AsRef<OsStr>,
    {
        let key = key.as_ref();

        match ::std::env::var(key) {
            Ok(conn_str) => Connection::connect(conn_str),
            Err(e) => Box::new(futures::future::err(format_err!(
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
    pub fn execute<'a, Q>(self, query: Q) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        Q: Into<Query<'a>>,
    {
        let query = query.into();
        let sql = query.sql.to_owned();
        let sql2 = sql.clone();
        let start = Instant::now();

        debug!("Executing...");

        let map_conn = move |(_, c)| {
            debug!(
                "Execute successful in {}.",
                format_duration(Instant::now() - start)
            );
            Connection(c)
        };

        let map_err = move |e| {
            let e = format_err!("Execute failed. {:?}", e);
            error!("{}", e);
            debug!("Failed sql: {}", sql2);
            e
        };

        if query.params.is_empty() {
            trace!("Sql: {}", sql);
            Box::new(self.0.simple_exec(sql).map(map_conn).map_err(map_err))
        } else {
            let mut params = Vec::with_capacity(query.params.len());

            for p in query.params.iter() {
                match p.to_parameter() {
                    Ok(p) => params.push(p),
                    Err(e) => return Box::new(futures::future::err(e)),
                }
            }

            utils::trace_sql_params(&sql, &params);

            let params: Vec<&ToSql> = params.iter().map(|p| p.into()).collect();
            Box::new(self.0.exec(sql, &params).map(map_conn).map_err(map_err))
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
    pub fn query<'a, Q, T: 'static>(
        self,
        query: Q,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        Q: Into<Query<'a>>,
        T: FromRow,
    {
        self.query_with(query, FromRow::from_row)
    }

    pub fn query_with<'a, Q, F: 'static, T: 'static>(
        self,
        query: Q,
        mut f: F,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        Q: Into<Query<'a>>,
        F: FnMut(&Row) -> Result<T, Error>,
    {
        let conn = self.0;
        let query = query.into();
        let sql = query.sql.to_owned();
        let sql2 = sql.clone();
        let start = Instant::now();

        debug!("Querying...");

        let select = move |row| f(&Row(row)).context("row conversion failed.");

        let map_result = move |(rows, c)| {
            debug!(
                "Query successful in {}.",
                format_duration(Instant::now() - start)
            );
            (Connection(c), rows)
        };

        let map_err = move |e| {
            let e = format_err!("Query failed. {:?}", e);
            error!("{}", e);
            debug!("Failed sql: {}", sql2);
            e
        };

        if query.params.is_empty() {
            trace!("Sql: {}", sql);

            let q = conn
                .simple_query(sql)
                .map_err(map_err)
                .map_result_exhaust(select)
                .collect()
                .map(map_result);

            Box::new(q)
        } else {
            let mut params = Vec::with_capacity(query.params.len());

            for p in query.params.iter() {
                match p.to_parameter() {
                    Ok(p) => params.push(p),
                    Err(e) => return Box::new(futures::future::err(e)),
                }
            }

            utils::trace_sql_params(&sql, &params);

            let params: Vec<&ToSql> = params.iter().map(|p| p.into()).collect();

            let q = conn
                .query(sql, &params[..])
                .map_err(map_err)
                .map_result_exhaust(select)
                .collect()
                .map(map_result);

            Box::new(q)
        }
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
                    "transaction started in {}.",
                    format_duration(Instant::now() - start)
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
    use futures::Future;
    use futures_locks::Mutex;
    use lazy_static::lazy_static;

    lazy_static! {
        static ref GATE: Mutex<()> = Mutex::new(());
    }

    // Lock while establishing a sql connection (only one call at a time)
    // This prevent a dead lock / timeout in the windows CertGetCertificateChain function.

    trace!("acquiring connection lock.");

    let q = GATE
        .lock()
        .map_err(|_| err_msg("Lock error."))
        .and_then(move |l| {
            trace!("connection lock acquired.");
            debug!("Connecting to db...");

            let start = Instant::now();

            SqlConnection::connect(&conn_str)
                .map(move |c| {
                    std::mem::drop(l); // drop the lock
                    debug!(
                        "Connected to db in {}.",
                        format_duration(Instant::now() - start)
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
                "Connected to db in {}.",
                format_duration(Instant::now() - start)
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
    use crate::{Query, ToParameter};
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
    fn query() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let (_connection, rows) = block_on_all(connection.query("SELECT 2")).unwrap();
        assert_eq!(2, rows[0]);
    }

    #[test]
    fn query_params() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let params = &[&"Foo" as &ToParameter, &3 as &ToParameter];
        let query = Query {
            sql: "SELECT @P1, @P2",
            params,
        };
        let (_connection, rows) =
            block_on_all(connection.query::<_, (String, i32)>(query)).unwrap();
        assert_eq!("Foo", &rows[0].0);
        assert_eq!(3, rows[0].1);
    }
}
