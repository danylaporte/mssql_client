use crate::{Command, Connection, FromRow, Params, Row, StateStreamExt};
use failure::{format_err, Error, ResultExt};
use futures::Future;
use futures_state_stream::StateStream;
use log::{debug, error, trace};
use std::borrow::Cow;
use std::time::Instant;
use tiberius::{BoxableIo, Transaction as SqlTransaction};

pub struct Transaction(pub(super) SqlTransaction<Box<BoxableIo>>);

impl Command for Transaction {
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

impl Transaction {
    pub fn commit(self) -> impl Future<Item = Connection, Error = Error> {
        trace!("Committing transaction...");
        let start = Instant::now();

        self.0
            .commit()
            .map(move |c| {
                trace!(
                    "Transaction committed in {}ms.",
                    (Instant::now() - start).as_millis(),
                );
                Connection(c)
            })
            .map_err(|e| {
                let e = format_err!("Transaction commit failed. {:?}", e);
                error!("{}", e);
                e
            })
    }

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

        trace!("Executing {}", log_sql);

        let map_err = move |e| {
            debug!("Execute failed {}.", log_sql);
            format_err!("Execute failed. {:?}", e)
        };

        let map_ok = move |(_, c)| {
            trace!(
                "Execute executed in {}ms.",
                (Instant::now() - start).as_millis(),
            );
            Transaction(c)
        };

        if p.is_empty() {
            Box::new(self.0.simple_exec(sql).map_err(map_err).map(map_ok))
        } else {
            let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();
            Box::new(self.0.exec(sql, &params).map_err(map_err).map(map_ok))
        }
    }

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

        trace!("Querying {}", log_sql);

        let map_err1 = |e| format_err!("Query failed. {:?}", e);
        let map_err2 = move |e| {
            trace!("Query failed {}.", log_sql);
            e
        };

        let map_ok = move |(rows, t)| {
            trace!(
                "Query executed in {}ms.",
                (Instant::now() - start).as_millis(),
            );
            (Transaction(t), rows)
        };

        let map_rows = move |row| func(&Row(row)).context("row conversion failed.");

        if p.is_empty() {
            Box::new(
                self.0
                    .simple_query(sql)
                    .map_err(map_err1)
                    .map_result_exhaust(map_rows)
                    .collect()
                    .map(map_ok)
                    .map_err(map_err2),
            )
        } else {
            let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();

            Box::new(
                self.0
                    .query(sql, &params)
                    .map_err(map_err1)
                    .map_result_exhaust(map_rows)
                    .collect()
                    .map(map_ok)
                    .map_err(map_err2),
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

    pub fn rollback(self) -> impl Future<Item = Connection, Error = Error> {
        trace!("Transaction rollback...");
        let start = Instant::now();

        self.0
            .rollback()
            .map(move |c| {
                trace!(
                    "Transaction rollback successful in {}ms.",
                    (Instant::now() - start).as_millis(),
                );
                Connection(c)
            })
            .map_err(|e| {
                let e = format_err!("Rollback failed.{:?}", e);
                error!("{}", e);
                e
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::executor::current_thread::block_on_all;

    #[test]
    fn commit() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let transaction = block_on_all(connection.transaction()).unwrap();
        let _connection = block_on_all(transaction.commit()).unwrap();
    }

    #[test]
    fn execute() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let transaction = block_on_all(connection.transaction()).unwrap();
        let _transaction = block_on_all(transaction.execute("DECLARE @a INT = 0")).unwrap();
    }

    #[test]
    fn execute_params() {
        block_on_all(
            Connection::from_env("MSSQL_DB")
                .and_then(Connection::transaction)
                .and_then(|t| t.execute_params("DECLARE @a INT = @p1", 10)),
        )
        .unwrap();
    }

    #[test]
    fn query() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let transaction = block_on_all(connection.transaction()).unwrap();
        let (_transaction, rows) = block_on_all(transaction.query("SELECT 5")).unwrap();

        assert_eq!(5, rows[0]);
    }

    #[test]
    fn rollback() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let transaction = block_on_all(connection.transaction()).unwrap();
        let _connection = block_on_all(transaction.rollback()).unwrap();
    }
}
