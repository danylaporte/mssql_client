use crate::{utils::reduce, Command, Connection, FromRow, Params, Row};
use failure::{format_err, Error};
use futures::future::{Either, Future};
use futures_state_stream::StateStream;
use log::{debug, error, trace};
use std::borrow::Cow;
use std::time::Instant;
use tiberius::{BoxableIo, Transaction as SqlTransaction};

pub struct Transaction(pub(super) SqlTransaction<Box<dyn BoxableIo>>);

impl Command for Transaction {
    fn execute<'a, S, P>(self, sql: S, params: P) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        P: Params<'a>,
        S: Into<Cow<'static, str>>,
    {
        Box::new(self.execute(sql, params))
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
        T: 'static,
    {
        Box::new(self.query_fold(sql, params, init, func))
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

    pub fn execute<'a, S, P>(self, sql: S, params: P) -> Box<dyn Future<Item = Self, Error = Error>>
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

        Box::new(if p.is_empty() {
            Either::A(self.0.simple_exec(sql).map_err(map_err).map(map_ok))
        } else {
            let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();
            Either::B(self.0.exec(sql, &params).map_err(map_err).map(map_ok))
        })
    }

    pub fn query<'a, T, S, P>(
        self,
        sql: S,
        params: P,
    ) -> Box<dyn Future<Item = (Self, Vec<T>), Error = Error>>
    where
        S: Into<Cow<'static, str>>,
        T: FromRow + 'static,
        P: Params<'a>,
    {
        self.query_map(sql, params, FromRow::from_row)
    }

    pub fn query_fold<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        init: T,
        mut func: F,
    ) -> Box<dyn Future<Item = (Self, T), Error = Error>>
    where
        F: FnMut(T, &Row) -> Result<T, Error> + 'static,
        S: Into<Cow<'static, str>>,
        P: Params<'a>,
        T: 'static,
    {
        let mut p = Vec::new();
        params.params(&mut p);

        let sql = sql.into();
        let log_sql = format!("{:?}\nParams: {:#?}", sql, p);
        let start = Instant::now();

        trace!("Querying {}", log_sql);

        let map_err = |e| format_err!("Query failed. {:?}", e);

        let next = move |r, row| match func(r, &Row(row)) {
            Ok(v) => Ok(v),
            Err(e) => Err(format_err!("Row conversion failed. {}", e)),
        };

        let fut = if p.is_empty() {
            let stream = self.0.simple_query(sql).map_err(map_err);
            Either::A(reduce(stream, init, next))
        } else {
            let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();
            let stream = self.0.query(sql, &params).map_err(map_err);

            Either::B(reduce(stream, init, next))
        };

        Box::new(
            fut.map(move |(c, r)| {
                trace!(
                    "Query executed in {}ms.",
                    (Instant::now() - start).as_millis(),
                );

                (Transaction(c), r)
            })
            .map_err(move |e| {
                trace!("Query failed {}.", log_sql);
                e
            }),
        )
    }

    pub fn query_map<'a, T, S, P, F>(
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
    {
        self.query_fold(sql, params, Vec::new(), move |mut vec, row| {
            vec.push(func(row)?);
            Ok(vec)
        })
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
        let _transaction = block_on_all(transaction.execute("DECLARE @a INT = 0", ())).unwrap();
    }

    #[test]
    fn execute_params() {
        block_on_all(
            Connection::from_env("MSSQL_DB")
                .and_then(Connection::transaction)
                .and_then(|t| t.execute("DECLARE @a INT = @p1", 10)),
        )
        .unwrap();
    }

    #[test]
    fn query() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let transaction = block_on_all(connection.transaction()).unwrap();
        let (_transaction, rows) = block_on_all(transaction.query("SELECT 5", ())).unwrap();

        assert_eq!(5, rows[0]);
    }

    #[test]
    fn rollback() {
        let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
        let transaction = block_on_all(connection.transaction()).unwrap();
        let _connection = block_on_all(transaction.rollback()).unwrap();
    }
}
