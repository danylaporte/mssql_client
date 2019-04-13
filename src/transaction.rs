use crate::{utils, Command, Connection, FromRow, Query, Row, StateStreamExt};
use failure::{format_err, Error, ResultExt};
use futures::Future;
use futures_state_stream::StateStream;
use log::{debug, error, trace};
use std::time::Instant;
use tiberius::ty::ToSql;
use tiberius::{BoxableIo, Transaction as SqlTransaction};

pub struct Transaction(pub(super) SqlTransaction<Box<BoxableIo>>);

impl Command for Transaction {
    fn execute<'a, Q>(self, query: Q) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        Q: Into<Query<'a>>,
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

impl Transaction {
    pub fn commit(self) -> impl Future<Item = Connection, Error = Error> {
        debug!("Committing transaction...");
        let start = Instant::now();

        self.0
            .commit()
            .map(move |c| {
                debug!(
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

    pub fn execute<'a, Q>(self, query: Q) -> Box<dyn Future<Item = Self, Error = Error>>
    where
        Q: Into<Query<'a>>,
    {
        let query = query.into();
        let sql = query.sql.to_owned();
        let sql2 = sql.clone();
        let start = Instant::now();

        debug!("Executing...");

        let map_conn = move |(_, t)| {
            debug!(
                "Execute successful in {}.",
                (Instant::now() - start).as_millis(),
            );
            Transaction(t)
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
        let transaction = self.0;
        let query = query.into();
        let sql = query.sql.to_owned();
        let sql2 = sql.clone();
        let map_rows = move |row| f(&Row(row)).context("row conversion failed.");
        let start = Instant::now();

        debug!("Querying...");

        let map_result = move |(rows, t)| {
            debug!(
                "Query successful in {}.",
                (Instant::now() - start).as_millis(),
            );
            (Transaction(t), rows)
        };

        let map_err = move |e| {
            let e = format_err!("Query failed. {:?}", e);
            error!("{}", e);
            debug!("Failed sql: {}", sql2);
            e
        };

        if query.params.is_empty() {
            trace!("Sql: {}", sql);

            let q = transaction
                .simple_query(sql)
                .map_err(map_err)
                .map_result_exhaust(map_rows)
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

            let q = transaction
                .query(sql, &params[..])
                .map_err(map_err)
                .map_result_exhaust(map_rows)
                .collect()
                .map(map_result);

            Box::new(q)
        }
    }

    pub fn rollback(self) -> impl Future<Item = Connection, Error = Error> {
        debug!("Transaction rollback...");
        let start = Instant::now();

        self.0
            .rollback()
            .map(move |c| {
                debug!(
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
