use crate::{utils::reduce, Command, Connection, FromRow, Params, Row};
use failure::{format_err, Error};
use futures03::{compat::Future01CompatExt, future::LocalBoxFuture};
use log::{debug, error, trace};
use std::{borrow::Cow, time::Instant};
use tiberius::{BoxableIo, Transaction as SqlTransaction};

pub struct Transaction(pub(super) SqlTransaction<Box<dyn BoxableIo>>);

impl Command for Transaction {
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

impl Transaction {
    pub async fn commit(self) -> Result<Connection, Error> {
        trace!("Committing transaction...");
        let start = Instant::now();

        let c = self.0.commit().compat().await.map_err(|e| {
            let e = format_err!("Transaction commit failed. {:?}", e);
            error!("{}", e);
            e
        })?;

        trace!(
            "Transaction committed in {}ms.",
            (Instant::now() - start).as_millis(),
        );

        Ok(Connection(c))
    }

    pub fn execute<'a, S, P>(self, sql: S, params: P) -> LocalBoxFuture<'a, Result<Self, Error>>
    where
        P: Params<'a> + 'a,
        S: Into<Cow<'static, str>> + 'a,
    {
        Box::pin(async {
            let mut p = Vec::new();
            params.params(&mut p);

            let sql = sql.into();
            let log_sql = format!("{:?}\nParams: {:#?}", sql, p);
            let start = Instant::now();

            trace!("Executing {}", log_sql);

            let (_affected_rows, t) = if p.is_empty() {
                self.0.simple_exec(sql).compat().await
            } else {
                let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();
                self.0.exec(sql, &params).compat().await
            }
            .map_err(move |e| {
                debug!("Execute failed {}.", log_sql);
                format_err!("Execute failed. {:?}", e)
            })?;

            trace!(
                "Execute executed in {}ms.",
                (Instant::now() - start).as_millis(),
            );

            Ok(Self(t))
        })
    }

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

            let next = move |r, row| match func(r, &Row(row)) {
                Ok(v) => Ok(v),
                Err(e) => Err(format_err!("Row conversion failed. {}", e)),
            };

            let (t, rows) = if p.is_empty() {
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

            Ok((Self(t), rows))
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

    pub async fn rollback(self) -> Result<Connection, Error> {
        trace!("Transaction rollback...");
        let start = Instant::now();

        let c = self.0.rollback().compat().await.map_err(|e| {
            let e = format_err!("Rollback failed.{:?}", e);
            error!("{}", e);
            e
        })?;

        trace!(
            "Transaction rollback successful in {}ms.",
            (Instant::now() - start).as_millis(),
        );

        Ok(Connection(c))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn commit() -> Result<(), failure::Error> {
        Connection::from_env("MSSQL_DB")
            .await?
            .transaction()
            .await?
            .commit()
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn execute() -> Result<(), failure::Error> {
        Connection::from_env("MSSQL_DB")
            .await?
            .transaction()
            .await?
            .execute("DECLARE @a INT = 0", ())
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn execute_params() -> Result<(), failure::Error> {
        Connection::from_env("MSSQL_DB")
            .await?
            .transaction()
            .await?
            .execute("DECLARE @a INT = @p1", 10)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn query() -> Result<(), failure::Error> {
        let (_, rows) = Connection::from_env("MSSQL_DB")
            .await?
            .transaction()
            .await?
            .query("SELECT 5", ())
            .await?;

        assert_eq!(5, rows[0]);
        Ok(())
    }

    #[tokio::test]
    async fn rollback() -> Result<(), failure::Error> {
        Connection::from_env("MSSQL_DB")
            .await?
            .transaction()
            .await?
            .rollback()
            .await?;

        Ok(())
    }
}
