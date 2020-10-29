use crate::{utils::reduce, Command, Connection, FromRow, Params, Result, Row};
use futures03::{compat::Future01CompatExt, future::LocalBoxFuture};
use std::{borrow::Cow, ffi::OsStr, fmt::Debug};
use tiberius::{BoxableIo, Transaction as SqlTransaction};
use tracing::instrument;

pub struct Transaction(pub(super) SqlTransaction<Box<dyn BoxableIo>>);

impl Command for Transaction {
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

impl Transaction {
    pub async fn from_env<K>(key: K) -> Result<Self>
    where
        K: AsRef<OsStr>,
    {
        Connection::from_env(key).await?.transaction().await
    }

    pub fn commit(self) -> LocalBoxFuture<'static, Result<Connection>> {
        Box::pin(self.commit_imp())
    }

    #[instrument(level = "debug", name = "Transaction::commit", skip(self), err)]
    async fn commit_imp(self) -> Result<Connection> {
        Ok(Connection(self.0.commit().compat().await?))
    }

    pub fn execute<'a, S, P>(self, sql: S, params: P) -> LocalBoxFuture<'a, Result<Self>>
    where
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
    {
        Box::pin(self.execute_imp(sql, params))
    }

    #[instrument(level = "debug", name = "Transaction::execute", skip(self), err)]
    async fn execute_imp<'a, S, P>(self, sql: S, params: P) -> Result<Self>
    where
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
    {
        let mut p = Vec::new();
        params.params(&mut p);

        let sql = sql.into();

        let (_affected_rows, t) = if p.is_empty() {
            self.0.simple_exec(sql).compat().await
        } else {
            let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();
            self.0.exec(sql, &params).compat().await
        }?;

        Ok(Self(t))
    }

    pub fn query<'a, T, S, P>(self, sql: S, params: P) -> LocalBoxFuture<'a, Result<(Self, Vec<T>)>>
    where
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
        T: FromRow + 'a,
    {
        self.query_map(sql, params, FromRow::from_row)
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
        name = "Transaction::query",
        skip(self, init, func),
        err
    )]
    async fn query_fold_imp<'a, T, S, P, F>(
        self,
        sql: S,
        params: P,
        init: T,
        mut func: F,
    ) -> Result<(Self, T)>
    where
        F: FnMut(T, &Row) -> Result<T> + 'a,
        P: Debug + Params<'a> + 'a,
        S: Debug + Into<Cow<'static, str>> + 'a,
        T: 'a,
    {
        let mut p = Vec::new();
        params.params(&mut p);

        let sql = sql.into();
        let next = move |r, row| func(r, &Row(row));

        let (t, rows) = if p.is_empty() {
            let stream = self.0.simple_query(sql);
            reduce(stream, init, next).await?
        } else {
            let params = p.iter().map(|p| p.into()).collect::<Vec<_>>();
            let stream = self.0.query(sql, &params);
            reduce(stream, init, next).await?
        };

        Ok((Self(t), rows))
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
    pub fn rollback(self) -> LocalBoxFuture<'static, Result<Connection>> {
        Box::pin(self.rollback_imp())
    }

    #[instrument(level = "trace", name = "Transaction::rollback", skip(self), err)]
    async fn rollback_imp(self) -> Result<Connection> {
        Ok(Connection(self.0.rollback().compat().await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn commit() -> Result<()> {
        Connection::from_env("MSSQL_DB")
            .await?
            .transaction()
            .await?
            .commit()
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn execute() -> Result<()> {
        Connection::from_env("MSSQL_DB")
            .await?
            .transaction()
            .await?
            .execute("DECLARE @a INT = 0", ())
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn execute_params() -> Result<()> {
        Connection::from_env("MSSQL_DB")
            .await?
            .transaction()
            .await?
            .execute("DECLARE @a INT = @p1", 10)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn query() -> Result<()> {
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
    async fn rollback() -> Result<()> {
        Connection::from_env("MSSQL_DB")
            .await?
            .transaction()
            .await?
            .rollback()
            .await?;

        Ok(())
    }
}
