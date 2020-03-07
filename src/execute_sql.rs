/// Execute a sql statement using named parameters.
///
/// # Example
///
/// ```
/// use mssql_client::{execute_sql, Connection};
///
/// #[tokio::main]
/// async fn main() -> Result<(), failure::Error> {
///     let conn = Connection::from_env("MSSQL_DB").await?;
///     let _conn = execute_sql!(conn, r#"
///         DECLARE @value1 int = @id;
///         DECLARE @value2 VARCHAR(20) = @name;"#,
///         id = 55,
///         name = "Foo"
///     ).await?;
///
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! execute_sql {
    ($command:expr, $sql:expr, $($fname:ident = $fvalue:expr),* $(,)*) => {
        {
            let sql = {
                let sql: &'static str = $sql;
                let mut sql = sql.to_owned();
                let mut i = 1;

                $(
                    $crate::replace_params(&mut sql, stringify!($fname), &format!("P{}", i));
                    #[allow(unused_assignments)]
                    {
                        i += 1;
                    }
                )*

                sql
            };

            $command.execute(sql, ($($fvalue,)*))
        }
    };
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn execute_works() -> Result<(), failure::Error> {
        use crate::Connection;

        struct Account<'a> {
            name: &'a str,
            id: i32,
        }

        let connection = Connection::from_env("MSSQL_DB").await?;

        let account = Account {
            name: "Foo",
            id: 54,
        };

        let conn = connection
            .execute("CREATE TABLE #Temp (Id int, Name NVARCHAR(10))", ())
            .await?;

        let conn = execute_sql!(
            conn,
            "INSERT #Temp (Id, Name) VALUES (@id, @name);",
            id = account.id,
            name = account.name
        )
        .await?;

        let (_, rows): (_, Vec<(i32, String)>) = conn.query("SELECT * FROM #Temp", ()).await?;

        assert_eq!(54, rows[0].0);
        assert_eq!("Foo", &rows[0].1);
        Ok(())
    }
}
