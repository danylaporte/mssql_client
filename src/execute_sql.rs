/// Execute a sql statement using named parameters.
///
/// # Example
///
/// ```
/// #[macro_use]
/// extern crate mssql_client;
/// #[macro_use]
/// extern crate lazy_static;
/// extern crate tokio;
///
/// use mssql_client::Connection;
/// use futures::Future;
/// use tokio::executor::current_thread::block_on_all;
///
/// fn main() {
///     let f = Connection::from_env("MSSQL_DB")
///         .and_then(|conn| execute_sql!(conn, r#"
///             DECLARE @value1 int = @id;
///             DECLARE @value2 VARCHAR(20) = @name;"#,
///             id = 55,
///             name = "Foo"
///         ));
///     
///     block_on_all(f).unwrap();
/// }
/// ```
#[macro_export]
macro_rules! execute_sql {
    ($command:expr, $sql:expr, $($fname:ident = $fvalue:expr),* $(,)*) => {
        {
            ::lazy_static::lazy_static! {
                static ref SQL: String = {
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
            }

            $command.execute_params(&*SQL, ($($fvalue,)*))
        }
    };
}

#[test]
fn execute_works() {
    use crate::Connection;
    use futures::Future;
    use tokio::executor::current_thread::block_on_all;

    struct Account<'a> {
        name: &'a str,
        id: i32,
    }

    let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();

    let account = Account {
        name: "Foo",
        id: 54,
    };

    let f = connection
        .execute("CREATE TABLE #Temp (Id int, Name NVARCHAR(10))")
        .and_then(|conn| {
            execute_sql!(
                conn,
                "INSERT #Temp (Id, Name) VALUES (@id, @name);",
                id = account.id,
                name = account.name
            )
        })
        .and_then(|conn| conn.query("SELECT * FROM #Temp"));

    let rows: Vec<(i32, String)> = block_on_all(f).unwrap().1;

    assert_eq!(54, rows[0].0);
    assert_eq!("Foo", &rows[0].1);
}
