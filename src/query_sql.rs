/// Query using named parameters.
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
///         .and_then(|conn| query_sql!(conn,
///             "SELECT @id, @name",
///             id = 55,
///             name = "Foo"
///         ));
///     
///     let rows: Vec<(i32, String)> = block_on_all(f).unwrap().1;
///
///     assert_eq!(55, rows[0].0);
///     assert_eq!("Foo", &rows[0].1);
/// }
/// ```
#[macro_export]
macro_rules! query_sql {
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

            $command.query_params(&*SQL, ($($fvalue,)*))
        }
    };
}
