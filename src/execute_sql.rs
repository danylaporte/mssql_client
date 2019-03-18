/// Turn a sql statement into a function.
///
/// # Example
///
/// Taking a struct in the argument of the function.
/// ```
/// #[macro_use]
/// extern crate mssql_client;
/// #[macro_use]
/// extern crate lazy_static;
/// extern crate regex;
/// extern crate tokio;
///
/// use mssql_client::Connection;
/// use tokio::executor::current_thread::block_on_all;
///
/// struct MyEntity<'a> {
///     name: &'a str,
///     id: i32,
/// }
///
/// // Create a function named insert_into_temp taking 2 parameters:
/// // - a command
/// // - a reference of the struct MyEntity.
/// execute_sql! {
///     fn insert_into_temp( MyEntity { id, name, }) {
///         "INSERT INTO #Temp (Id, Name) VALUES (@id, @name)"
///     }
/// }
///
/// fn main() {
///     let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
///     let execute = connection.execute("CREATE TABLE #Temp (Id int, Name NVARCHAR(10))");
///     let connection = block_on_all(execute).unwrap();
///
///     let record = MyEntity {
///         name: "FooBar",
///         id: 2,
///     };
///
///     // perform the execute request
///     let _connection = block_on_all(insert_into_temp(connection, &record)).unwrap();
/// }
/// ```
///
/// Taking a list of arguments in the functions.
/// ```
/// #[macro_use]
/// extern crate mssql_client;
/// #[macro_use]
/// extern crate lazy_static;
/// extern crate regex;
/// extern crate tokio;
///
/// use mssql_client::Connection;
/// use tokio::executor::current_thread::block_on_all;
///
/// // Create a function named insert_into_temp taking 3 parameters,
/// // - a command,
/// // - an id
/// // - a name.
/// execute_sql! {
///     fn insert_into_temp(id: i32, name: &str,) {
///         "INSERT INTO #Temp (Id, Name) VALUES (@id, @name)"
///     }
/// }
///
/// fn main() {
///     let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
///     let execute = connection.execute("CREATE TABLE #Temp (Id int, Name NVARCHAR(10))");
///     let connection = block_on_all(execute).unwrap();
///
///     // perform the execute request
///     let _connection = block_on_all(insert_into_temp(connection, 2, "FooBar")).unwrap();
/// }
/// ```
///
/// Querying.
/// ```
/// #[macro_use]
/// extern crate mssql_client;
/// #[macro_use]
/// extern crate lazy_static;
/// extern crate regex;
/// extern crate tokio;
///
/// use mssql_client::Connection;
/// use tokio::executor::current_thread::block_on_all;
///
/// // Create a function named insert_into_temp taking 3 parameters,
/// // - a command,
/// // - an id
/// // - a name.
/// execute_sql! {
///     fn fetch_values(id: i32, name: &str,) -> (i32, String) {
///         "SELECT @id, @name"
///     }
/// }
///
/// fn main() {
///     let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
///     let values = block_on_all(fetch_values(connection, 55, "Foo")).unwrap().1;
///     
///     assert_eq!(values[0].0, 55);
///     assert_eq!(&values[0].1, "Foo");
/// }
/// ```
#[macro_export]
macro_rules! execute_sql {
    (fn $name:ident($($field:ident: $t:ty,)*) { $q:expr }) => {
        fn $name<C>(command: C, $($field: $t,)*) -> Box<dyn ::futures::Future<Item = C, Error = ::failure::Error>> where C: $crate::Command {

            ::lazy_static::lazy_static! {
                static ref SQL: String = $crate::replace_sql_params!($($field,)* { $q });
            }

            let q = $crate::Query {
                sql: &SQL,
                params: &[$(&$field as &$crate::ToParameter,)*],
            };

            command.execute(q)
        }
    };
    (pub fn $name:ident($($field:ident: $t:ty,)*) { $q:expr }) => {
        pub fn $name<C>(command: C, $($field: $t,)*) -> Box<dyn  ::futures::Future<Item = C, Error = ::failure::Error>> where C: $crate::Command {

            ::lazy_static::lazy_static! {
                static ref SQL: String = $crate::replace_sql_params!($($field,)* { $q });
            }

            let q = $crate::Query {
                sql: &SQL,
                params: &[$(&$field as &$crate::ToParameter,)*],
            };

            command.execute(q)
        }
    };
    (fn $name:ident($($field:ident: $t:ty,)*) -> $out:ty { $q:expr }) => {
        fn $name<C>(command: C, $($field: $t,)*) -> Box<dyn ::futures::Future<Item = (C, Vec<$out>), Error = ::failure::Error>> where C: $crate::Command {

            ::lazy_static::lazy_static! {
                static ref SQL: String = $crate::replace_sql_params!($($field,)* { $q });
            }

            let q = $crate::Query {
                sql: &SQL,
                params: &[$(&$field as &$crate::ToParameter,)*],
            };

            command.query(q)
        }
    };
    (pub fn $name:ident($($field:ident: $t:ty,)*) -> $out:ty { $q:expr }) => {
        pub fn $name<C>(command: C, $($field: $t,)*) -> Box<dyn ::futures::Future<Item = (C, Vec<$out>), Error = ::failure::Error>> where C: $crate::Command {

            ::lazy_static::lazy_static! {
                static ref SQL: String = $crate::replace_sql_params!($($field,)* { $q });
            }

            let q = $crate::Query {
                sql: &SQL,
                params: &[$(&$field as &$crate::ToParameter,)*],
            };

            command.query(q)
        }
    };
    (fn $name:ident($e:ty { $($field:ident,)* }) { $q:expr }) => {
        fn $name<C>(command: C, entity: &$e) -> Box<dyn ::futures::Future<Item = C, Error = ::failure::Error>> where C: $crate::Command {

            ::lazy_static::lazy_static! {
                static ref SQL: String = $crate::replace_sql_params!($($field,)* { $q });
            }

            let q = $crate::Query {
                sql: &SQL,
                params: &[$(&entity.$field as &$crate::ToParameter,)*],
            };

            command.execute(q)
        }
    };
    (pub fn $name:ident($e:ty { $($field:ident,)* }) { $q:expr }) => {
        pub fn $name<C>(command: C, entity: &$e) -> Box<dyn ::futures::Future<Item = C, Error = ::failure::Error>> where C: $crate::Command {

            ::lazy_static::lazy_static! {
                static ref SQL: String = $crate::replace_sql_params!($($field,)* { $q });
            }

            let q = $crate::Query {
                sql: &SQL,
                params: &[$(&entity.$field as &$crate::ToParameter,)*],
            };

            command.execute(q)
        }
    };
    (fn $name:ident($e:ty { $($field:ident,)* }) -> $out:ty { $q:expr }) => {
        fn $name<C>(command: C, entity: &$e) -> Box<dyn ::futures::Future<Item = (C, Vec<$out>), Error = ::failure::Error>> where C: $crate::Command {

            ::lazy_static::lazy_static! {
                static ref SQL: String = $crate::replace_sql_params!($($field,)* { $q });
            }

            let q = $crate::Query {
                sql: &SQL,
                params: &[$(&entity.$field as &$crate::ToParameter,)*],
            };

            command.query(q)
        }
    };
    (pub fn $name:ident($e:ty { $($field:ident,)* }) -> $out:ty { $q:expr }) => {
        pub fn $name<C>(command: C, entity: &$e) -> Box<dyn ::futures::Future<Item = (C, Vec<$out>), Error = ::failure::Error>> where C: $crate::Command {

            ::lazy_static::lazy_static! {
                static ref SQL: String = $crate::replace_sql_params!($($field,)* { $q });
            }

            let q = $crate::Query {
                sql: &SQL,
                params: &[$(&entity.$field as &$crate::ToParameter,)*],
            };

            command.query(q)
        }
    };
}

#[test]
fn test_with_struct_arg() {
    use crate::Connection;
    use tokio::executor::current_thread::block_on_all;

    struct Account<'a> {
        name: &'a str,
        id: i32,
    }

    execute_sql! {
        fn set_account_name(Account { name, id, }) {
            "INSERT #Temp (Id, Name) VALUES (@id, @name);"
        }
    }

    let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();

    let account = Account {
        name: "Foo",
        id: 54,
    };

    let execute = connection.execute("CREATE TABLE #Temp (Id int, Name NVARCHAR(10))");
    let connection = block_on_all(execute).unwrap();
    let connection = block_on_all(set_account_name(connection, &account)).unwrap();
    let query = connection.query("SELECT * FROM #Temp");
    let rows: Vec<(i32, String)> = block_on_all(query).unwrap().1;

    assert_eq!(54, rows[0].0);
    assert_eq!("Foo", &rows[0].1);
}

#[test]
fn test_with_simple_args() {
    use crate::Connection;
    use tokio::executor::current_thread::block_on_all;

    execute_sql! {
        fn set_account_name2(name: &str, id: i32,) {
            "INSERT #Temp (Id, Name) VALUES (@id, @name);"
        }
    }

    let connection = block_on_all(Connection::from_env("MSSQL_DB")).unwrap();
    let execute = connection.execute("CREATE TABLE #Temp (Id int, Name NVARCHAR(10))");
    let connection = block_on_all(execute).unwrap();
    let connection = block_on_all(set_account_name2(connection, "Foo", 54)).unwrap();
    let query = connection.query("SELECT * FROM #Temp");
    let rows: Vec<(i32, String)> = block_on_all(query).unwrap().1;

    assert_eq!(54, rows[0].0);
    assert_eq!("Foo", &rows[0].1);
}

#[test]
fn test_select_str() {
    use crate::Connection;
    use futures::Future;

    execute_sql! {
        fn exec(v: &str,) -> String {
            "SELECT @v"
        }
    }

    let f = Connection::from_env("MSSQL_DB").and_then(|c| exec(c, "Foo"));
    let (_, rows) = tokio::executor::current_thread::block_on_all(f).unwrap();
    assert_eq!("Foo", &rows[0]);
}

#[test]
fn test_select_uuid() {
    use crate::Connection;
    use futures::Future;
    use uuid::Uuid;

    execute_sql! {
        fn exec(v: Uuid,) -> Uuid {
            "SELECT @v"
        }
    }

    let id = Uuid::new_v4();
    let f = Connection::from_env("MSSQL_DB").and_then(|c| exec(c, id));
    let (_, rows) = tokio::executor::current_thread::block_on_all(f).unwrap();
    assert_eq!(id, rows[0]);
}
