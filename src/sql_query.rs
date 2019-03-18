#[macro_export]
macro_rules! sql_query {
    (delete from $t:ident where ($($w:ident),+$(,)*)) => {
        {
            let mut i = 0;
            let w = &[$(format!("[{}] = @p{}", stringify!($w), {i += 1; i}),)*].join(" AND ");

            format!(
                "DELETE FROM [{t}] WHERE {w};",
                t = stringify!($t),
                w = w,
            )
        }
    };
    (insert into $t:ident ($($f:ident),+$(,)*)) => {
        {
            let f = &[$(format!("[{}]", stringify!($f)),)*];
            let v = f.iter().enumerate().map(|(i, _)| format!("@p{}", i+1)).collect::<Vec<_>>().join(",");
            let f = f.join(",");

            format!(
                "INSERT INTO [{t}] ({f}) VALUES ({v});",
                t = stringify!($t),
                f = f,
                v = v,
            )
        }
    };
    (merge into $t:ident set ($($f:ident),+$(,)*) where ($($k:ident),+$(,)*)) => {
        {
            let mut i = 0;
            let select = &[$(format!("{} [{}]", {i += 1; i}, stringify!($k)),)*].join(",");
            let select_on = &[$(format!("s.[{key}] = t.[{key}]", key = stringify!($k)),)*].join(" AND ");
            let update_set = &[$(format!("t.[{}] = @p{}", stringify!($f), {i += 1; i}),)*].join(",");
            let insert_fields = &[$(format!("[{}]", stringify!($k)),)* $(format!("[{}]", stringify!($f)),)*].join(",");
            let insert_values = (0..i).into_iter().map(|i| format!("@p{}", i+1)).collect::<Vec<_>>().join(",");

            format!(
                r#"
                MERGE INTO [dbo].[{t}] AS t
                USING (SELECT {select}) AS s ON {select_on}
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_fields}) VALUES ({insert_values});
                "#,
                t = stringify!($t),
                select = select,
                select_on = select_on,
                update_set = update_set,
                insert_fields = insert_fields,
                insert_values = insert_values,
            )
        }
    };
    (select ($($s:ident),+$(,)*) from $t:ident) => {
        {
            let s = &[$(format!("[{}]", stringify!($s)),)*].join(",");

            format!(
                "SELECT {s} FROM [{t}]",
                t = stringify!($t),
                s = s,
            )
        }
    };
    (select ($($s:ident),+$(,)*) from $t:ident where ($($w:ident),+$(,)*)) => {
        {
            let mut i = 0;
            let s = &[$(format!("[{}]", stringify!($s)),)*].join(",");
            let w = &[$(format!("[{}] = @p{}", stringify!($w), {i += 1; i}),)*].join(" AND ");

            format!(
                "SELECT {s} FROM [{t}] WHERE {w}",
                t = stringify!($t),
                s = s,
                w = w,
            )
        }
    };
    (update $t:ident set ($($s:ident),+$(,)*) where ($($w:ident),+$(,)*)) => {
        {
            let mut i = 0;
            let s = &[$(format!("[{}] = @p{}", stringify!($s), {i += 1; i}),)*].join(",");
            let w = &[$(format!("[{}] = @p{}", stringify!($w), {i += 1; i}),)*].join(" AND ");

            format!(
                "UPDATE [{t}] SET {s} WHERE {w};",
                t = stringify!($t),
                s = s,
                w = w,
            )
        }
    };
    (_body $command:ident { delete from $t:ident where $(($tf:ident = $ef:expr)) and+ }) => {
        {
            ::lazy_static::lazy_static! {
                static ref SQL: String = sql_query!(delete from $t where ($($tf),+));
            }

            let params = &[$(&$ef as &$crate::ToParameter,)*];
            let query = $crate::Query { sql: SQL.as_str(), params };
            $command.execute(query)
        }
    };
    (_body $command:ident { insert into $t:ident ($($tf:ident=$ef:expr),+$(,)*) }) => {
        {
            ::lazy_static::lazy_static! {
                static ref SQL: String = sql_query!(insert into $t ($($tf),+));
            }

            let params = &[$(&$ef as &$crate::ToParameter,)*];
            let query = $crate::Query { sql: SQL.as_str(), params };
            $command.execute(query)
        }
    };
    (_body $command:ident { merge into $t:ident set ($($tf:ident = $ef:expr),+$(,)*) where $(($tk:ident = $ek:expr)) and+ }) => {
        {
            ::lazy_static::lazy_static! {
                static ref SQL: String = sql_query!(merge into $t set ($($tf),+) where ($($tk),+));
            }

            let params = &[$(&$ek as &$crate::ToParameter,)* $(&$ef as &$crate::ToParameter,)*];
            let query = $crate::Query { sql: SQL.as_str(), params };
            $command.execute(query)
        }
    };
    (_body $command:ident $o:ident { select ($($ts:ident as $fs:ident),+$(,)*) from $t:ident }) => {
        {
            sql_query! { _body $command $o { select ($($ts as $fs,)+) from $t others {  } } }
        }
    };
    (_body $command:ident $o:ident { select ($($ts:ident as $fs:ident),+$(,)*) from $t:ident others { $($fo:ident: $xo:expr),*$(,)*}  }) => {
        {
            ::lazy_static::lazy_static! {
                static ref SQL: String = sql_query!(select ($($ts),+) from $t);
            }

            #[allow(unused_assignments, clippy::eval_order_dependence)]
            fn from_row(row: &$crate::Row) -> Result<$o, ::failure::Error> {
                let mut i = 0;
                Ok($o {
                    $($fs: row.get({let a = i; i += 1; a})?,)*
                    $($fo: $xo,)*
                })
            }

            $command.query_with(SQL.as_str(), from_row)
        }
    };
    (_body $command:ident $o:ident { select ($($ts:ident as $fs:ident),+$(,)*) from $t:ident where $(($tw:ident = $ew:ident)) and+ }) => {
        sql_query! { _body $command $o { select ($($ts as $fs,)+) from $t where $(($tw = $ew)) and+ others {  } } }
    };
    (_body $command:ident $o:ident { select ($($ts:ident as $fs:ident),+$(,)*) from $t:ident where $(($tw:ident = $ew:ident)) and+ others { $($fo:ident: $xo:expr),*$(,)*}  }) => {
        {
            ::lazy_static::lazy_static! {
                static ref SQL: String = sql_query!(select ($($ts),+) from $t where ($($tw),+) );
            }

            #[allow(unused_assignments,clippy::eval_order_dependence)]
            fn from_row(row: &$crate::Row) -> Result<$o, ::failure::Error> {
                let mut i = 0;
                Ok($o {
                    $($fs: row.get({let a = i; i += 1; a})?,)*
                    $($fo: $xo,)*
                })
            }

            let params = &[$(&$ew as &$crate::ToParameter,)*];
            let query = $crate::Query { sql: SQL.as_str(), params };
            $command.query_with(query, from_row)
        }
    };
    (_body $command:ident { update $t:ident set ($($ts:ident=$es:expr),+$(,)*) where $(($tw:ident = $ew:expr)) and+ }) => {
        {
            ::lazy_static::lazy_static! {
                static ref SQL: String = sql_query!(update $t set ($($ts),+) where ($($tw),+));
            }

            let params = &[$(&$es as &$crate::ToParameter,)* $(&$ew as &$crate::ToParameter,)*];
            let query = $crate::Query { sql: SQL.as_str(), params };
            $command.execute(query)
        }
    };
    (fn $fn:ident($($row:ident: $e:ty),+$(,)*) { $($statement:tt)+ }) => {
        fn $fn<C: $crate::Command>(command: C, $($row: $e,)+) -> Box<dyn ::futures::Future<Item = C, Error = ::failure::Error>> {
            sql_query!(_body command { $($statement)+ })
        }
    };
    (pub fn $fn:ident($($row:ident: $e:ty),+$(,)*) { $($statement:tt)+ }) => {
        pub fn $fn<C: $crate::Command>(command: C, $($row: $e),+) -> Box<dyn ::futures::Future<Item = C, ::failure::Error>> {
            sql_query!(_body command { $($statement)+ })
        }
    };
    (fn $fn:ident() -> $o:ident {
        select (
            $($ts:ident as $fs:ident: $ft:ty),+$(,)*
        ) from $table:ident
    }) => {
        struct $o {
            $($fs: $ft,)*
        }

        fn $fn<C: $crate::Command>(command: C) -> Box<dyn ::futures::Future<Item = (C, Vec<$o>), Error = ::failure::Error>> {
            sql_query!(_body command $o { select ($($ts as $fs),+) from $table others { } })
        }
    };
    (fn $fn:ident($($row:ident: $e:ty),*$(,)*) -> $o:ident {
        select (
            $($ts:ident as $fs:ident: $ft:ty),+$(,)*
        ) from $table:ident
        where $(($tw:ident = $ew:ident)) and+
    }) => {
        struct $o {
            $($fs: $ft,)*
        }

        fn $fn<C: $crate::Command>(command: C, $($row: $e,)*) -> Box<dyn ::futures::Future<Item = (C, Vec<$o>), Error = ::failure::Error>> {
            sql_query!(_body command $o { select ($($ts as $fs),+) from $table where $(($tw = $ew)) and+ others { } })
        }
    };
    (fn $fn:ident($($row:ident: $e:ty),*$(,)*) -> $o:ident { $($statement:tt)+ }) => {
        fn $fn<C: $crate::Command>(command: C, $($row: $e,)*) -> Box<dyn ::futures::Future<Item = (C, Vec<$o>), Error = ::failure::Error>> {
            sql_query!(_body command $o { $($statement)+ })
        }
    };
    (pub fn $fn:ident() -> $o:ident {
        select (
            $($ts:ident as $fs:ident: $ft:ty),+$(,)*
        ) from $table:ident
    }) => {
        pub struct $o {
            pub $($fs: $ft,)*
        }

        pub fn $fn<C: $crate::Command>(command: C, $($row: $e,)*) -> Box<dyn ::futures::Future<Item = (C, Vec<$o>), Error = ::failure::Error>> {
            sql_query!(_body command $o { select ($($ts as $fs),+) from $table others { } })
        }
    };
    (pub fn $fn:ident($($row:ident: $e:ty),*$(,)*) -> $o:ident {
        select (
            $($ts:ident as $fs:ident: $ft:ty),+$(,)*
        ) from $table:ident
        where $(($tw:ident = $ew:ident)) and+
    }) => {
        pub struct $o {
            pub $($fs: $ft,)*
        }

        pub fn $fn<C: $crate::Command>(command: C, $($row: $e,)*) -> Box<dyn ::futures::Future<Item = (C, Vec<$o>), Error = ::failure::Error>> {
            sql_query!(_body command $o {
                select (
                    $($ts as $fs),+
                )
                from $table
                where $(($tw = $ew)) and+
                others { }
             })
        }
    };
    (pub fn $fn:ident($($row:ident: $e:ty),*$(,)*) -> $o:ident { $($statement:tt)+ }) => {
        pub fn $fn<C: $crate::Command>(command: C, $($row: $e),*) -> Box<dyn ::futures::Future<Item = (C, Vec<$o>), Error = ::failure::Error>> {
            sql_query!(_body command $o { $($statement)+ })
        }
    };
}

#[cfg(test)]
fn _compile_tests() {
    struct A {
        id: i32,
    }

    struct B {
        id: i32,
        name: String,
    }

    let _ = sql_query!(select (id) from MyTable);
    let _ = sql_query!(select (id, name) from MyTable);
    let _ = sql_query!(select (id, name,) from MyTable);
    let _ = sql_query!(select (id) from MyTable where (id));
    let _ = sql_query!(select (id, name) from MyTable where (id, name));
    let _ = sql_query!(select (id, name,) from MyTable where (id, name,));

    let _ = sql_query!(delete from MyTable where (name));
    let _ = sql_query!(delete from MyTable where (id, name));
    let _ = sql_query!(delete from MyTable where (id, name,));

    let _ = sql_query!(insert into MyTable (id));
    let _ = sql_query!(insert into MyTable (id,name));
    let _ = sql_query!(insert into MyTable (id,name,));

    let _ = sql_query!(merge into MyTable set (name) where (id));
    let _ = sql_query!(merge into MyTable set (id, name) where (id,name));
    let _ = sql_query!(merge into MyTable set (id, name,) where (id,name,));

    let _ = sql_query!(update MyTable set (name) where (id));
    let _ = sql_query!(update MyTable set (id, name) where (id, name));
    let _ = sql_query!(update MyTable set (id, name,) where (id, name,));

    sql_query! {
        fn select1_a() -> A {
            select (Id as id) from MyTable
        }
    }

    sql_query! {
        fn select1_b() -> B {
            select (Id as id, Name as name) from MyTable
        }
    }

    sql_query! {
        fn select1_c() -> B {
            select (Id as id, Name as name,) from MyTable
        }
    }

    sql_query! {
        fn select1_d() -> Select1d {
            select (Id as id: i32, Name as name: String,) from MyTable
        }
    }

    sql_query! {
        fn select2_a(id: i32, name: &str) -> A {
            select (Id as id,) from MyTable where (Id = id) and (Name = name)
        }
    }

    sql_query! {
        fn select2_b(id: i32, name: &str) -> B {
            select (Id as id, Name as name) from MyTable where (Id = id) and (Name = name)
        }
    }

    sql_query! {
        fn select2_c(id: i32, name: &str,) -> B {
            select (Id as id, Name as name,) from MyTable where (Id = id) and (Name = name)
        }
    }

    sql_query! {
        fn select2_d(id: i32, name: &str,) -> Select2d {
            select (Id as id: i32, Name as name: String,) from MyTable where (Id = id) and (Name = name)
        }
    }

    sql_query! {
        fn delete1(id: i32) {
            delete from MyTable where (Id = id)
        }
    }

    sql_query! {
        fn delete2(id: i32, v: i32) {
            delete from MyTable where (Id = id) and (Id = v)
        }
    }

    sql_query! {
        fn delete3(id: i32, v: i32,) {
            delete from MyTable where (Id = id) and (Id = v)
        }
    }

    sql_query! {
        fn insert1(id: i32) {
            insert into MyTable (Id = id)
        }
    }

    sql_query! {
        fn insert2(id: i32, name: String) {
            insert into MyTable (Id = id, Name = name)
        }
    }

    sql_query! {
        fn insert3(id: i32, name: String,) {
            insert into MyTable (Id = id, Name = name,)
        }
    }

    sql_query! {
        fn merge1(id: i32) {
            merge into MyTable set (Id = id) where (Id = id)
        }
    }

    sql_query! {
        fn merge2(id: i32, name: &str) {
            merge into MyTable set (Id = id, Name = name) where (Id = id) and (Name = name)
        }
    }

    sql_query! {
        fn merge3(id: i32, name: &str,) {
            merge into MyTable set (Id = id, Name = name,) where (Id = id) and (Name = name)
        }
    }

    sql_query! {
        fn update1(id: i32) {
            update MyTable set (Id = id) where (Id = id)
        }
    }

    sql_query! {
        fn update2(id: i32, name: String) {
            update MyTable set (Id = id, Name = name) where (Id = id) and (Name = name)
        }
    }

    sql_query! {
        fn update3(id: i32, name: String,) {
            update MyTable set (Id = id, Name = name,) where (Id = id) and (Name = name)
        }
    }
}
