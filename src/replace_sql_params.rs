#[doc(hidden)]
#[macro_export]
macro_rules! replace_sql_params {
    ($($field:ident,)* { $sql:expr }) => {
        {
            let q = $sql;
            let mut i = 1;

            $(
                let r = format!("\\B@{}\\b", stringify!($field));
                let q = $crate::regex::Regex::new(&r).unwrap().replace_all(&q[..], format!("@P{}", i).as_str());
                #[allow(unused_assignments)]
                {
                    i += 1;
                }
            )*

            q.to_string()
        }
    }
}
