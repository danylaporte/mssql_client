use crate::{row::Row, Error, Result};
use chrono::{NaiveDate, NaiveDateTime};
use decimal::Decimal;
use tiberius::ty::{Guid, Numeric};
use uuid::Uuid;

/// This trait implement loading from a sql column into a primitive rust type.
pub trait SqlValue<'a>: private::Sealed {
    fn check_db_ty(v: &str) -> bool;
    fn is_nullable() -> bool;
    fn from_row(row: &'a Row, idx: usize) -> Result<Self>
    where
        Self: Sized;
}

macro_rules! sql_value {
    ($t:ty, $e:expr, $v:ident => $vv:expr) => {
        impl<'a> SqlValue<'a> for $t {
            fn check_db_ty($v: &str) -> bool {
                $vv
            }

            fn is_nullable() -> bool {
                false
            }

            fn from_row(row: &'a Row, idx: usize) -> Result<Self> {
                read(row.0.try_get(idx), idx).map($e)
            }
        }

        impl<'a> SqlValue<'a> for Option<$t> {
            fn check_db_ty(v: &str) -> bool {
                <$t>::check_db_ty(v)
            }

            fn is_nullable() -> bool {
                true
            }

            fn from_row(row: &'a Row, idx: usize) -> Result<Self> {
                read(row.0.try_get(idx), idx).map(|v: Option<_>| v.map($e))
            }
        }
    };
}

#[rustfmt::skip]
mod m {
    use super::*;

    sql_value!(&'a [u8], identity, v => v == "varbinary" || v == "binary" || v == "image");
    sql_value!(&'a str, identity, v => v == "nvarchar" || v == "varchar" || v == "ntext" || v == "text" || v == "nchar" || v == "char");
    sql_value!(Decimal, numeric_to_decimal, v => v == "decimal" || v == "numeric");
    sql_value!(NaiveDate, identity, v => v == "date");
    sql_value!(NaiveDateTime, identity, v => v == "datetime" || v == "datetime2" || v == "datetimeoffset");
    sql_value!(String, |v: &str| v.to_string(), v => <&str>::check_db_ty(v));
    sql_value!(Uuid, guid_to_uuid, v => v == "uniqueidentifier");
    sql_value!(Vec<u8>, |v: &[u8]| v.to_vec(), v => <&[u8]>::check_db_ty(v));
    sql_value!(bool, identity, v => v == "bit");
    sql_value!(f32, identity, v => v == "real" || v == "smallmoney");
    sql_value!(f64, identity, v => v == "float" || v == "money");
    sql_value!(i16, identity, v => v == "smallint");
    sql_value!(i32, identity, v => v == "int");
    sql_value!(i64, identity, v => v == "bigint");
    sql_value!(i8, identity, v => v == "tinyint");
}

mod private {
    use decimal::Decimal;
    use uuid::Uuid;

    pub trait Sealed {}
    impl Sealed for Decimal {}
    impl Sealed for String {}
    impl Sealed for Uuid {}
    impl Sealed for Vec<u8> {}
    impl Sealed for bool {}
    impl Sealed for chrono::NaiveDate {}
    impl Sealed for chrono::NaiveDateTime {}
    impl Sealed for f32 {}
    impl Sealed for f64 {}
    impl Sealed for i16 {}
    impl Sealed for i32 {}
    impl Sealed for i64 {}
    impl Sealed for i8 {}
    impl<'a> Sealed for &'a [u8] {}
    impl<'a> Sealed for &'a str {}
    impl<T> Sealed for Option<T> where T: Sealed {}
}

fn guid_to_uuid(g: &Guid) -> Uuid {
    let b = g.as_bytes();
    Uuid::from_bytes([
        b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15],
    ])
}

fn identity<T>(v: T) -> T {
    v
}

fn numeric_to_decimal(n: Numeric) -> Decimal {
    decimal::Decimal::new_with_scale(n.value(), n.scale())
}

fn read<R>(result: std::result::Result<Option<R>, tiberius::Error>, idx: usize) -> Result<R> {
    match result {
        Ok(Some(r)) => Ok(r),
        Ok(None) => Err(Error::FieldNotFound(idx)),
        Err(e) => Err(Error::TiberiusField(e, idx)),
    }
}
