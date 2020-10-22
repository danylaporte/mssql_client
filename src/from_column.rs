use crate::{Result, SqlValue};

/// This trait convert a sql column value into a rust type.
/// Implement this trait to be able to support more types as needed.
///
/// ```
/// use mssql_client::{FromColumn, Result};
///
/// enum Code {
///     A,
///     B,
/// }
///
/// impl<'a> FromColumn<'a> for Code {
///     type Value = &'a str;
///
///     fn from_column(v: Self::Value) -> Result<Self> {
///         Ok(match v {
///             "A" => Code::A,
///             "B" => Code::B,
///             _ => return Err("invalid code".into()),
///         })
///     }
/// }
/// ```
pub trait FromColumn<'a>: Sized {
    type Value: SqlValue<'a>;
    fn from_column(v: Self::Value) -> Result<Self>;
}

impl<'a, T> FromColumn<'a> for Option<T>
where
    T: FromColumn<'a>,
    Option<T::Value>: SqlValue<'a>,
{
    type Value = Option<T::Value>;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(match v {
            Some(v) => Some(T::from_column(v)?),
            None => None,
        })
    }
}

impl<'a> FromColumn<'a> for decimal::Decimal {
    type Value = decimal::Decimal;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for String {
    type Value = String;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for uuid::Uuid {
    type Value = uuid::Uuid;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for Vec<u8> {
    type Value = Vec<u8>;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for bool {
    type Value = bool;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for chrono::NaiveDate {
    type Value = chrono::NaiveDate;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for chrono::NaiveDateTime {
    type Value = chrono::NaiveDateTime;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for f32 {
    type Value = f32;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for f64 {
    type Value = f64;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for i16 {
    type Value = i16;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for i32 {
    type Value = i32;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for i64 {
    type Value = i64;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for i8 {
    type Value = i8;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for &'a [u8] {
    type Value = &'a [u8];

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}

impl<'a> FromColumn<'a> for &'a str {
    type Value = &'a str;

    fn from_column(v: Self::Value) -> Result<Self> {
        Ok(v)
    }
}
