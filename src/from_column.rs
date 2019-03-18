use crate::Row;
use chrono::{NaiveDate, NaiveDateTime};
use decimal::Decimal;
use failure::{bail, Error};
use tiberius::ty::{Guid, Numeric};
use uuid::Uuid;

/// This trait take a [Row](struct.Row.html) and a column index and
/// transform into a real type.
///
/// Implement this trait to be able to support more types as needed.
pub trait FromColumn<'a> {
    fn from_column(row: &'a Row, idx: usize) -> Result<Self, Error>
    where
        Self: Sized;
}

/// This type is the counterpart of the [FromColumn](trait.FromColumn.html) trait.
/// It allows to support optional type.
pub trait FromColumnOpt<'a> {
    fn from_column_opt(row: &'a Row, idx: usize) -> Result<Option<Self>, Error>
    where
        Self: Sized;
}

macro_rules! from_column {
    (body $row:ident($idx:ident)) => {
        match $row.0.try_get($idx) {
            Ok(Some(v)) => v,
            Ok(None) => bail!("Field {} of out range {}.", $idx, $row.0.len()),
            Err(e) => bail!("Conversion of field {} is invalid. {:?}", $idx, e),
        }
    };
    ($($t:ty,)*) => {
        $(
            impl<'a> FromColumn<'a> for $t {
                fn from_column(row: &'a Row, idx: usize) -> Result<Self, Error> {
                    Ok(from_column!(body row(idx)))
                }
            }

            impl<'a> FromColumnOpt<'a> for $t {
                fn from_column_opt(row: &'a Row, idx: usize) -> Result<Option<Self>, Error> {
                    Ok(from_column!(body row(idx)))
                }
            }
        )*
    }
}

from_column! {
    NaiveDate,
    NaiveDateTime,
    bool,
    f32,
    f64,
    i16,
    i32,
    i64,
}

impl<'a, T> FromColumn<'a> for Option<T>
where
    T: FromColumnOpt<'a>,
{
    fn from_column(row: &'a Row, idx: usize) -> Result<Option<T>, Error> {
        FromColumnOpt::from_column_opt(row, idx)
    }
}

impl<'a> FromColumn<'a> for &'a str {
    fn from_column(row: &'a Row, idx: usize) -> Result<Self, Error> {
        Ok(from_column!(body row(idx)))
    }
}

impl<'a> FromColumnOpt<'a> for &'a str {
    fn from_column_opt(row: &'a Row, idx: usize) -> Result<Option<Self>, Error> {
        let s: Option<&str> = from_column!(body row(idx));
        Ok(match s {
            Some(s) if s.is_empty() => None,
            Some(s) => Some(s),
            None => None,
        })
    }
}

impl<'a> FromColumn<'a> for String {
    fn from_column(row: &'a Row, idx: usize) -> Result<Self, Error> {
        Ok(<&str>::from_column(row, idx)?.to_owned())
    }
}

impl<'a> FromColumnOpt<'a> for String {
    fn from_column_opt(row: &'a Row, idx: usize) -> Result<Option<Self>, Error> {
        Ok(FromColumnOpt::from_column_opt(row, idx)?.map(std::borrow::ToOwned::to_owned))
    }
}

impl<'a> FromColumn<'a> for Uuid {
    fn from_column(row: &'a Row, idx: usize) -> Result<Self, Error> {
        Ok(to_uuid(from_column!(body row(idx))))
    }
}

impl<'a> FromColumnOpt<'a> for Uuid {
    fn from_column_opt(row: &'a Row, idx: usize) -> Result<Option<Uuid>, Error> {
        let v: Option<&Guid> = from_column!(body row(idx));
        Ok(v.map(to_uuid))
    }
}

impl<'a> FromColumn<'a> for Decimal {
    fn from_column(row: &'a Row, idx: usize) -> Result<Self, Error> {
        let n: Numeric = from_column!(body row(idx));
        Ok(Decimal::new_with_scale(n.value() as i64, n.scale()))
    }
}

impl<'a> FromColumnOpt<'a> for Decimal {
    fn from_column_opt(row: &'a Row, idx: usize) -> Result<Option<Self>, Error> {
        let n: Option<Numeric> = from_column!(body row(idx));
        Ok(n.map(|n| Decimal::new_with_scale(n.value() as i64, n.scale())))
    }
}

fn to_uuid(g: &Guid) -> Uuid {
    let b = g.as_bytes();
    Uuid::from_bytes([
        b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15],
    ])
}
