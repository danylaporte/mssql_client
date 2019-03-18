use crate::{FromColumn, Row};
use failure::Error;

/// Takes a [Row](struct.Row.html) and convert it into a type.
pub trait FromRow {
    fn from_row(row: &Row) -> Result<Self, Error>
    where
        Self: Sized;
}

impl<A> FromRow for (A)
where
    A: for<'a> FromColumn<'a>,
{
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok(row.get(0)?)
    }
}

impl<A, B> FromRow for (A, B)
where
    A: for<'a> FromColumn<'a>,
    B: for<'a> FromColumn<'a>,
{
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok((row.get(0)?, row.get(1)?))
    }
}

impl<A, B, C> FromRow for (A, B, C)
where
    A: for<'a> FromColumn<'a>,
    B: for<'a> FromColumn<'a>,
    C: for<'a> FromColumn<'a>,
{
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    }
}

impl<A, B, C, D> FromRow for (A, B, C, D)
where
    A: for<'a> FromColumn<'a>,
    B: for<'a> FromColumn<'a>,
    C: for<'a> FromColumn<'a>,
    D: for<'a> FromColumn<'a>,
{
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    }
}

impl<A, B, C, D, E> FromRow for (A, B, C, D, E)
where
    A: for<'a> FromColumn<'a>,
    B: for<'a> FromColumn<'a>,
    C: for<'a> FromColumn<'a>,
    D: for<'a> FromColumn<'a>,
    E: for<'a> FromColumn<'a>,
{
    fn from_row(row: &Row) -> Result<Self, Error> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
        ))
    }
}
