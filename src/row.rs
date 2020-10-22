use crate::{Error, FromColumn, Result, SqlValue};
use tiberius::query::QueryRow;

/// A row is a temporary struct that must be transformed into a
/// definitive struct using the [FromColumn](trait.FromColumn.html) trait.
///
/// Do no use directly.
pub struct Row(pub(crate) QueryRow);

impl Row {
    pub fn get<'a, R>(&'a self, idx: usize) -> Result<R>
    where
        R: FromColumn<'a>,
    {
        match <R::Value>::from_row(self, idx) {
            Ok(v) => R::from_column(v),
            Err(e) => Err(e),
        }
    }

    /// This is the same as `get` but in case of error, return the field_name.
    pub fn get_named_err<'a, R>(&'a self, idx: usize, field_name: &'static str) -> Result<R>
    where
        R: FromColumn<'a>,
    {
        match self.get(idx) {
            Ok(v) => Ok(v),
            Err(e) => Err(Error::FieldName(Box::new(e), field_name)),
        }
    }
}
