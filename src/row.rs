use crate::{FromColumn, SqlValue};
use failure::Error;
use tiberius::query::QueryRow;

/// A row is a temporary struct that must be transformed into a
/// definitive struct using the [FromColumn](trait.FromColumn.html) trait.
///
/// Do no use directly.
pub struct Row(pub(crate) QueryRow);

impl Row {
    pub fn get<'a, R>(&'a self, idx: usize) -> Result<R, Error>
    where
        R: FromColumn<'a>,
    {
        R::from_column(<R::Value>::from_row(self, idx)?)
    }
}
