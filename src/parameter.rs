use chrono::{NaiveDate, NaiveDateTime};
use std::borrow::Cow;
use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use tiberius::ty::{Guid, ToSql};
use uuid::Uuid;

pub enum Parameter<'a> {
    Bool(Option<bool>),
    Date(Option<NaiveDate>),
    DateTime(Option<NaiveDateTime>),
    F32(Option<f32>),
    F64(Option<f64>),
    I16(Option<i16>),
    I32(Option<i32>),
    I64(Option<i64>),
    String(Option<Cow<'a, str>>),
    Uuid(Option<Guid>),
}

impl<'a> Debug for Parameter<'a> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        fn write<T: Display>(f: &mut Formatter, v: &Option<T>) -> Result<(), FmtError> {
            match v {
                Some(v) => write!(f, "{}", v),
                None => f.write_str("null"),
            }
        }
        match self {
            Parameter::Bool(v) => write(f, v),
            Parameter::Date(v) => write(f, v),
            Parameter::DateTime(v) => write(f, v),
            Parameter::F32(v) => write(f, v),
            Parameter::F64(v) => write(f, v),
            Parameter::I16(v) => write(f, v),
            Parameter::I32(v) => write(f, v),
            Parameter::I64(v) => write(f, v),
            Parameter::String(v) => write(f, v),
            Parameter::Uuid(g) => write(f, g),
        }
    }
}

impl<'a> From<&'a Parameter<'a>> for &'a dyn ToSql {
    fn from(d: &'a Parameter<'a>) -> &'a dyn ToSql {
        match d {
            Parameter::Bool(v) => v,
            Parameter::Date(v) => v,
            Parameter::DateTime(v) => v,
            Parameter::F32(v) => v,
            Parameter::F64(v) => v,
            Parameter::I16(v) => v,
            Parameter::I32(v) => v,
            Parameter::I64(v) => v,
            Parameter::String(v) => v,
            Parameter::Uuid(v) => v,
        }
    }
}

impl<'a, 'b> From<&'b Uuid> for Parameter<'a> {
    fn from(id: &'b Uuid) -> Self {
        let b = id.as_bytes();

        Parameter::Uuid(Some(Guid::from_bytes(&[
            b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13],
            b[14], b[15],
        ])))
    }
}

impl<'a> From<Uuid> for Parameter<'a> {
    fn from(id: Uuid) -> Self {
        (&id).into()
    }
}
