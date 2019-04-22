use chrono::{NaiveDate, NaiveDateTime};
use std::borrow::Cow;
use std::fmt::{Debug, Error as FmtError, Formatter};
use tiberius::ty::{Guid, ToSql};

pub enum Parameter<'a> {
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    F32(f32),
    F64(f64),
    I16(i16),
    I32(i32),
    I64(i64),
    Null,
    String(&'a str),
    StringCow(Cow<'a, str>),
    Uuid(Guid),
}

impl<'a> Debug for Parameter<'a> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        match self {
            Parameter::Bool(v) => write!(f, "{}", v),
            Parameter::Date(v) => write!(f, "{}", v),
            Parameter::DateTime(v) => write!(f, "{}", v),
            Parameter::F32(v) => write!(f, "{}", v),
            Parameter::F64(v) => write!(f, "{}", v),
            Parameter::I16(v) => write!(f, "{}", v),
            Parameter::I32(v) => write!(f, "{}", v),
            Parameter::I64(v) => write!(f, "{}", v),
            Parameter::Null => f.write_str("null"),
            Parameter::String(v) => f.write_str(v),
            Parameter::StringCow(v) => f.write_str(v.as_ref()),
            Parameter::Uuid(g) => write!(f, "{}", g),
        }
    }
}

impl<'a> From<&'a Parameter<'a>> for &'a ToSql {
    fn from(d: &'a Parameter<'a>) -> &'a ToSql {
        match d {
            Parameter::Bool(v) => v,
            Parameter::Date(v) => v,
            Parameter::DateTime(v) => v,
            Parameter::F32(v) => v,
            Parameter::F64(v) => v,
            Parameter::I16(v) => v,
            Parameter::I32(v) => v,
            Parameter::I64(v) => v,
            Parameter::Null => &NULL_I32,
            Parameter::String(v) => v,
            Parameter::StringCow(v) => v,
            Parameter::Uuid(v) => v,
        }
    }
}

const NULL_I32: Option<i32> = None;
