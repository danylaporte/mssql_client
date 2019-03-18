use chrono::{NaiveDate, NaiveDateTime};
use decimal::Decimal;
use failure::Error;
use std::borrow::Cow;
use std::fmt::{Debug, Error as FmtError, Formatter};
use tiberius::ty::{Guid, ToSql};
use uuid::Uuid;

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

pub trait ToParameter {
    fn to_parameter(&self) -> Result<Parameter, Error>;
}

pub trait ToParameters {
    fn to_parameters(&self) -> Vec<&ToParameter>;
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

impl<'a> From<Uuid> for Parameter<'a> {
    fn from(u: Uuid) -> Parameter<'a> {
        Parameter::Uuid(to_guid(&u))
    }
}

impl<'a, T> ToParameter for Option<T>
where
    T: ToParameter,
{
    fn to_parameter(&self) -> Result<Parameter, Error> {
        match self {
            Some(v) => v.to_parameter(),
            None => Ok(Parameter::Null),
        }
    }
}

impl<'a, T> ToParameter for &'a T
where
    T: ToParameter,
{
    fn to_parameter(&self) -> Result<Parameter, Error> {
        ToParameter::to_parameter(*self)
    }
}

macro_rules! to_parameter {
    ($target:ident { $($t:ty => $e:expr,)* }) => {
        $(impl<'a> ToParameter for $t {
            fn to_parameter(&self) -> Result<Parameter, Error> {
                let $target = self;
                Ok($e)
            }
        })*
    };
}

macro_rules! to_parameters {
    ($t:ty { $($i:tt: $v:tt,)* }) => {
        impl<$($v,)*> ToParameters for $t where $($v: ToParameter,)* {
            fn to_parameters(&self) -> Vec<&ToParameter> {
                vec![$(&self.$i,)*]
            }
        }
    };
}

fn to_guid(id: &Uuid) -> Guid {
    let b = id.as_bytes();
    Guid::from_bytes(&[
        b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15],
    ])
}

to_parameter! {
    self_ {
        &'a str => Parameter::String(self_),
        Decimal => Parameter::F64((*self_).into()),
        NaiveDate => Parameter::Date(*self_),
        NaiveDateTime => Parameter::DateTime(*self_),
        String => Parameter::String(self_.as_str()),
        Uuid => Parameter::Uuid(to_guid(self_)),
        bool => Parameter::Bool(*self_),
        f32 => Parameter::F32(*self_),
        f64 => Parameter::F64(*self_),
        i16 => Parameter::I16(*self_),
        i32 => Parameter::I32(*self_),
        i64 => Parameter::I64(*self_),
    }
}

impl<T> ToParameters for (T)
where
    T: ToParameter,
{
    fn to_parameters(&self) -> Vec<&ToParameter> {
        vec![self]
    }
}

to_parameters! { (A,B) { 0:A, 1:B, } }
to_parameters! { (A,B,C) { 0:A, 1:B, 2:C, } }
to_parameters! { (A,B,C,D) { 0:A, 1:B, 2:C, 3:D, } }
to_parameters! { (A,B,C,D,E) { 0:A, 1:B, 2:C, 3:D, 4:E, } }

const NULL_I32: Option<i32> = None;
