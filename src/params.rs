use std::borrow::Cow;

use crate::Parameter;
use chrono::{NaiveDate, NaiveDateTime};
use decimal::Decimal;
use uuid::Uuid;

/// Convert a type into an iterator of sql params for use with sql queries.
pub trait Params<'a> {
    fn params(self, out: &mut Vec<Parameter<'a>>);
    fn params_null(vec: &mut Vec<Parameter<'a>>);
}

impl<'a, T> Params<'a> for &T
where
    T: Params<'a> + Clone,
{
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        T::params(self.clone(), out)
    }

    fn params_null(vec: &mut Vec<Parameter<'a>>) {
        T::params_null(vec)
    }
}

impl<'a, T> Params<'a> for Option<T>
where
    T: Params<'a>,
{
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        match self {
            Some(v) => v.params(out),
            None => T::params_null(out),
        }
    }

    fn params_null(vec: &mut Vec<Parameter<'a>>) {
        T::params_null(vec)
    }
}

impl<'a, T> Params<'a> for std::collections::HashSet<T>
where
    T: Params<'a>,
{
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        self.into_iter().for_each(|v| v.params(out))
    }

    fn params_null(_: &mut Vec<Parameter<'a>>) {}
}

impl<'a, T> Params<'a> for Vec<T>
where
    T: Params<'a>,
{
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        self.into_iter().for_each(|v| v.params(out))
    }

    fn params_null(_: &mut Vec<Parameter<'a>>) {}
}

impl<'a> Params<'a> for Parameter<'a> {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(self)
    }

    fn params_null(_: &mut Vec<Parameter<'a>>) {}
}

impl<'a> Params<'a> for bool {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::Bool(Some(self)))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::Bool(None))
    }
}

impl<'a> Params<'a> for Decimal {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::F64(Some(self.into())))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::F64(None))
    }
}

impl<'a> Params<'a> for f32 {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::F32(Some(self)))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::F32(None))
    }
}

impl<'a> Params<'a> for f64 {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::F64(Some(self)))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::F64(None))
    }
}

impl<'a> Params<'a> for i16 {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::I16(Some(self)))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::I16(None))
    }
}

impl<'a> Params<'a> for i32 {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::I32(Some(self)))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::I32(None))
    }
}

impl<'a> Params<'a> for i64 {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::I64(Some(self)))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::I64(None))
    }
}

impl<'a> Params<'a> for NaiveDate {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::Date(Some(self)))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::Date(None))
    }
}

impl<'a> Params<'a> for NaiveDateTime {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::DateTime(Some(self)))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::DateTime(None))
    }
}

impl<'a> Params<'a> for String {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::String(Some(Cow::Owned(self))))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::String(None))
    }
}

impl<'a> Params<'a> for &'a str {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::String(Some(Cow::Borrowed(self))))
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::String(None))
    }
}

impl<'a> Params<'a> for Uuid {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(self.into())
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::Uuid(None))
    }
}

impl<'a> Params<'a> for () {
    fn params(self, _: &mut Vec<Parameter<'a>>) {}
    fn params_null(_: &mut Vec<Parameter<'a>>) {}
}

impl<'a, A: Params<'a>> Params<'a> for (A,) {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        self.0.params(out);
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        A::params_null(out);
    }
}

impl<'a, A: Params<'a>, B: Params<'a>> Params<'a> for (A, B) {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        self.0.params(out);
        self.1.params(out);
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        A::params_null(out);
        B::params_null(out);
    }
}

impl<'a, A: Params<'a>, B: Params<'a>, C: Params<'a>> Params<'a> for (A, B, C) {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        self.0.params(out);
        self.1.params(out);
        self.2.params(out);
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        A::params_null(out);
        B::params_null(out);
        C::params_null(out);
    }
}

impl<'a, A: Params<'a>, B: Params<'a>, C: Params<'a>, D: Params<'a>> Params<'a> for (A, B, C, D) {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        self.0.params(out);
        self.1.params(out);
        self.2.params(out);
        self.3.params(out);
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        A::params_null(out);
        B::params_null(out);
        C::params_null(out);
        D::params_null(out);
    }
}

impl<'a, A: Params<'a>, B: Params<'a>, C: Params<'a>, D: Params<'a>, E: Params<'a>> Params<'a>
    for (A, B, C, D, E)
{
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        self.0.params(out);
        self.1.params(out);
        self.2.params(out);
        self.3.params(out);
        self.4.params(out);
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        A::params_null(out);
        B::params_null(out);
        C::params_null(out);
        D::params_null(out);
        E::params_null(out);
    }
}

#[test]
fn check_compile() {
    fn execute<'a, P>(p: P)
    where
        P: Params<'a>,
    {
        let mut v = Vec::new();
        p.params(&mut v);
    }

    execute(10);
    execute("test");
    execute("test2".to_owned());
    execute(&"test3".to_owned());
    execute(Uuid::nil());
    execute(&Uuid::nil());
    execute(vec![2, 3, 4]);
    execute(Some(10));
    execute((10, "test"));
    execute((10, "test", NaiveDate::from_ymd(2000, 1, 1)));
    execute((
        10,
        "test",
        NaiveDate::from_ymd(2000, 1, 1),
        NaiveDate::from_ymd(2000, 1, 1).and_hms(12, 10, 1),
    ));
}
