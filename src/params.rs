use crate::Parameter;
use chrono::{NaiveDate, NaiveDateTime};
use decimal::Decimal;
use std::borrow::Cow;
use tiberius::ty::Guid;
use uuid::Uuid;

/// Convert a type into an iterator of sql params for use with sql queries.
pub trait Params<'a> {
    fn params(self, out: &mut Vec<Parameter<'a>>);
}

macro_rules! params {
    (ref $t:ty, $p:ident) => {
        impl<'a> Params<'a> for &$t {
            #[inline]
            fn params(self, out: &mut Vec<Parameter<'a>>) {
                out.push(Parameter::$p(*self));
            }
        }
    };
    (tuple $($i:ident,)+) => {
        impl<'a, $($i,)+> Params<'a> for ($($i,)+)
        where
            $($i: Params<'a>,)+
        {
            #[allow(non_snake_case)]
            #[inline]
            fn params(self, out: &mut Vec<Parameter<'a>>) {
                let ($($i,)+) = self;
                $($i.params(out);)+
            }
        }
    };

    ($t:ty, $p:ident) => {
        impl<'a> Params<'a> for $t {
            #[inline]
            fn params(self, out: &mut Vec<Parameter<'a>>) {
                out.push(Parameter::$p(self));
            }
        }
    };
}

params!(&'a String, String);
params!(&'a str, String);
params!(Cow<'a, str>, StringCow);
params!(NaiveDate, Date);
params!(NaiveDateTime, DateTime);
params!(bool, Bool);
params!(f32, F32);
params!(f64, F64);
params!(i16, I16);
params!(i32, I32);
params!(i64, I64);
params!(ref &'a String, String);
params!(ref &'a str, String);
params!(ref NaiveDate, Date);
params!(ref NaiveDateTime, DateTime);
params!(ref bool, Bool);
params!(ref f32, F32);
params!(ref f64, F64);
params!(ref i16, I16);
params!(ref i32, I32);
params!(ref i64, I64);

params!(tuple A,);
params!(tuple A,B,);
params!(tuple A,B,C,);
params!(tuple A,B,C,D,);
params!(tuple A,B,C,D,E,);
params!(tuple A,B,C,D,E,F,);
params!(tuple A,B,C,D,E,F,G,);
params!(tuple A,B,C,D,E,F,G,H,);
params!(tuple A,B,C,D,E,F,G,H,I,);
params!(tuple A,B,C,D,E,F,G,H,I,J,);

impl<'a> Params<'a> for Decimal {
    #[inline]
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::F64(self.into()));
    }
}

impl<'a> Params<'a> for &Decimal {
    #[inline]
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        (*self).params(out)
    }
}

impl<'a, T> Params<'a> for Option<T>
where
    T: Params<'a>,
{
    #[inline]
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        match self {
            Some(v) => v.params(out),
            None => out.push(Parameter::Null),
        }
    }
}

impl<'a, T> Params<'a> for &'a Option<T>
where
    &'a T: Params<'a>,
{
    #[inline]
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        match self {
            Some(v) => v.params(out),
            None => out.push(Parameter::Null),
        }
    }
}

impl<'a> Params<'a> for Parameter<'a> {
    #[inline]
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(self);
    }
}

impl<'a> Params<'a> for String {
    #[inline]
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::StringCow(Cow::Owned(self)));
    }
}

impl<'a> Params<'a> for Uuid {
    #[inline]
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        (&self).params(out)
    }
}

impl<'a> Params<'a> for &Uuid {
    #[inline]
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        let b = self.as_bytes();
        out.push(Parameter::Uuid(Guid::from_bytes(&[
            b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13],
            b[14], b[15],
        ])));
    }
}

impl<'a, T> Params<'a> for Vec<T>
where
    T: Params<'a>,
{
    #[inline]
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        for item in self {
            item.params(out);
        }
    }
}

impl<'a> Params<'a> for () {
    #[inline]
    fn params(self, _: &mut Vec<Parameter<'a>>) {}
}

#[test]
fn check_compile() {
    fn execute<'a, P>(p: P)
    where
        P: Params<'a> + 'a,
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
