use crate::Parameter;
use chrono::{NaiveDate, NaiveDateTime};
use decimal::Decimal;
use uuid::Uuid;

/// Convert a type into an iterator of sql params for use with sql queries.
pub trait Params<'a> {
    fn params(self, out: &mut Vec<Parameter<'a>>);
    fn params_null(vec: &mut Vec<Parameter<'a>>);
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

macro_rules! params {
    (tuple $($t:ident:$n:tt),+$(,)?) => {
        impl<'a, $($t),+> Params<'a> for ($($t,)+)
        where $($t: Params<'a>),+
        {
            fn params(self, out: &mut Vec<Parameter<'a>>) {
                $(
                    self.$n.params(out);
                )+
            }

            fn params_null(out: &mut Vec<Parameter<'a>>) {
                $(
                    $t::params_null(out);
                )+
            }
        }
    };
    (deref $t:ty, $n:ident) => {
        impl<'a> Params<'a> for $t {
            fn params(self, out: &mut Vec<Parameter<'a>>) {
                out.push(Parameter::$n(Some(self.into())))
            }

            fn params_null(out: &mut Vec<Parameter<'a>>) {
                out.push(Parameter::$n(None))
            }
        }

        impl<'a> Params<'a> for &$t {
            fn params(self, out: &mut Vec<Parameter<'a>>) {
                (*self).params(out)
            }

            fn params_null(out: &mut Vec<Parameter<'a>>) {
                <$t>::params_null(out)
            }
        }
    };
    ($t:ty, $n:ident) => {
        impl<'a> Params<'a> for $t {
            fn params(self, out: &mut Vec<Parameter<'a>>) {
                out.push(Parameter::$n(Some(self.into())))
            }

            fn params_null(out: &mut Vec<Parameter<'a>>) {
                out.push(Parameter::$n(None))
            }
        }
    };
}

params!(tuple A:0,);
params!(tuple A:0,B:1,);
params!(tuple A:0,B:1,C:2,);
params!(tuple A:0,B:1,C:2,D:3,);
params!(tuple A:0,B:1,C:2,D:3,E:4,);
params!(tuple A:0,B:1,C:2,D:3,E:4,F:5,);
params!(tuple A:0,B:1,C:2,D:3,E:4,F:5,G:6,);
params!(tuple A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,);
params!(tuple A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,);
params!(tuple A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,);

params!(&'a String, String);
params!(&'a str, String);
params!(String, String);
params!(deref Decimal, F64);
params!(deref NaiveDate, Date);
params!(deref NaiveDateTime, DateTime);
params!(deref bool, Bool);
params!(deref f32, F32);
params!(deref f64, F64);
params!(deref i16, I16);
params!(deref i32, I32);
params!(deref i64, I64);

impl<'a> Params<'a> for Uuid {
    fn params(self, out: &mut Vec<Parameter<'a>>) {
        out.push(self.into())
    }

    fn params_null(out: &mut Vec<Parameter<'a>>) {
        out.push(Parameter::Uuid(None))
    }
}

impl<'a> Params<'a> for &Uuid {
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
