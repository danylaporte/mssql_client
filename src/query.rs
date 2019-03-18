use crate::ToParameter;

pub struct Query<'a> {
    pub params: &'a [&'a ToParameter],
    pub sql: &'a str,
}

impl<'a> Query<'a> {
    pub fn new<S>(sql: S) -> Self
    where
        S: Into<&'a str>,
    {
        Query {
            params: &[],
            sql: sql.into(),
        }
    }
}

impl<'a> From<&'a str> for Query<'a> {
    fn from(s: &'a str) -> Self {
        Query::new(s)
    }
}
