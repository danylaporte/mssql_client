use std::fmt;

#[derive(Debug)]
pub enum Error {
    Box(Box<dyn std::error::Error>),
    ConnStr(conn_str::Error),
    DataSourceNotSpecified,
    FieldName(Box<dyn std::error::Error>, &'static str),
    FieldNotFound(usize),
    HostNotFound(String),
    Io(std::io::Error),
    Tiberius(tiberius::Error),
    TiberiusField(tiberius::Error, usize),
    Str(&'static str),
    String(String),
    Var(std::env::VarError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::Box(e) => e.fmt(f),
            Self::ConnStr(e) => e.fmt(f),
            Self::DataSourceNotSpecified => {
                f.write_str("Data source / server not specified in connection string.")
            }
            Self::FieldName(e, n) => write!(f, "{}, field: `{}`", e, n),
            Self::FieldNotFound(i) => write!(f, "FieldIndex: `{}` not found.", i),
            Self::HostNotFound(s) => write!(f, "Host `{}` not found", s),
            Self::Io(e) => e.fmt(f),
            Self::Str(e) => e.fmt(f),
            Self::String(e) => e.fmt(f),
            Self::Tiberius(e) => write!(f, "{:?}", e),
            Self::TiberiusField(e, i) => write!(f, "{:?}, Field index `{}`", e, i),
            Self::Var(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<Box<dyn std::error::Error + 'static>> for Error {
    fn from(e: Box<dyn std::error::Error + 'static>) -> Self {
        Self::Box(e)
    }
}

impl<E> From<Box<E>> for Error
where
    E: std::error::Error + 'static,
{
    fn from(e: Box<E>) -> Self {
        Self::Box(e)
    }
}

impl From<conn_str::Error> for Error {
    fn from(e: conn_str::Error) -> Self {
        Self::ConnStr(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<&'static str> for Error {
    fn from(e: &'static str) -> Self {
        Self::Str(e)
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::String(e)
    }
}

impl From<tiberius::Error> for Error {
    fn from(e: tiberius::Error) -> Self {
        Self::Tiberius(e)
    }
}

impl From<std::env::VarError> for Error {
    fn from(e: std::env::VarError) -> Self {
        Self::Var(e)
    }
}
