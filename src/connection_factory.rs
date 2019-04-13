use crate::Connection;
use failure::{bail, Error};
use futures::Future;
use std::ffi::OsStr;

/// Creates a database [Connection](struct.Connection.html) on demand.
#[derive(Clone)]
pub struct ConnectionFactory(String);

impl ConnectionFactory {
    /// Creates a new instance.
    ///
    /// # Example
    /// ```
    /// use mssql_client::ConnectionFactory;
    ///
    /// let conn_str = "server=tcp:localhost\\SQL2017;database=Database1;integratedsecurity=sspi;trustservercertificate=true";
    /// let connection_factory = ConnectionFactory::new(conn_str);
    ///
    /// // creates a connection from a ConnectionFactory
    /// let connection = connection_factory.create_connection();
    ///
    /// // do want you want with the connection ...
    /// ```
    pub fn new<S>(s: S) -> Self
    where
        S: Into<String>,
    {
        ConnectionFactory(s.into())
    }

    /// Create a new instance based on an environment variable.
    ///
    /// # Example
    /// ```
    /// use mssql_client::ConnectionFactory;
    ///
    /// let env_var = "MSSQL_DB";
    /// let connection_factory = ConnectionFactory::from_env(env_var).unwrap();
    ///
    /// // creates a connection from a ConnectionFactory
    /// let connection = connection_factory.create_connection();
    ///
    /// // do want you want with the connection ...
    /// ```
    pub fn from_env<S>(key: S) -> Result<Self, Error>
    where
        S: AsRef<OsStr>,
    {
        let key = key.as_ref();

        match ::std::env::var(key) {
            Ok(s) => Ok(ConnectionFactory::from(s)),
            Err(e) => bail!(
                "ConnectionFactory from env variable {:#?} failed. {}",
                key,
                e
            ),
        }
    }

    /// Creates an instance of a [Connection](struct.Connection.html)
    ///
    /// # Example
    /// ```
    /// use mssql_client::ConnectionFactory;
    ///
    /// let env_var = "MSSQL_DB";
    /// let connection_factory = ConnectionFactory::from_env(env_var).unwrap();
    ///
    /// // creates a connection from a ConnectionFactory
    /// let connection = connection_factory.create_connection();
    ///
    /// // do want you want with the connection ...
    /// ```
    pub fn create_connection<'a>(&'a self) -> impl Future<Item = Connection, Error = Error> + 'a {
        Connection::connect(self.0.as_str())
    }
}

impl<S> From<S> for ConnectionFactory
where
    S: Into<String>,
{
    /// Convert a connection string into a ConnectionFactory.
    fn from(s: S) -> Self {
        ConnectionFactory::new(s)
    }
}
