use conn_str::{append_key_value, MsSqlConnStr};
use failure::{format_err, Error};
use std::str::FromStr;

pub(crate) fn adjust_conn_str(s: &str) -> Result<String, Error> {
    let conn = MsSqlConnStr::from_str(s)?;

    let datasource = conn
        .data_source()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| format_err!("data source / server not specified in connection string."))?;

    let datasource = resolve_datasource_into_ip(datasource)?;
    let mut out = String::new();

    append_key_value(&mut out, "server", &datasource, false);

    if let Some(v) = conn.initial_catalog() {
        append_key_value(&mut out, "database", v, false);
    }

    if let Some(v) = conn.user_id() {
        append_key_value(&mut out, "user", v, false);
    }

    if let Some(v) = conn.password() {
        append_key_value(&mut out, "password", v, false);
    }

    if conn.integrated_security()? {
        append_key_value(&mut out, "integratedsecurity", "sspi", false);
    }

    if conn.trust_server_certificate_or(true)? {
        append_key_value(&mut out, "trustservercertificate", "true", false);
    }

    if conn.encrypt()? {
        append_key_value(&mut out, "encrypt", "true", false)
    }

    Ok(out)
}

/// Resolve the sql server for replacing in connection str with the ip.
fn resolve_datasource_into_ip(s: &str) -> Result<String, Error> {
    let mut out = String::new();

    let instance_sep = s.find('\\');
    let port_sep = s.find(',');
    let has_tcp = s.to_lowercase().starts_with("tcp:");
    let mut tcp_sep = 0;

    if has_tcp {
        tcp_sep = 4;
    }

    out.push_str("tcp:");

    let m = std::cmp::min(
        port_sep.unwrap_or_else(|| s.len()),
        instance_sep.unwrap_or_else(|| s.len()),
    );

    let machine = s.chars().take(m).skip(tcp_sep).collect::<String>();
    let machine = resolve(&machine)?;

    out.push_str(&machine);

    let instance = instance_sep.map(|i| {
        s.chars()
            .take(port_sep.unwrap_or_else(|| s.len()))
            .skip(i)
            .collect::<String>()
    });

    let port = port_sep
        .map(|i| s.chars().skip(i + 1).collect::<String>())
        .filter(|p| !p.is_empty());

    match (instance, port) {
        (Some(instance), Some(port)) => {
            out.push_str(&instance);
            out.push(',');
            out.push_str(&port);
        }
        (Some(instance), None) => {
            out.push_str(&instance);
        }
        (None, Some(port)) => {
            out.push(',');
            out.push_str(&port);
        }
        (None, None) => {}
    }

    log::trace!(
        "resolved server connection string from `{}` to `{}`",
        s,
        out
    );
    Ok(out)
}

#[test]
fn resolve_datasource_into_ip_works() {
    assert!(resolve_datasource_into_ip(r#"tcp:localhost\Sql2017"#).is_ok());

    assert!(resolve_datasource_into_ip(r#"tcp:localhost"#).is_ok());

    assert_eq!(
        "tcp:127.0.0.1,1433",
        resolve_datasource_into_ip(r#"tcp:localhost,1433"#).unwrap()
    );

    assert_eq!(
        "tcp:172.18.71.36,1433",
        resolve_datasource_into_ip(r#"tcp:172.18.71.36,1433"#).unwrap()
    );

    assert!(resolve_datasource_into_ip(r#"tcp:localhost"#).is_ok());

    assert!(resolve_datasource_into_ip(r#"tcp:."#).is_ok());

    assert!(resolve_datasource_into_ip(r#".\Sql2017"#).is_ok());

    assert!(resolve_datasource_into_ip(r#"."#).is_ok());

    assert!(resolve_datasource_into_ip(r#".,1433"#).is_ok());

    assert!(resolve_datasource_into_ip(r#".\Sql2017,1433"#).is_ok());
}

fn resolve(mut host: &str) -> Result<String, Error> {
    use std::net::ToSocketAddrs;

    if host == "." {
        host = "localhost";
    }

    let mut ipv4 = None;
    let mut ipv6 = None;
    let iter = (host, 0).to_socket_addrs()?;

    for addr in iter {
        if addr.is_ipv4() {
            ipv4 = Some(addr);
            break;
        }
        if addr.is_ipv6() {
            ipv6 = Some(addr);
        }
    }

    let socket_address = ipv4.or(ipv6);

    if let Some(socket_address) = socket_address {
        Ok(socket_address.ip().to_string())
    } else {
        Err(format_err!("Host {} not found.", host))
    }
}

#[test]
fn resolve_works() {
    assert!(resolve(".").is_ok());
    assert!(resolve("localhost").is_ok());
    assert!(resolve(&std::env::var("COMPUTERNAME").unwrap()).is_ok());
}

pub fn replace_params(sql: &mut String, param: &str, replace: &str) {
    enum State {
        None,
        Other,
        Param(usize),
    }

    let mut vec = Vec::new();
    let mut state = State::None;

    for (index, c) in sql.char_indices() {
        match state {
            State::None => {
                if c == '@' {
                    state = State::Param(index + 1);
                } else if !c.is_whitespace() && !c.is_ascii_punctuation() {
                    state = State::Other;
                }
            }
            State::Param(start) => {
                if (c.is_whitespace() || c.is_ascii_punctuation()) && c != '@' && c != '_' {
                    state = State::None;

                    if sql[start..index].to_lowercase() == param {
                        vec.push(start..index);
                    }
                } else if !c.is_alphanumeric() && c != '_' {
                    state = State::Other;
                }
            }
            State::Other => {
                if c.is_whitespace() || c.is_ascii_punctuation() {
                    state = State::None;
                }
            }
        }
    }

    match state {
        State::None | State::Other => {}
        State::Param(start) => {
            if sql[start..].to_lowercase() == param {
                vec.push(start..sql.len());
            }
        }
    }

    for r in vec.into_iter().rev() {
        sql.replace_range(r, replace);
    }
}

#[test]
fn replace_params_works() {
    let mut s = "SELECT @p0,@p1,@p2 FROM Test".to_owned();

    replace_params(&mut s, "p0", "param1");
    replace_params(&mut s, "p1", "param2");
    replace_params(&mut s, "p2", "param3");

    assert_eq!("SELECT @param1,@param2,@param3 FROM Test", &s);
}
