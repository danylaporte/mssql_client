use failure::{bail, format_err, Error};

/// Replace the machine name (if any) in the connection str with the ip.
pub(crate) fn replace_conn_str_machine_with_ip(s: &str) -> Result<String, Error> {
    let mut out = String::new();
    for s in s.split(';') {
        if !out.is_empty() {
            out.push(';');
        }

        let mut s = s.split('=');

        match (s.next(), s.next()) {
            (Some("server"), Some(s)) => {
                out.push_str("server=");

                let instance_sep = s.find('\\');
                let port_sep = s.find(',');
                let has_tcp = s.to_lowercase().starts_with("tcp:");
                let mut tcp_sep = 0;

                if has_tcp {
                    tcp_sep = 4;
                    out.push_str("tcp:");
                }

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
            }
            (Some(a), Some(b)) => {
                out.push_str(a);
                out.push('=');
                out.push_str(b);
            }
            _ => {}
        }

        if s.next().is_some() {
            bail!("Invalid connection string");
        }
    }

    log::trace!("resolved server connection string: {}", out);

    Ok(out)
}

#[test]
fn replace_conn_str_machine_with_ip_works() {
    assert!(replace_conn_str_machine_with_ip(
        r#"server=tcp:localhost\Sql2017;database=Database1;"#
    )
    .is_ok());

    assert!(
        replace_conn_str_machine_with_ip(r#"server=tcp:localhost;database=Database1;"#).is_ok()
    );

    assert_eq!(
        "server=tcp:127.0.0.1,1433;database=Database1;",
        replace_conn_str_machine_with_ip(r#"server=tcp:localhost,1433;database=Database1;"#)
            .unwrap()
    );

    assert_eq!(
        "server=tcp:172.18.71.36,1433;database=Db1;",
        replace_conn_str_machine_with_ip(r#"server=tcp:172.18.71.36,1433;database=Db1;"#).unwrap()
    );

    assert!(replace_conn_str_machine_with_ip(r#"server=tcp:localhost;database=Database1"#).is_ok());

    assert!(replace_conn_str_machine_with_ip(r#"server=tcp:.;database=Database1;"#).is_ok());

    assert!(replace_conn_str_machine_with_ip(r#"server=.\Sql2017;database=Database1;"#).is_ok());

    assert!(replace_conn_str_machine_with_ip(r#"server=.;database=Database1;"#).is_ok());

    assert!(replace_conn_str_machine_with_ip(r#"server=.,1433;database=Database1;"#).is_ok());

    assert!(
        replace_conn_str_machine_with_ip(r#"server=.\Sql2017,1433;database=Database1;"#).is_ok()
    );
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
