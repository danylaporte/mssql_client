[![Build Status](https://travis-ci.org/danylaporte/mssql_client.svg?branch=master)](https://travis-ci.org/danylaporte/mssql_client)
[![Build status](https://ci.appveyor.com/api/projects/status/64ax67o8u6srqlv9?svg=true)](https://ci.appveyor.com/project/danylaporte/mssql-client)

A sql client lib for rust.

## Documentation
[API Documentation](https://danylaporte.github.io/mssql_client/mssql_client)

## Example

```rust
use mssql_client::Connection;
use tokio::executor::current_thread::block_on_all;

fn main() {
    let conn_str = "server=tcp:localhost\\SQL2017;database=Database1;integratedsecurity=sspi;trustservercertificate=true";
    let connection = Connection::connect(conn_str);
    let query = connection.query("SELECT 1 FROM Table1", ());
    let (connection, rows): (_, Vec<i32>) = block_on_all(query).unwrap();
}
```

## License

Dual-licensed to be compatible with the Rust project.

Licensed under the Apache License, Version 2.0
[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0) or the MIT license
[http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT), at your
option. This file may not be copied, modified, or distributed
except according to those terms.