# Client

The toyDB client is in the [`client`](https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs)
module. It uses the same Bincode-based protocol that we saw in the server section, sending
`toydb::Request` and receiving `toydb::Response`.

## Client Library

The main client library `toydb::Client` is used to connect to a toyDB server:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs#L15-L24

When initialized, it connects to a toyDB server over TCP:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs#L27-L33

It can then send Bincode-encoded `toydb::Request` to the server, and receive `toydb::Response`
back.

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs#L35-L40


In particular, `Client::execute` can be used to execute arbitrary SQL statements in the client's
current session:

https://github.com/erikgrinaker/toydb/blob/213e5c02b09f1a3cac6a8bbd0a81773462f367f5/src/client.rs#L42-L56

## `toysql` Binary

However, `toydb::Client` is a programmatic API, and we want a more convenient user interface.
The `toysql` client in [`src/bin/toysql.rs`](https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs)
provides a typical [REPL](https://en.wikipedia.org/wiki/Read–eval–print_loop) (read-evaluate-print loop) where users can enter SQL statements and view the results.

Like `toydb`, `toysql` is a tiny [`clap`](https://docs.rs/clap/latest/clap/) command that takes a
toyDB server address to connect to and starts an interactive shell:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L29-L53

This first attempts to connect to the toyDB server using the `toydb::Client` client, and then starts
an interactive shell using the [Rustyline](https://docs.rs/rustyline/latest/rustyline/) library.

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L55-L81

The shell is simply a loop that prompts the user to input a SQL statement:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L216-L250

Each statement is the executed against the server via `toydb::Client::execute`, and the response
is formatted and printed as output:

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L83-L92

https://github.com/erikgrinaker/toydb/blob/0839215770e31f1e693d5cccf20a68210deaaa3f/src/bin/toysql.rs#L175-L204

And with that, we have a fully functional SQL database system and can run queries to our heart's
content. Have fun!

---

<p align="center">
← <a href="server.md">Server</a>
</p>