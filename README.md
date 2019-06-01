raft-sqlite3
===========

This repository provides the `raftsqlite3` package. The package exports the
`Sqlite3Store` which is an implementation of both a `LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` [package
here](https://github.com/hashicorp/raft).

This implementation uses [SQLite3](https://www.sqlite.org/index.html). SQLite is
a C-language library that implements a small, fast, self-contained, high-reliability, 
full-featured, SQL database engine.
