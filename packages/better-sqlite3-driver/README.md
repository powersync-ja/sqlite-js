# @sqlite-js/better-sqlite3-driver

This contains a driver implementation based on better-sqlite3.

There are two main options for using the driver:

```ts
const driver = BetterSqliteDriver.open(path);
```

This opens a connection pool using worker_threads, giving asynchronous I/O.

```ts
const driver = BetterSqliteDriver.openInProcess(path);
```

This opens a single in-process connection, with blocking I/O. This can give lower latency and higher throughput, at the cost of blocking the process and not supporting concurrent operations.
