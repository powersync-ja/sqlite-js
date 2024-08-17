# @sqlite-js/driver

This contains a driver API, as well as driver utilities.

## @sqlite-js/driver

The main export contains the driver API. The API is centered around connection pooling and prepared statements.

## @sqlite-js/driver/node

This is a driver implementation for NodeJS based on the experimental `node:sqlite` package.

There are two main options for using the driver:

```ts
const driver = NodeSqliteDriver.open(path);
```

This opens a connection pool using worker_threads, giving asynchronous I/O.

```ts
const driver = NodeSqliteDriver.openInProcess(path);
```

This opens a single in-process connection, with blocking I/O. This can give lower latency and higher throughput, at the cost of blocking the process and not supporting concurrent operations.

## @sqlite-js/driver/util

This contains utilities for driver implementations, such as connection pooling.

## @sqlite-js/driver/worker_threads

This contains utilities for running a driver implementation in a NodeJS worker_thread, to get non-blocking I/O.

The APIs here are not stable, and expected to change.
