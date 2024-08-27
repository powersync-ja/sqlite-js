# sqlite-js

Universal SQLite APIs for JavaScript.

The project provides two primary APIs:

1. The driver API. This aims to expose a minimum API for drivers to implement, while supporting a rich set of functionality. This should have as little as possible performance overhead, while still supporting asynchronous implementations.

2. The end-user API. This is a library built on top of the driver API, that exposes higher-level functionality such as transactions, convenience methods, template strings (later), pipelining.

## @sqlite-js/driver

This is a universal driver API and utilities for implementing drivers.

The APIs here are low-level. These are intended to be implemented by drivers, and used by higher-level libraries.

See [DRIVER-API.md](./DRIVER-API.md) for details on the design.

### @sqlite-js/driver/node

This is a driver implementation for NodeJS based on the experimental `node:sqlite` package.

## @sqlite-js/better-sqlite3-driver

This is a driver implementation for NodeJS implementation based `better-sqlite3`.

## @sqlite-js/api

This contains a higher-level API, with simple methods to execute queries, and supports transactions and pipelining.

This is largely a proof-of-concept to validate and test the underlying driver APIs, rather than having a fixed design.

The current iteration of the APIs is visible at [packages/api/src/api.ts](packages/api/src/api.ts).

# Why split the APIs?

A previous iteration used a single API for both the end-user API and the driver API. This had serveral disadvantages:

1. The implementation per driver requires a lot more effort.
2. Iterating on the API becomes much more difficult.
   1. Implementing minor quality-of-life improvements for the end user becomes a required change in every driver.
3. Optimizing the end-user API for performance is difficult. To cover all the different use cases, it requires implementing many different features such as prepared statements, batching, pipelining. This becomes a very large API for drivers to implement.
4. The goals for the end-user API is different from the driver API:
   1. End-users want a rich but simple-to-use API to access the database.
   2. Drivers want a small surface area, that doesn't change often.

Splitting out a separate driver API, and implementing the end-user API as a separate library, avoids all the above issues.
