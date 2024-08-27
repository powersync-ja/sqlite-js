## Driver API

The driver API aims to have a small surface area, with little performance overhead. Ease of use is not important.

To support all potential implementations, the main APIs are asynchronous. This does add overhead, but this is unavoidable when our goal is to have a universal driver API. We do however aim to keep the performance overhead as low as possible.

The driver API primarily exposes:

1. Connection pooling. Even when using a single connection, that connection should be locked for exclusive use by one consumer at a time.
2. Prepared statements. Even if the underlying implementation does not use actual prepared statements, the same APIs can be exposed.

In general, the setup of prepared statements (preparing a statement, binding parameters) are synchronous APIs, and don't throw on invalid queries. Executing the statement is asynchronous, and this is where errors are thrown.

The driver API does not include transaction management. This is easily implemented on top of connection pooling/locking + prepared statements for begin/commit/rollback.

### The API

This is a simplified version of the API. For full details, see:
[packages/driver/src/driver-api.ts](packages/driver/src/driver-api.ts).

```ts
export interface SqliteDriverConnectionPool {
  /**
   * Reserve a connection for exclusive use.
   *
   * If there is no available connection, this will wait until one is available.
   */
  reserveConnection(
    options?: ReserveConnectionOptions
  ): Promise<ReservedConnection>;

  close(): Promise<void>;

  [Symbol.asyncDispose](): Promise<void>;
}

export interface ReservedConnection {
  /** Direct handle to the underlying connection. */
  connection: SqliteDriverConnection;

  /** Proxied to the underlying connection */
  prepare(sql: string, options?: PrepareOptions): SqliteDriverStatement;

  [Symbol.asyncDispose](): Promise<void>;
}

export interface SqliteDriverConnection {
  /**
   * Prepare a statement.
   *
   * Does not return any errors.
   */
  prepare(sql: string, options?: PrepareOptions): SqliteDriverStatement;
}

/**
 * Represents a single prepared statement.
 * Loosely modeled on the SQLite API.
 */
export interface SqliteDriverStatement {
  bind(parameters: SqliteParameterBinding): void;

  step(n?: number, options?: StepOptions): Promise<SqliteStepResult>;
  getColumns(): Promise<string[]>;
  finalize(): void;

  reset(options?: ResetOptions): void;

  [Symbol.dispose](): void;
}
```

## Design decisions

### Small surface area

We want the driver to have as small surface area as possible. In rare cases we do allow exceptions for performance or simplicity reasons.

### Reusability

The same driver connection pool should be usable by multiple different consumers within the same process. For example, the same connection pool can be used directly, by an ORM, and/or by a sync library, without running into concurrency issues. This specifically affects connection pooling (see below).

### Synchronous vs asynchronous

Many implementations can only support asynchronous methods. However, having _every_ method asynchronous can add significant overhead, if you need to chain multiple methods to run a single query. We therefore aim to have a single asynchronous call per query for most use cases. This does mean that we defer errors until that asynchronous call, and do not throw errors in `prepare()` or `bind()`.

### Transactions

Full transaction support requires a large surface area, with many design possibilities. For example, do we support nested transactions (savepoints in SQLite)? Do we expose immediate/defferred/exclusive transactions? Do we use a wrapper function, explicit resource management, or manual commit/rollback calls to manage transactions?

Instead, the driver API just provides the building blocks for transactions - connection pooling and prepared statements.

### Connection pooling

The driver API requires a connection pooling implementation, even if there is only a single underlying connection. Even in that case, it is important that a connection can be "reserved" for a single consumer at a time. This is needed for example to implement transactions, without requiring additional locking mechanisms (which would break the reusability requirement).

Connection pooling also supports specifically requesting a read-only vs read-write connection. This is important for concurrency in SQLite, which can only support a single writer at a time, but any number of concurrent readers.

### Read vs write queries

There is no fundamental distinction between read and write queries in the driver prepared statement API. This is important for use cases such as `INSERT INTO ... RETURNING *` - a "write" api that also returns data. However, read vs write locks are taken into account with connection pooling.

### "run" with results

The `run` API that returns the last insert row id and number of changes are primarily for compatibility with current libraries/APIs. Many libraries in use return that automatically for any "run" statement, and splitting that out into a separate prepared statement could add significant performance overhead (requiring two prepared statements for every single "write" query).

### Row arrays vs objects

Returning an array of cells for each row, along with a separate "columns" array, is more flexible than just using an object per row. It is always possible to convert the array to an object, given the columns header.

However, many current SQLite bindings do not expose the raw array calls. Even if they do, this path may be slower than using objects from the start. Since using the results as an array is quite rare in practice, this is left as an optional configuration, rather than a requirement for the all queries.

### Separate bind/step/reset

This allows a lot of flexibility, for example partial rebinding of parameters instead of specifying all parameters each time a prepared statement is used. However, those type of use cases are rare, and this is not important in the overall architecture. These could all be combined into a single "query with parameters" call, but would need to take into account optional streaming of results.

### bigint

SQLite supports up to 8-byte signed integers (up to 2^64-1), while JavaScript's number is limited to 2^53-1. General approaches include:

1. Always use JS numbers. This requires using TEXT for larger integers, but can still store as INTEGER and cast when inserting or returning results.
2. Automatically switching to bigint if the number is `>= 2^53`. This can easily introduce issues in the client, since `bigint` an `number` are not interoperable.
3. Require an explicit option to get `bigint` results. This is the approach we went for here.
4. Always use `number` for `REAL`, and `bigint` for `INTEGER`. You can use `cast(n to REAL)` to get a value back as a `number`. Since many users will just use small integers, this may not be ideal.

### Pipelining

The APIs guarantee that statements on a connection will be ordered in the order that calls were made. This allows pipelining statements to improve performance - the client can issue many queries before waiting for the results. One place where this breaks down is within transactions: It is possible for one statement to trigger a transaction rollback, in which case the next pipelined statement will run outside the transaction.

The current API includes a flag to indicate a statement may only be run within a transaction to work around this issue, but other suggestions are welcome.

## Driver implementation helpers

The driver package also includes helpers to assist in implementating drivers. These are optional, and not part of the driver spec. It does however make it simple to support:

1. Connection pooling - the driver itself just needs to implement logic for a single connection, and the utilities will handle connection pooling.
2. Worker threads - this can assist in spawing a separate worker thread per conneciton, to get true concurrency. The same approaches could work to support web workers in browsers in the future.

Some drivers may use different approaches for concurrency and connection pooling, without using these utilities.
