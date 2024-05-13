# Idea: Stream/Pipeline

Some background literature:

libsql hrana protocol:

- https://github.com/tursodatabase/libsql-client-ts/blob/main/packages/libsql-client/src/hrana.ts
- https://github.com/libsql/hrana-client-ts/blob/main/src/batch.ts

Postgres extended query protocol:

- https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING

In both those cases the protocol is optimized for network usage which could have high latency.
We're working with much lower latency, but the same principles still apply.

# Pipelining

The main idea is using a pipeline:
There are still individual requests/responses (Promise-style).
However, you can pipeline multiple requests without waiting for the previous one to complete.

Using those semantics, we can implement very granular requests, while still maintaining performance. For example, getting the last row id can be a separate request after a statement, without adding significant overhead.

With pipelining multiple requests, error handling becomes more difficult. Without pipelining, you can just stop and rollback a transaction or batch if there is an error. With pipelining, you may only get the error after submitting more statements. So you need a way to not execute those statements if an earlier one errored.

## Approaches

Postgres-style:
When one request errors, ignore all future requests.
To reset the state, the client must make a "sync" request.

libsql hrana style:
Each statement can have a conditional call. This could be:

1. Require that the previous statement succeeded.
2. Require still having an active transaction.

This is more flexible, but also adds complexity to the protocol.

## Requests

prepare_statement
bind_statement
step_n # return next n rows
step_all # return all rows
reset_statement
close_statement
