import {
  BatchedUpdateEvent,
  ExecuteOptions,
  PreparedQuery,
  QueryInterface,
  QueryOptions,
  ReserveConnectionOptions,
  ReservedSqliteConnection,
  ResultSet,
  SqliteConnection,
  SqliteConnectionPool,
  SqliteQuery,
  SqliteTransaction,
  StreamedExecuteOptions,
  TablesChangedEvent,
  TablesChangedListener,
  TransactionCloseEvent,
  TransactionCloseListener,
  TransactionOptions,
  UpdateListener
} from './api.js';
import { SqliteArguments, SqliteValue } from './common.js';
import {
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqlitePrepareResponse
} from './driver-api.js';

export class ConnectionPoolImpl
  implements SqliteConnectionPool, QueryInterface
{
  private connections = new WeakMap<SqliteDriverConnection, SqliteConnection>();
  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;

  constructor(private driver: SqliteDriverConnectionPool) {
    if (typeof Symbol.asyncDispose != 'undefined') {
      this[Symbol.asyncDispose] = () => this.close();
    }
  }

  prepare<T>(sql: string, args?: SqliteArguments): PreparedQuery<T> {
    return new ConnectionPoolPreparedQueryImpl<T>(this, sql, args);
  }

  query<T>(sql: string, args?: SqliteArguments): SqliteQuery<T> {
    return new QueryImpl<T>(this, sql, args);
  }

  execute<T>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    return this.reserveConnection((con) => {
      return con.execute(query, args, options);
    });
  }

  async *executeStreamed<T>(
    query: string | PreparedQuery<T>,
    args?: SqliteArguments | undefined,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, any, unknown> {
    const con = await this.driver.reserveConnection(options ?? {});
    try {
      const c2 = new ConnectionImpl(con);
      for await (let chunk of c2.executeStreamed(query, args, options)) {
        yield chunk;
      }
    } finally {
      con.release();
    }
  }

  select<T>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    return this.reserveConnection(
      (con) => {
        return con.select(query, args, options);
      },
      { readonly: true }
    );
  }

  async reserveConnection<T>(
    callback: (connection: SqliteConnection) => Promise<T>,
    options?: ReserveConnectionOptions | undefined
  ): Promise<T>;

  async reserveConnection(
    options?: ReserveConnectionOptions | undefined
  ): Promise<ReservedSqliteConnection>;

  async reserveConnection<T>(
    callback:
      | ((connection: SqliteConnection) => Promise<T>)
      | ReserveConnectionOptions
      | undefined,
    options?: ReserveConnectionOptions | undefined
  ): Promise<T | ReservedSqliteConnection> {
    const resolvedOptions =
      (typeof callback == 'function' ? options : callback) ?? {};

    const con = await this.driver.reserveConnection(options ?? {});
    let wrapped = this.connections.get(con);
    if (wrapped == null) {
      wrapped = new ConnectionImpl(con);
      this.connections.set(con, wrapped);
    }

    if (typeof callback == 'function') {
      try {
        return await callback(wrapped);
      } finally {
        await con.release();
      }
    } else {
      return new ReservedConnectionImpl(wrapped, () => con.release());
    }
  }

  close(): Promise<void> {
    return this.driver.close();
  }
}

export class ReservedConnectionImpl implements ReservedSqliteConnection {
  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;

  constructor(
    public connection: SqliteConnection,
    public release: () => Promise<void>
  ) {
    if (typeof Symbol.asyncDispose != 'undefined') {
      this[Symbol.asyncDispose] = release;
    }
  }

  query<T>(
    sql: string,
    args?: SqliteArguments,
    options?: ReserveConnectionOptions | undefined
  ): SqliteQuery<T> {
    return new QueryImpl<T>(this, sql, args);
  }

  prepare<T>(sql: string, args?: SqliteArguments): PreparedQuery<T> {
    return this.connection.prepare(sql, args);
  }

  transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options?: TransactionOptions | undefined
  ): Promise<T> {
    return this.connection.transaction(callback, options);
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    return this.connection.onUpdate(listener, options);
  }
  onTransactionClose(listener: TransactionCloseListener): () => void {
    return this.connection.onTransactionClose(listener);
  }

  onTablesChanged(listener: TablesChangedListener): () => void {
    return this.connection.onTablesChanged(listener);
  }

  close(): Promise<void> {
    return this.connection.close();
  }

  execute<T>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    return this.connection.execute(query, args, options);
  }

  executeStreamed<T>(
    query: string,
    args: SqliteArguments | undefined,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, any, unknown> {
    return this.connection.executeStreamed(query, args, options);
  }

  select<T>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    return this.connection.select(query, args, options);
  }
}

export class ConnectionImpl implements SqliteConnection {
  private nextPreparedQueryId = 10;

  private begin: PreparedQuery<void> | undefined;
  private commit: PreparedQuery<void> | undefined;
  private rollback: PreparedQuery<void> | undefined;

  constructor(private driver: SqliteDriverConnection) {}

  async transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options: TransactionOptions
  ): Promise<T> {
    this.begin ??= this.prepare('BEGIN');
    this.commit ??= this.prepare('COMMIT');
    this.rollback ??= this.prepare('ROLLBACK');

    await this.begin.execute();
    try {
      const tx = new TransactionImpl(this);
      const result = await callback(tx);

      await this.commit.execute();
      return result;
    } catch (e) {
      await this.rollback.execute();
      throw e;
    }
  }

  onUpdate(
    listener: (event: BatchedUpdateEvent) => void,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    throw new Error('Method not implemented.');
  }
  onTransactionClose(
    listener: (event: TransactionCloseEvent) => void
  ): () => void {
    throw new Error('Method not implemented.');
  }

  onTablesChanged(listener: (event: TablesChangedEvent) => void): () => void {
    throw new Error('Method not implemented.');
  }

  close(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  prepare<T>(sql: string, args?: SqliteArguments): PreparedQuery<T> {
    const id = this.nextPreparedQueryId++;
    return new ConnectionPreparedQueryImpl(this, this.driver, id, sql, args);
  }

  query<T>(query: string, args?: SqliteArguments): SqliteQuery<T> {
    return new QueryImpl<T>(this, query, args);
  }

  async execute<T>(
    query: string,
    args: SqliteArguments | undefined,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    let result: ResultSet<T> | null = null;

    for await (let rs of this.executeStreamed<any>(query, args, options)) {
      if (result == null) {
        result = rs;
      } else {
        result.cells.push(...rs.cells);
        result.changes = rs.changes ?? result.changes;
        result.rowId = rs.rowId ?? result.rowId;
      }
    }
    return result!;
  }

  async *executeStreamed<T>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments | undefined,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, void, unknown> {
    try {
      const [{ columns, error }, { error: error2, skipped }] =
        await this.driver.execute([
          { prepare: { id: 0, sql: query as string } },
          { bind: { id: 0, parameters: args ?? [] } }
        ]);
      if (error != null) {
        throw error;
      }
      if (error2 != null) {
        throw error2;
      }
      if (skipped) {
        // Not expected
        throw new Error('Previous query failed');
      }

      while (true) {
        const [{ rows, error, done, skipped }] = await this.driver.execute([
          { step: { id: 0, n: 10, bigint: options?.bigint } }
        ]);
        if (error != null) {
          throw error;
        }
        if (skipped) {
          // Not expected
          throw new Error('Previous query failed');
        }
        if (rows != null) {
          yield new ResultSetImpl(columns!, rows);
        }
        if (done) {
          break;
        }
      }

      if (options?.includeChanges) {
        const results = await run(
          this.driver,
          'select changes(), last_insert_rowid()'
        );
        const [[changes, rowid]] = results;
        const rs = new ResultSetImpl<T>(columns!, []);
        rs.changes = changes as number;
        rs.rowId = rowid as number;
        yield rs;
      }
    } finally {
      await this.driver.execute([{ sync: {} }]);
    }
  }

  async select<T>(
    query: string,
    args?: SqliteArguments,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    const rs = await this.execute<T>(query, args, options);
    return rs.rows;
  }
}

export class TransactionImpl implements SqliteTransaction {
  private preparedQueries: PreparedQuery<any>[] = [];

  constructor(private con: ConnectionImpl) {}

  getAutoCommit(): Promise<boolean> {
    throw new Error('Method not implemented.');
  }

  async rollback(): Promise<void> {
    await this.select('ROLLBACK');
  }

  prepare<T>(sql: string, args?: SqliteArguments): PreparedQuery<T> {
    const q = this.con.prepare<T>(sql, args);
    // FIXME: auto-dispose these after transaction commit / rollback
    this.preparedQueries.push(q);
    return q;
  }

  query<T>(query: string, args: SqliteArguments): SqliteQuery<T> {
    return new QueryImpl(this, query, args);
  }

  execute<T>(
    query: string,
    args: SqliteArguments,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    return this.con.execute(query, args, options);
  }

  executeStreamed<T>(
    query: string,
    args: SqliteArguments,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, any, unknown> {
    return this.con.executeStreamed(query, args, options);
  }

  select<T>(
    query: string,
    args?: SqliteArguments,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    return this.con.select(query, args, options);
  }
}

class ResultSetImpl<T> implements ResultSet<T> {
  columns: (keyof T)[];
  cells: SqliteValue[][];
  private _rowObjects: T[] | undefined;

  rowId?: number;
  changes?: number;

  constructor(columns: string[], rows?: SqliteValue[][]) {
    this.columns = columns as any[] as (keyof T)[];
    this.cells = rows ?? [];
  }

  get rows() {
    if (this._rowObjects == null) {
      this._rowObjects = this.cells.map((row) => {
        return Object.fromEntries(
          this.columns.map((column, i) => {
            return [column, row[i]];
          })
        ) as T;
      });
    }
    return this._rowObjects;
  }
}

async function run(con: SqliteDriverConnection, sql: string) {
  const [{ columns }, { rows }, { error }] = (await con.execute([
    { prepare: { id: 0, sql } },
    { step: { id: 0, all: true } },
    { sync: {} }
  ])) as any[];
  if (error != null) {
    throw error;
  }
  return rows as SqliteValue[][];
}

class QueryImpl<T> implements SqliteQuery<T> {
  constructor(
    private context: QueryInterface,
    public sql: string,
    public args: SqliteArguments
  ) {}

  in(transaction: SqliteTransaction): SqliteQuery<T> {
    return new QueryImpl<T>(transaction, this.sql, this.args);
  }

  executeStreamed(
    options?: StreamedExecuteOptions | undefined
  ): AsyncGenerator<ResultSet<any>, any, unknown> {
    return this.context.executeStreamed(this.sql, this.args, options);
  }

  execute(options?: ExecuteOptions | undefined): Promise<ResultSet<T>> {
    return this.context.execute(this.sql, this.args, options);
  }

  select(options?: QueryOptions | undefined): Promise<T[]> {
    return this.context.select(this.sql, this.args, options);
  }
}

class ConnectionPoolPreparedQueryImpl<T> implements PreparedQuery<T> {
  [Symbol.dispose]: () => void = undefined as any;

  private byConnection: Map<ConnectionImpl, PreparedQuery<T>> = new Map();

  constructor(
    private context: ConnectionPoolImpl,
    public sql: string,
    public args: SqliteArguments
  ) {
    if (typeof Symbol.dispose != 'undefined') {
      this[Symbol.dispose] = () => this.dispose();
    }
  }

  parse(): Promise<{ columns: string[] }> {
    throw new Error('Method not implemented.');
  }

  in(context: QueryInterface): SqliteQuery<T> {
    throw new Error('Method not implemented.');
  }

  executeStreamed(
    options?: StreamedExecuteOptions | undefined
  ): AsyncGenerator<ResultSet<any>, any, unknown> {
    throw new Error('Method not implemented.');
  }

  execute(options?: ExecuteOptions | undefined): Promise<ResultSet<T>> {
    return this.reservedQuery((sub) => sub.execute(options));
  }

  select(options?: QueryOptions | undefined): Promise<T[]> {
    return this.reservedQuery((sub) => sub.select(options));
  }

  dispose(): void {
    for (let sub of this.byConnection.values()) {
      sub.dispose();
    }
    this.byConnection.clear();
  }

  private reservedQuery<R>(callback: (sub: PreparedQuery<T>) => Promise<R>) {
    return this.context.reserveConnection(async (connection) => {
      const cimpl = connection as ConnectionImpl;
      let sub = this.byConnection.get(cimpl);
      if (sub == null) {
        sub = cimpl.prepare(this.sql, this.args);
        this.byConnection.set(cimpl, sub);
      }
      return callback(sub);
    });
  }
}

class ConnectionPreparedQueryImpl<T> implements PreparedQuery<T> {
  [Symbol.dispose]: () => void = undefined as any;

  private preparePromise: Promise<SqlitePrepareResponse>;

  constructor(
    private context: ConnectionImpl,
    private driver: SqliteDriverConnection,
    private queryId: number,
    public sql: string,
    public args: SqliteArguments
  ) {
    if (typeof Symbol.dispose != 'undefined') {
      this[Symbol.dispose] = () => this.dispose();
    }

    this.preparePromise = this.driver
      .execute([
        { prepare: { id: this.queryId, sql: sql } },
        { bind: { id: this.queryId, parameters: this.args } },
        { sync: {} }
      ])
      .then((r) => {
        if (r[2].error != null) {
          throw r[2].error;
        } else {
          return r[0];
        }
      });
  }

  async parse(): Promise<{ columns: string[] }> {
    const r = await this.preparePromise;
    return { columns: r.columns! };
  }

  in(context: QueryInterface): SqliteQuery<T> {
    throw new Error('Method not implemented.');
  }

  async *executeStreamed(
    options?: StreamedExecuteOptions | undefined
  ): AsyncGenerator<ResultSet<any>, any, unknown> {
    try {
      const { columns } = await this.parse();
      this.driver.execute([{ reset: { id: this.queryId } }]);
      while (true) {
        const [{ rows, error, done, skipped }] = await this.driver.execute([
          { step: { id: this.queryId, n: 10, bigint: options?.bigint } }
        ]);
        if (error != null) {
          throw error;
        }
        if (skipped) {
          // Not expected
          throw new Error('Previous query failed');
        }
        if (rows != null) {
          yield new ResultSetImpl(columns!, rows);
        }
        if (done) {
          break;
        }
      }

      if (options?.includeChanges) {
        const results = await run(
          this.driver,
          'select changes(), last_insert_rowid()'
        );
        const [[changes, rowid]] = results;
        const rs = new ResultSetImpl<T>(columns!, []);
        rs.changes = changes as number;
        rs.rowId = rowid as number;
        yield rs;
      }
    } finally {
      await this.driver.execute([{ sync: {} }]);
    }
  }

  async execute(options?: ExecuteOptions | undefined): Promise<ResultSet<T>> {
    let result: ResultSet<T> | null = null;

    for await (let rs of this.executeStreamed(options)) {
      if (result == null) {
        result = rs;
      } else {
        result.cells.push(...rs.cells);
        result.changes = rs.changes ?? result.changes;
        result.rowId = rs.rowId ?? result.rowId;
      }
    }
    return result!;
  }

  async select(options?: QueryOptions | undefined): Promise<T[]> {
    const rs = await this.execute(options);
    return rs.rows;
  }

  dispose(): void {
    this.driver.execute([{ finalize: { id: this.queryId } }]);
  }
}
