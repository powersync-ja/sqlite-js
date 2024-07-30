import {
  BatchedUpdateEvent,
  ExecuteOptions,
  PreparedQuery,
  QueryInterface,
  QueryOptions,
  QueryPipeline,
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
  PrepareOptions,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqliteDriverStatement,
  SqliteRowObject
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

  prepare<T extends SqliteRowObject>(
    sql: string,
    args?: SqliteArguments
  ): PreparedQuery<T> {
    return new ConnectionPoolPreparedQueryImpl<T>(this, sql, args);
  }

  query<T extends SqliteRowObject>(
    sql: string,
    args?: SqliteArguments
  ): SqliteQuery<T> {
    return new QueryImpl<T>(this, sql, args);
  }

  pipeline(options?: ReserveConnectionOptions | undefined): QueryPipeline {
    throw new Error('pipeline not supported here');
  }

  async execute<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    const r = await this.reserveConnection(options);
    try {
      return r.execute(query, args, options);
    } finally {
      await r.release();
    }
  }

  async transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options?: (TransactionOptions & ReserveConnectionOptions) | undefined
  ): Promise<T> {
    const r = await this.reserveConnection(options);
    try {
      return r.transaction(callback, {
        type: options?.type ?? (options?.readonly ? 'deferred' : 'exclusive')
      });
    } finally {
      await r.release();
    }
  }

  async *executeStreamed<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, any, unknown> {
    const r = await this.reserveConnection(options);
    try {
      return r.executeStreamed(query, args, options);
    } finally {
      await r.release();
    }
  }

  async select<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    const r = await this.reserveConnection({ readonly: true, ...options });
    try {
      return r.select(query, args, options);
    } finally {
      await r.release();
    }
  }

  async withReservedConnection<T>(
    callback: (connection: SqliteConnection) => Promise<T>,
    options?: ReserveConnectionOptions | undefined
  ): Promise<T> {
    const con = await this.driver.reserveConnection(options ?? {});
    let wrapped = this.connections.get(con.connection);
    if (wrapped == null) {
      wrapped = new ConnectionImpl(con.connection);
      this.connections.set(con.connection, wrapped);
    }
    try {
      return await callback(wrapped);
    } finally {
      await con.release();
    }
  }

  async reserveConnection(
    options?: ReserveConnectionOptions | undefined
  ): Promise<ReservedSqliteConnection> {
    const con = await this.driver.reserveConnection(options ?? {});
    let wrapped = this.connections.get(con.connection);
    if (wrapped == null) {
      wrapped = new ConnectionImpl(con.connection);
      this.connections.set(con.connection, wrapped);
    }

    return new ReservedConnectionImpl(wrapped, () => con.release());
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

  query<T extends SqliteRowObject>(
    sql: string,
    args?: SqliteArguments,
    options?: ReserveConnectionOptions | undefined
  ): SqliteQuery<T> {
    return new QueryImpl<T>(this, sql, args);
  }

  prepare<T extends SqliteRowObject>(
    sql: string,
    args?: SqliteArguments
  ): PreparedQuery<T> {
    return this.connection.prepare(sql, args);
  }

  pipeline(options?: ReserveConnectionOptions | undefined): QueryPipeline {
    return this.connection.pipeline(options);
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

  execute<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    return this.connection.execute(query, args, options);
  }

  executeStreamed<T extends SqliteRowObject>(
    query: string,
    args: SqliteArguments | undefined,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, any, unknown> {
    return this.connection.executeStreamed(query, args, options);
  }

  select<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    return this.connection.select(query, args, options);
  }
}

export class ConnectionImpl implements SqliteConnection {
  private begin: PreparedQuery<{}> | undefined;
  private beginExclusive: PreparedQuery<{}> | undefined;
  private commit: PreparedQuery<{}> | undefined;
  private rollback: PreparedQuery<{}> | undefined;

  constructor(private driver: SqliteDriverConnection) {}

  async transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options?: TransactionOptions
  ): Promise<T> {
    this.beginExclusive ??= this.prepare('BEGIN EXCLUSIVE', undefined, {
      persist: true
    });
    this.begin ??= this.prepare('BEGIN', undefined, { persist: true });
    this.commit ??= this.prepare('COMMIT', undefined, { persist: true });
    this.rollback ??= this.prepare('ROLLBACK', undefined, { persist: true });

    if ((options?.type ?? 'exclusive') == 'exclusive') {
      await this.beginExclusive.execute();
    } else {
      await this.begin.execute();
    }
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

  async close(): Promise<void> {
    this.begin?.dispose();
    this.commit?.dispose();
    this.rollback?.dispose();
  }

  prepare<T extends SqliteRowObject>(
    sql: string,
    args?: SqliteArguments,
    options?: PrepareOptions
  ): PreparedQuery<T> {
    const statement = this.driver.prepare(sql, options);
    if (args) {
      statement.bind(args);
    }
    return new ConnectionPreparedQueryImpl(
      this,
      this.driver,
      statement,
      sql,
      args
    );
  }

  query<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments
  ): SqliteQuery<T> {
    return new QueryImpl<T>(this, query, args);
  }

  pipeline(options?: ReserveConnectionOptions | undefined): QueryPipeline {
    return new QueryPipelineImpl(this.driver);
  }

  async execute<T extends SqliteRowObject>(
    query: string,
    args: SqliteArguments | undefined,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    using statement = this.driver.prepare(query, {
      bigint: options?.bigint,
      rawResults: false
    });
    if (args != null) {
      statement.bind(args);
    }
    const stepPromise = statement.step();

    const rs: ResultSet<T> = new ResultSetImpl<T>(
      (await stepPromise).rows! as T[]
    );

    if (options?.includeChanges) {
      const results = await run(
        this.driver,
        'select changes() as changes, last_insert_rowid() as rowid'
      );
      const [{ changes, rowid }] = results;

      rs.changes = changes as number;
      rs.rowId = rowid as number;
    }
    return rs;
  }

  async *executeStreamed<T extends SqliteRowObject>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments | undefined,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, void, unknown> {
    using statement = this.driver.prepare(query as string, {
      bigint: options?.bigint
    });
    if (args != null) {
      statement.bind(args);
    }

    while (true) {
      const { rows, done } = await statement.step(10);
      if (rows != null) {
        yield new ResultSetImpl(rows as T[]);
      }
      if (done) {
        break;
      }
    }

    if (options?.includeChanges) {
      const results = await run(
        this.driver,
        'select changes() as changes, last_insert_rowid() as rowid'
      );
      const [{ changes, rowid }] = results;
      const rs = new ResultSetImpl<T>([]);
      rs.changes = changes as number;
      rs.rowId = rowid as number;
      yield rs;
    }
  }

  async select<T extends SqliteRowObject>(
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

  prepare<T extends SqliteRowObject>(
    sql: string,
    args?: SqliteArguments
  ): PreparedQuery<T> {
    const q = this.con.prepare<T>(sql, args);
    // FIXME: auto-dispose these after transaction commit / rollback
    this.preparedQueries.push(q);
    return q;
  }

  query<T extends SqliteRowObject>(
    query: string,
    args: SqliteArguments
  ): SqliteQuery<T> {
    return new QueryImpl(this, query, args);
  }

  pipeline(options?: ReserveConnectionOptions | undefined): QueryPipeline {
    return this.con.pipeline(options);
  }

  execute<T extends SqliteRowObject>(
    query: string,
    args: SqliteArguments,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    return this.con.execute(query, args, options);
  }

  executeStreamed<T extends SqliteRowObject>(
    query: string,
    args: SqliteArguments,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, any, unknown> {
    return this.con.executeStreamed(query, args, options);
  }

  select<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    return this.con.select(query, args, options);
  }
}

class RawResultSetImpl<T extends SqliteRowObject> implements ResultSet<T> {
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

class ResultSetImpl<T extends SqliteRowObject> implements ResultSet<T> {
  rows: T[];

  rowId?: number;
  changes?: number;

  constructor(rows?: T[]) {
    this.rows = rows ?? [];
  }

  get columns(): never {
    throw new Error('Only available on raw queries');
  }

  get cells(): never {
    throw new Error('Only available on raw queries');
  }
}

async function run(con: SqliteDriverConnection, sql: string) {
  using statement = con.prepare(sql, { rawResults: false });
  const { rows } = await statement.step();
  return rows as SqliteRowObject[];
}

class QueryImpl<T extends SqliteRowObject> implements SqliteQuery<T> {
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

class ConnectionPoolPreparedQueryImpl<T extends SqliteRowObject>
  implements PreparedQuery<T>
{
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

  async parse(): Promise<{ columns: string[] }> {
    const r = await this.context.reserveConnection();
    try {
      const q = this.cachedQuery(r);
      return q.parse();
    } finally {
      await r.release();
    }
  }

  in(context: QueryInterface): SqliteQuery<T> {
    throw new Error('Method not implemented.');
  }

  async *executeStreamed(
    args?: SqliteArguments,
    options?: StreamedExecuteOptions | undefined
  ): AsyncGenerator<ResultSet<any>, any, unknown> {
    const r = await this.context.reserveConnection();
    try {
      const q = this.cachedQuery(r);
      yield* q.executeStreamed(args, options);
    } finally {
      await r.release();
    }
  }

  async execute(
    args?: SqliteArguments,
    options?: ExecuteOptions | undefined
  ): Promise<ResultSet<T>> {
    const r = await this.context.reserveConnection();
    try {
      const q = this.cachedQuery(r);
      return q.execute(args, options);
    } finally {
      await r.release();
    }
  }

  async select(
    args?: SqliteArguments,
    options?: QueryOptions | undefined
  ): Promise<T[]> {
    const r = await this.context.reserveConnection();
    try {
      const q = this.cachedQuery(r);
      return q.select(args, options);
    } finally {
      await r.release();
    }
  }

  dispose(): void {
    for (let sub of this.byConnection.values()) {
      sub.dispose();
    }
    this.byConnection.clear();
  }

  private cachedQuery(connection: SqliteConnection) {
    const cimpl = connection as ConnectionImpl;
    let sub = this.byConnection.get(cimpl);
    if (sub == null) {
      sub = cimpl.prepare(this.sql, this.args);
      this.byConnection.set(cimpl, sub);
    }
    return sub;
  }
}

class ConnectionPreparedQueryImpl<T extends SqliteRowObject>
  implements PreparedQuery<T>
{
  [Symbol.dispose]: () => void = undefined as any;

  private columnsPromise: Promise<string[]>;

  constructor(
    private context: ConnectionImpl,
    private driver: SqliteDriverConnection,
    public statement: SqliteDriverStatement,
    public sql: string,
    public args: SqliteArguments
  ) {
    if (typeof Symbol.dispose != 'undefined') {
      this[Symbol.dispose] = () => this.dispose();
    }
    this.columnsPromise = statement.getColumns();
  }

  async parse(): Promise<{ columns: string[] }> {
    return {
      columns: await this.columnsPromise
    };
  }

  in(context: QueryInterface): SqliteQuery<T> {
    throw new Error('Method not implemented.');
  }

  async *executeStreamed(
    args?: SqliteArguments,
    options?: StreamedExecuteOptions | undefined
  ): AsyncGenerator<ResultSet<any>, any, unknown> {
    const chunkSize = options?.chunkSize ?? 10;
    if (args != null) {
      this.statement.bind(args);
    }
    try {
      while (true) {
        const { rows, done } = await this.statement.step(chunkSize);
        if (rows != null) {
          yield new ResultSetImpl(rows as T[]);
        }
        if (done) {
          break;
        }
      }

      if (options?.includeChanges) {
        const results = await run(
          this.driver,
          'select changes() as changes, last_insert_rowid() as rowid'
        );
        const [{ changes, rowid }] = results;
        const rs = new ResultSetImpl<T>([]);
        rs.changes = changes as number;
        rs.rowId = rowid as number;
        yield rs;
      }
    } finally {
      this.statement.reset();
    }
  }

  async execute(
    args?: SqliteArguments,
    options?: ExecuteOptions | undefined
  ): Promise<ResultSet<T>> {
    try {
      if (args != null) {
        this.statement.bind(args);
      }
      const { rows } = await this.statement.step();

      const rs = new ResultSetImpl<T>(rows as T[]);

      if (options?.includeChanges) {
        const results = await run(
          this.driver,
          'select changes() as changes, last_insert_rowid() as rowid'
        );
        const [{ changes, rowid }] = results;
        const rs = new ResultSetImpl<T>([]);
        rs.changes = changes as number;
        rs.rowId = rowid as number;
      }
      return rs;
    } finally {
      this.statement.reset();
    }
  }

  async select(
    args?: SqliteArguments,
    options?: QueryOptions | undefined
  ): Promise<T[]> {
    const rs = await this.execute(args, options);
    return rs.rows;
  }

  dispose(): void {
    this.statement.finalize();
  }
}

class QueryPipelineImpl implements QueryPipeline {
  count: number = 0;
  private lastPromise: Promise<any> | undefined = undefined;

  constructor(private driver: SqliteDriverConnection) {}

  execute(query: string | PreparedQuery<any>, args?: SqliteArguments): void {
    this.count += 1;
    if (typeof query == 'string') {
      using statement = this.driver.prepare(query);
      if (args) {
        statement.bind(args);
      }
      this.lastPromise = statement.step(undefined, {
        requireTransaction: true
      });
    } else if (query instanceof ConnectionPreparedQueryImpl) {
      const statement = query.statement;
      statement.bind(args ?? []);
      this.lastPromise = statement.step(undefined, {
        requireTransaction: true
      });
      statement.reset();
    } else {
      throw new Error('not implemented yet');
    }
  }

  async flush(): Promise<void> {
    this.count = 0;
    await this.lastPromise;
  }
}
