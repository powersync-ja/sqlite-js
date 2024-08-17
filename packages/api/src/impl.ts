import {
  BatchedUpdateEvent,
  PreparedQuery,
  QueryInterface,
  QueryOptions,
  QueryPipeline,
  ReserveConnectionOptions,
  ReservedSqliteConnection,
  RunResult,
  SqliteBeginTransaction,
  SqliteConnection,
  SqliteConnectionPool,
  SqliteTransaction,
  StreamOptions,
  TablesChangedEvent,
  TablesChangedListener,
  TransactionCloseEvent,
  TransactionCloseListener,
  TransactionOptions,
  UpdateListener
} from './api.js';
import { SqliteArguments } from '@sqlite-js/driver';
import { Deferred } from './deferred.js';
import {
  PrepareOptions,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqliteDriverStatement,
  SqliteRowObject
} from '@sqlite-js/driver';

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

  onUpdate(
    listener: UpdateListener,
    options?: { tables?: string[]; batchLimit?: number }
  ): () => void {
    throw new Error('Method not implemented.');
  }
  onTransactionClose(listener: TransactionCloseListener): () => void {
    throw new Error('Method not implemented.');
  }
  onTablesChanged(listener: TablesChangedListener): () => void {
    throw new Error('Method not implemented.');
  }

  prepare<T extends SqliteRowObject>(
    sql: string,
    args?: SqliteArguments
  ): PreparedQuery<T> {
    return new ConnectionPoolPreparedQueryImpl<T>(this, sql, args);
  }

  pipeline(options?: ReserveConnectionOptions | undefined): QueryPipeline {
    throw new Error('pipeline not supported here');
  }

  async run(
    query: string,
    args?: SqliteArguments,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<RunResult> {
    const r = await this.reserveConnection(options);
    try {
      return r.connection.run(query, args, options);
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
      return await r.transaction(callback, {
        type: options?.type ?? (options?.readonly ? 'deferred' : 'exclusive')
      });
    } finally {
      await r.release();
    }
  }

  async begin<T>(
    options?: (TransactionOptions & ReserveConnectionOptions) | undefined
  ): Promise<SqliteBeginTransaction> {
    const r = await this.reserveConnection(options);
    const tx = await r.connection.begin(options);
    (tx as BeginTransactionImpl).onComplete.finally(() => {
      return r.release();
    });
    return tx;
  }

  async *stream<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: (StreamOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<T[], any, unknown> {
    const r = await this.reserveConnection(options);
    try {
      return r.stream<T>(query, args, options);
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
      return r.select<T>(query, args, options);
    } finally {
      await r.release();
    }
  }

  async get<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T> {
    const r = await this.reserveConnection({ readonly: true, ...options });
    try {
      return r.connection.get<T>(query, args, options);
    } finally {
      await r.release();
    }
  }

  async getOptional<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T | null> {
    const r = await this.reserveConnection({ readonly: true, ...options });
    try {
      return r.connection.getOptional<T>(query, args, options);
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

  prepare<T extends SqliteRowObject>(
    sql: string,
    args?: SqliteArguments,
    options?: QueryOptions
  ): PreparedQuery<T> {
    return this.connection.prepare(sql, args, options);
  }

  pipeline(): QueryPipeline {
    return this.connection.pipeline();
  }

  transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options?: TransactionOptions | undefined
  ): Promise<T> {
    return this.connection.transaction(callback, options);
  }

  begin(
    options?: TransactionOptions | undefined
  ): Promise<SqliteBeginTransaction> {
    return this.connection.begin(options);
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

  run(query: string, args?: SqliteArguments | undefined): Promise<RunResult> {
    return this.connection.run(query, args);
  }

  stream<T extends SqliteRowObject>(
    query: string,
    args: SqliteArguments | undefined,
    options?: StreamOptions | undefined
  ): AsyncGenerator<T[], any, unknown> {
    return this.connection.stream(query, args, options);
  }

  select<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments | undefined,
    options?: QueryOptions | undefined
  ): Promise<T[]> {
    return this.connection.select(query, args, options);
  }

  get<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: QueryOptions
  ): Promise<T> {
    return this.connection.get(query, args, options);
  }

  getOptional<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: QueryOptions
  ): Promise<T | null> {
    return this.connection.getOptional(query, args, options);
  }
}

export class ConnectionImpl implements SqliteConnection {
  private _begin: PreparedQuery<{}> | undefined;
  private _beginExclusive: PreparedQuery<{}> | undefined;
  public commit: PreparedQuery<{}> | undefined;
  public rollback: PreparedQuery<{}> | undefined;

  constructor(private driver: SqliteDriverConnection) {}

  private init() {
    this._beginExclusive ??= this.prepare('BEGIN EXCLUSIVE', undefined, {
      persist: true
    });
    this._begin ??= this.prepare('BEGIN', undefined, { persist: true });
    this.commit ??= this.prepare('COMMIT', undefined, { persist: true });
    this.rollback ??= this.prepare('ROLLBACK', undefined, { persist: true });
  }

  async begin(options?: TransactionOptions): Promise<SqliteBeginTransaction> {
    await this.init();

    if ((options?.type ?? 'exclusive') == 'exclusive') {
      await this._beginExclusive!.select();
    } else {
      await this._begin!.select();
    }

    return new BeginTransactionImpl(this);
  }

  async transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options?: TransactionOptions
  ): Promise<T> {
    this.init();

    if ((options?.type ?? 'exclusive') == 'exclusive') {
      await this._beginExclusive!.select();
    } else {
      await this._begin!.select();
    }
    try {
      const tx = new TransactionImpl(this);
      const result = await callback(tx);

      await this.commit!.select();
      return result;
    } catch (e) {
      await this.rollback!.select();
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
    this._beginExclusive?.dispose();
    this._begin?.dispose();
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

  pipeline(): QueryPipeline {
    return new QueryPipelineImpl(this.driver);
  }

  async run(query: string, args: SqliteArguments): Promise<RunResult> {
    using statement = this.driver.prepare(query);
    if (args != null) {
      statement.bind(args);
    }
    return await statement.run();
  }

  async *stream<T extends SqliteRowObject>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments | undefined,
    options?: StreamOptions | undefined
  ): AsyncGenerator<T[], void, unknown> {
    using statement = this.driver.prepare(query as string, {
      bigint: options?.bigint
    });
    if (args != null) {
      statement.bind(args);
    }
    const chunkSize = options?.chunkSize ?? 100;

    while (true) {
      const { rows, done } = await statement.step(chunkSize);
      if (rows != null) {
        yield rows as T[];
      }
      if (done) {
        break;
      }
    }
  }

  async select<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    using statement = this.driver.prepare(query, {
      bigint: options?.bigint,
      rawResults: false
    });
    if (args != null) {
      statement.bind(args);
    }
    const { rows } = await statement.step();
    return rows as T[];
  }

  async get<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T> {
    const row = await this.getOptional<T>(query, args, options);
    if (row == null) {
      throw new Error('Query returned 0 rows');
    }
    return row;
  }

  async getOptional<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T | null> {
    const rows = await this.select<T>(query, args, options);
    return rows[0];
  }
}

export class TransactionImpl implements SqliteTransaction {
  private preparedQueries: PreparedQuery<any>[] = [];

  constructor(public con: ConnectionImpl) {}

  getAutoCommit(): Promise<boolean> {
    throw new Error('Method not implemented.');
  }

  async rollback(): Promise<void> {
    await this.con.rollback!.select();
  }

  prepare<T extends SqliteRowObject>(
    sql: string,
    args?: SqliteArguments,
    options?: QueryOptions
  ): PreparedQuery<T> {
    const q = this.con.prepare<T>(sql, args, options);
    // FIXME: auto-dispose these after transaction commit / rollback
    this.preparedQueries.push(q);
    return q;
  }

  pipeline(): QueryPipeline {
    return this.con.pipeline();
  }

  run(query: string, args: SqliteArguments): Promise<RunResult> {
    return this.con.run(query, args);
  }

  stream<T extends SqliteRowObject>(
    query: string,
    args: SqliteArguments,
    options?: StreamOptions | undefined
  ): AsyncGenerator<T[], any, unknown> {
    return this.con.stream(query, args, options);
  }

  select<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: QueryOptions | undefined
  ): Promise<T[]> {
    return this.con.select(query, args, options);
  }

  get<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: QueryOptions | undefined
  ): Promise<T> {
    return this.con.get(query, args, options);
  }

  getOptional<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: QueryOptions | undefined
  ): Promise<T | null> {
    return this.con.getOptional(query, args, options);
  }
}

class BeginTransactionImpl
  extends TransactionImpl
  implements SqliteBeginTransaction
{
  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;

  private didCommit = false;

  private completeDeferred = new Deferred<void>();
  private didGetDispose = false;

  get onComplete(): Promise<void> {
    return this.completeDeferred.promise;
  }

  constructor(connection: ConnectionImpl) {
    super(connection);
    if (typeof Symbol.asyncDispose != 'undefined') {
      Object.defineProperty(this, Symbol.asyncDispose, {
        configurable: false,
        enumerable: false,
        get: () => {
          this.didGetDispose = true;
          return this.dispose;
        }
      });
    }
  }

  private checkDispose() {
    if (!this.didGetDispose) {
      throw new Error(
        'Transaction dispose handler is not registered. Usage:\n  await using tx = await db.begin()'
      );
    }
  }

  async select<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    this.checkDispose();
    return super.select(query, args, options);
  }

  async commit(): Promise<void> {
    this.checkDispose();
    if (this.didCommit) {
      return;
    }
    await this.con.commit!.select();
    this.didCommit = true;
    this.completeDeferred.resolve();
  }

  async rollback(): Promise<void> {
    if (this.didCommit) {
      return;
    }
    await super.rollback();

    this.didCommit = true;
    this.completeDeferred.resolve();
  }

  async dispose(): Promise<void> {
    await this.rollback();
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

  async *stream(
    args?: SqliteArguments,
    options?: StreamOptions | undefined
  ): AsyncGenerator<T[], any, unknown> {
    const r = await this.context.reserveConnection();
    try {
      const q = this.cachedQuery(r);
      yield* q.stream(args, options);
    } finally {
      await r.release();
    }
  }

  async run(args?: SqliteArguments): Promise<RunResult> {
    const r = await this.context.reserveConnection();
    try {
      const q = this.cachedQuery(r);
      return q.run(args);
    } finally {
      await r.release();
    }
  }

  async select(args?: SqliteArguments): Promise<T[]> {
    const r = await this.context.reserveConnection();
    try {
      const q = this.cachedQuery(r);
      return q.select(args);
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

  async *stream(
    args?: SqliteArguments,
    options?: StreamOptions | undefined
  ): AsyncGenerator<T[], any, unknown> {
    const chunkSize = options?.chunkSize ?? 10;
    if (args != null) {
      this.statement.bind(args);
    }
    try {
      while (true) {
        const { rows, done } = await this.statement.step(chunkSize);
        if (rows != null) {
          yield rows as T[];
        }
        if (done) {
          break;
        }
      }
    } finally {
      this.statement.reset();
    }
  }

  async run(args?: SqliteArguments): Promise<RunResult> {
    if (args != null) {
      this.statement.bind(args);
    }
    return await this.statement.run();
  }

  async select(args?: SqliteArguments): Promise<T[]> {
    try {
      if (args != null) {
        this.statement.bind(args);
      }
      const { rows } = await this.statement.step();
      return rows as T[];
    } finally {
      this.statement.reset();
    }
  }

  dispose(): void {
    this.statement.finalize();
  }
}

class QueryPipelineImpl implements QueryPipeline {
  count: number = 0;
  private lastPromise: Promise<any> | undefined = undefined;

  constructor(private driver: SqliteDriverConnection) {}

  run(query: string | PreparedQuery<any>, args?: SqliteArguments): void {
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
