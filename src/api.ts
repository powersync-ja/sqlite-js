import { SqliteArguments, SqliteValue } from './common.js';
import { SqliteRowObject } from './driver-api.js';

export type SqliteDatabase = SqliteConnectionPool & SqliteConnection;

export interface SqliteConnectionPool {
  /**
   * Reserve a connection for the duration of the callback.
   *
   * @param callback
   * @param options
   */
  withReservedConnection<T>(
    callback: (connection: SqliteConnection) => Promise<T>,
    options?: ReserveConnectionOptions
  ): Promise<T>;

  /**
   * Reserve a connection until released.
   *
   * @param options
   */
  reserveConnection(
    options?: ReserveConnectionOptions
  ): Promise<ReservedSqliteConnection>;

  /**
   * Start a transaction.
   */
  transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options?: TransactionOptions & ReserveConnectionOptions
  ): Promise<T>;

  /**
   * Usage:
   *
   * await using tx = await db.usingTransaction();
   * ...
   * await tx.commit();
   *
   * If commit is not called, the transaction is rolled back automatically.
   */
  usingTransaction(
    options?: TransactionOptions & ReserveConnectionOptions
  ): Promise<SqliteUsingTransaction>;

  close(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
}

export interface ReserveConnectionOptions {
  readonly?: boolean;
}

export interface ReservedSqliteConnection extends SqliteConnection {
  /** Direct handle to the underlying connection. */
  connection: SqliteConnection;

  release(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
}

export interface QueryInterface {
  query<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: ReserveConnectionOptions
  ): SqliteQuery<T>;

  prepare<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments
  ): PreparedQuery<T>;

  /**
   * Convenience method, same as query(query, args).execute(options).
   *
   * When called on a connection pool, uses readonly: false by default.
   */
  execute<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: ExecuteOptions & ReserveConnectionOptions
  ): Promise<ResultSet<T>>;

  /**
   * Convenience method, same as query(query, args).executeStreamed(options).
   */
  executeStreamed<T extends SqliteRowObject>(
    query: string,
    args: SqliteArguments,
    options?: StreamedExecuteOptions & ReserveConnectionOptions
  ): AsyncGenerator<ResultSet<T>>;

  /**
   * Convenience method, same as query(query, args).select(options).
   *
   * When called on a connection pool, uses readonly: true by default.
   */
  select<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: QueryOptions & ReserveConnectionOptions
  ): Promise<T[]>;

  /**
   * Get a single row.
   *
   * Throws an exception if the query returns no results.
   *
   * @param query
   * @param args
   * @param options
   */
  get<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: QueryOptions & ReserveConnectionOptions
  ): Promise<T>;

  /**
   * Get a single row.
   *
   * Returns null if the query returns no results.
   *
   * @param query
   * @param args
   * @param options
   */
  getOptional<T extends SqliteRowObject>(
    query: string,
    args?: SqliteArguments,
    options?: QueryOptions & ReserveConnectionOptions
  ): Promise<T | null>;

  pipeline(options?: ReserveConnectionOptions): QueryPipeline;
}

export interface SqliteConnection extends QueryInterface {
  /**
   * Start a transaction.
   */
  transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options?: TransactionOptions
  ): Promise<T>;

  /**
   * Usage:
   *
   * await using tx = await db.usingTransaction();
   * ...
   * await tx.commit();
   *
   * If commit is not called, the transaction is rolled back automatically.
   */
  usingTransaction(
    options?: TransactionOptions
  ): Promise<SqliteUsingTransaction>;

  /**
   * Listen for individual update events as they occur.
   *
   * For efficiency, multiple updates may be batched together.
   *
   * These events may be batched together for efficiency.
   * Either way, all events in a transaction must be emitted before
   * "onTransactionClose" is emitted for that transaction.
   *
   * Use options.tables to limit the events to specific tables.
   *
   * Use options.batchLimit == 1 to disable event batching.
   */
  onUpdate(
    listener: UpdateListener,
    options?: { tables?: string[]; batchLimit?: number }
  ): () => void;

  /**
   * Listen for transaction events. Fired when either:
   * 1. Transaction is rolled back.
   * 2. Transaction is committed and persisted.
   *
   * @param listener
   */
  onTransactionClose(listener: TransactionCloseListener): () => void;

  /**
   * Listen for committed updates to tables.
   *
   * This can be achieved by combining `onUpdate()` and `onTransactionClose()`, although
   * implementations may optimize this version for large transactions.
   */
  onTablesChanged(listener: TablesChangedListener): () => void;

  close(): Promise<void>;
}

export interface BatchedUpdateEvent {
  events: UpdateEvent[];
}

export interface UpdateEvent {
  table: string;
  type: 'insert' | 'update' | 'delete';
  rowId: bigint;
}

export interface TablesChangedEvent {
  tables: string[];
}

export type UpdateListener = (event: BatchedUpdateEvent) => void;
export type TablesChangedListener = (event: TablesChangedEvent) => void;

export interface TransactionCloseEvent {
  rollback: boolean;
}

export type TransactionCloseListener = (event: TransactionCloseEvent) => void;

export interface SqliteTransaction extends QueryInterface {
  /**
   * Returns true if auto-commit is enabled.
   * This means the database is not currently in a transaction.
   * This may be true even if a transaction lock is still held,
   * when the transaction has been committed or rolled back.
   */
  getAutoCommit(): Promise<boolean>;

  rollback(): Promise<void>;
}

export interface SqliteUsingTransaction extends SqliteTransaction {
  commit(): Promise<void>;

  [Symbol.asyncDispose](): Promise<void>;
}

export interface TransactionOptions {
  /**
   * See SQLite docs on the type.
   *
   * For WAL mode, immediate and exclusive are the same.
   *
   * Read transactions should use "deferred".
   */
  type?: 'exclusive' | 'immediate' | 'deferred';
}

export interface ResultSet<T extends SqliteRowObject = SqliteRowObject> {
  rowId?: number;
  changes?: number;

  columns: (keyof T)[];
  cells: SqliteValue[][];

  /**
   * Convenience method to combine columns and rows into objects.
   */
  rows: T[];
}

export interface SqliteQuery<T extends SqliteRowObject> {
  executeStreamed(options?: StreamedExecuteOptions): AsyncGenerator<ResultSet>;

  execute(options?: ExecuteOptions): Promise<ResultSet<T>>;

  /**
   * Convenience method.
   *
   * Same as execute, but returns an array of row objects directly.
   */
  select(options?: QueryOptions): Promise<T[]>;

  /**
   * Get a single row.
   *
   * Throws an exception if the results contain no rows.
   */
  get(options?: QueryOptions): Promise<T>;

  /**
   * Get a single row.
   *
   * Returns null if the results contain no rows.
   */
  getOptional(options?: QueryOptions): Promise<T | null>;
}

export interface PreparedQuery<T extends SqliteRowObject> {
  parse(): Promise<{ columns: string[] }>;

  executeStreamed(
    args?: SqliteArguments,
    options?: StreamedExecuteOptions
  ): AsyncGenerator<ResultSet>;

  execute(
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): Promise<ResultSet<T>>;

  /**
   * Convenience method.
   *
   * Same as execute, but returns an array of row objects directly.
   */
  select(args?: SqliteArguments, options?: QueryOptions): Promise<T[]>;

  dispose(): void;
  [Symbol.dispose](): void;
}

export interface QueryOptions {
  /** true to return all integers as bigint */
  bigint?: boolean;
}

export interface ExecuteOptions extends QueryOptions {
  includeRowId?: boolean;
  includeChanges?: boolean;
}

export interface StreamedExecuteOptions extends ExecuteOptions {
  /** Size limit in bytes for each chunk */
  chunkSize?: number;
}

export interface QueryPipeline {
  /**
   * Enqueue a query.
   */
  execute(query: string | PreparedQuery<any>, args?: SqliteArguments): void;

  /**
   * Flush all existing queries, wait for the last query to complete.
   *
   * TODO: define error handling.
   */
  flush(): Promise<void>;

  readonly count: number;
}
