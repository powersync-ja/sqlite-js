import { SqliteArguments, SqliteValue } from './common.js';

export type SqliteDatabase = SqliteConnectionPool & SqliteConnection;

export interface SqliteConnectionPool {
  /**
   * Reserve a connection for the duration of the callback.
   *
   * @param callback
   * @param options
   */
  reserveConnection<T>(
    callback: (connection: SqliteConnection) => Promise<T>,
    options?: ReserveConnectionOptions
  ): Promise<T>;
}

export interface ReserveConnectionOptions {
  readonly?: boolean;
}

export interface QueryInterface {
  /**
   * Advanced usage: Prepare a query. The query can be executed directly on the connection,
   * or in a transaction.
   *
   * The query must be disposed before closing the connection.
   */
  prepare<T>(query: string): PreparedQuery<T>;

  /**
   * Convenience method, same as query(query, args).execute(options).
   *
   * When called on a connection pool, uses readonly: true by default.
   */
  execute<T>(
    query: string | PreparedQuery<T>,
    args?: SqliteArguments | undefined,
    options?: ExecuteOptions & ReserveConnectionOptions
  ): Promise<ResultSet<T>>;

  /**
   * Convenience method, same as query(query, args).executeStreamed(options).
   */
  executeStreamed<T>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments | undefined,
    options?: StreamedExecuteOptions & ReserveConnectionOptions
  ): AsyncGenerator<ResultSet<T>>;

  /**
   * Convenience method, same as query(query, args).select(options).
   *
   * When called on a connection pool, uses readonly: true by default.
   */
  select<T>(
    query: string | PreparedQuery<T>,
    args?: SqliteArguments | undefined,
    options?: QueryOptions & ReserveConnectionOptions
  ): Promise<T[]>;
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

export interface ResultSet<T = any> {
  rowId?: number;
  changes?: number;

  columns: (keyof T)[];
  raw_rows: SqliteValue[][];

  /**
   * Convenience method to combine columns and rows into objects.
   */
  rows: T[];
}

export interface SqliteQuery<T> {
  // Implementation note: The implementation only needs to provide one execute method,
  // The rest can be provided by utilities.

  executeStreamed(options?: StreamedExecuteOptions): AsyncGenerator<ResultSet>;

  /**
   * Convenience method.
   */
  execute(options?: ExecuteOptions): Promise<ResultSet<T>>;

  /**
   * Convenience method.
   *
   * Same as execute, but returns an array of row objects directly.
   */
  select(options?: QueryOptions): Promise<T[]>;
}

export interface PreparedQuery<T> {
  dispose(): Promise<void>;
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
