export type SqliteValue = null | string | number | bigint | Uint8Array;
export type SqliteArguments =
  | SqliteValue[]
  | Record<string, SqliteValue>
  | null
  | undefined;

export type SqliteArrayRow = SqliteValue[];
export type SqliteObjectRow = Record<string, SqliteValue>;

export interface PrepareOptions {
  autoFinalize?: boolean;
}

export interface ResetOptions {
  clearBindings?: boolean;
}

export interface SqliteDriverConnection {
  /**
   * Prepare a statement.
   *
   * Does not return any errors.
   */
  prepare(sql: string, options?: PrepareOptions): SqliteDriverStatement;

  onUpdate(
    listener: UpdateListener,
    options?: { tables?: string[]; batchLimit?: number }
  ): () => void;

  close(): Promise<void>;
}

export type SqliteParameterBinding =
  | SqliteValue[]
  | Record<string, SqliteValue>
  | null
  | undefined;

export interface QueryOptions {
  requireTransaction?: boolean;
  bigint?: boolean;
}

export interface StreamQueryOptions extends QueryOptions {
  chunkMaxRows?: number;
  chunkMaxSize?: number;
}

export interface SqliteDriverStatement {
  all(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteObjectRow[]>;
  allArray(
    parameters: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteArrayRow[]>;

  stream(
    parameters?: SqliteParameterBinding,
    options?: StreamQueryOptions
  ): AsyncIterator<SqliteObjectRow[]>;
  streamArray(
    parameters?: SqliteParameterBinding,
    options?: StreamQueryOptions
  ): AsyncIterator<SqliteArrayRow[]>;

  /**
   * Run a query, and return the number of changed rows, and last insert id.
   */
  run(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteChanges>;

  getColumns(): Promise<string[]>;

  finalize(): void;
  [Symbol.dispose](): void;
}

export interface SqliteDriverConnectionPool {
  /**
   * Reserve a connection for exclusive use.
   *
   * If there is no available connection, this will wait until one is available.
   * @param options
   */
  reserveConnection(
    options?: ReserveConnectionOptions
  ): Promise<ReservedConnection>;

  close(): Promise<void>;

  [Symbol.asyncDispose](): Promise<void>;

  onUpdate(
    listener: UpdateListener,
    options?: { tables?: string[]; batchLimit?: number }
  ): () => void;
}

export type UpdateListener = (event: BatchedUpdateEvent) => void;

export interface BatchedUpdateEvent {
  events: UpdateEvent[];
}

export interface UpdateEvent {
  table: string;
  type: 'insert' | 'update' | 'delete';
  rowId: bigint;
}

export interface ReservedConnection {
  /** Direct handle to the underlying connection. */
  connection: SqliteDriverConnection;

  /** Proxied to the underlying connection */
  prepare(sql: string, options?: PrepareOptions): SqliteDriverStatement;

  release(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
}

export interface ReserveConnectionOptions {
  readonly?: boolean;
  signal?: AbortSignal;
}

export interface SqliteChanges {
  changes: number;
  lastInsertRowId: bigint;
}

export interface ResultSet {
  columns: string[];
  rows: SqliteValue[][];
}

export interface ExecuteOptions {
  chunkSize?: number;
  bigint?: boolean;
}
