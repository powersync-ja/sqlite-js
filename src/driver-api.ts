import { SqliteValue } from './common.js';

export type SqliteRowRaw = SqliteValue[];
export type SqliteRowObject = Record<string, SqliteValue>;
export type SqliteRow = SqliteRowRaw | SqliteRowObject;

export interface PrepareOptions {
  bigint?: boolean;
  rawResults?: boolean;
  persist?: boolean;
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
  | (SqliteValue | undefined)[]
  | Record<string, SqliteValue>
  | null
  | undefined;

export interface SqliteStepResult {
  rows?: SqliteRow[];
  done?: boolean;
}

export interface SqliteDriverStatement {
  getColumns(): Promise<string[]>;

  bind(parameters: SqliteParameterBinding): void;
  step(n?: number, options?: StepOptions): Promise<SqliteStepResult>;
  finalize(): void;
  reset(options?: ResetOptions): void;

  /**
   * Similar to step, followed by reset, and returning number of changed rows.
   *
   * Avoids the need to use a separate statement to get changes.
   */
  run(options?: StepOptions): Promise<SqliteRunResult>;

  [Symbol.dispose](): void;
}

export interface StepOptions {
  requireTransaction?: boolean;
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

export interface SqliteRunResult {
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
