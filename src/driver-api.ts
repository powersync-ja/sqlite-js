import { SqliteArguments, SqliteValue } from './common.js';

export enum SqliteCommandType {
  prepare = 1,
  bind = 2,
  step = 3,
  reset = 4,
  finalize = 5,
  sync = 6,
  parse = 7
}

export interface SqliteCommandResponse {
  error?: {
    message: string;
    code: string;
    stack?: string;
  };
  skipped?: true;
}

export interface SqliteBaseCommand {
  type: SqliteCommandType;
}

export interface SqlitePrepare extends SqliteBaseCommand {
  type: SqliteCommandType.prepare;
  id: number;
  sql: string;
  bigint?: boolean;
}

export interface SqliteParseResponse extends SqliteCommandResponse {
  columns: string[];
}

export type SqliteParameterBinding =
  | (SqliteValue | undefined)[]
  | Record<string, SqliteValue>
  | null
  | undefined;

export interface SqliteBind extends SqliteBaseCommand {
  type: SqliteCommandType.bind;
  id: number;
  parameters: SqliteParameterBinding;
}

export interface SqliteParse extends SqliteBaseCommand {
  type: SqliteCommandType.parse;
  id: number;
}

export interface SqliteStep extends SqliteBaseCommand {
  type: SqliteCommandType.step;
  id: number;
  n?: number;
}

export interface SqliteStepResponse extends SqliteCommandResponse {
  rows?: SqliteValue[][];
  done?: boolean;
}

export interface SqliteReset extends SqliteBaseCommand {
  type: SqliteCommandType.reset;
  id: number;
  clear_bindings?: boolean;
}

export interface SqliteFinalize extends SqliteBaseCommand {
  type: SqliteCommandType.finalize;
  id: number;
}

export interface SqliteSync {
  type: SqliteCommandType.sync;
}

export type SqliteCommand =
  | SqlitePrepare
  | SqliteBind
  | SqliteStep
  | SqliteReset
  | SqliteFinalize
  | SqliteSync
  | SqliteParse;

export type InferCommandResult<T extends SqliteCommand> =
  T extends SqlitePrepare
    ? SqliteCommandResponse
    : T extends SqliteStep
      ? SqliteStepResponse
      : T extends SqliteParse
        ? SqliteParseResponse
        : SqliteCommandResponse;

export type InferBatchResult<T extends SqliteCommand[]> = {
  [i in keyof T]: InferCommandResult<T[i]>;
};

export interface PrepareOptions {
  bigint?: boolean;
}

export interface ResetOptions {
  clear_bindings?: boolean;
}

export interface SqliteDriverConnection {
  /**
   * Prepare a statement.
   *
   * Does not return any errors.
   */
  prepare(sql: string, options?: PrepareOptions): SqliteDriverStatement;

  sync(): Promise<void>;

  onUpdate(
    listener: UpdateListener,
    options?: { tables?: string[]; batchLimit?: number }
  ): () => void;

  close(): Promise<void>;
}

export interface SqliteDriverStatement {
  getColumns(): Promise<string[]>;

  bind(parameters: SqliteParameterBinding): void;
  step(n?: number): Promise<SqliteStepResponse>;
  finalize(): void;
  reset(options?: ResetOptions): void;
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

  release(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
}

export interface ReserveConnectionOptions {
  readonly?: boolean;
  signal?: AbortSignal;
}

export interface RunResults {
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
