import { SqliteArguments, SqliteValue } from './common.js';

export enum SqliteCommandType {
  prepare = 1,
  bind = 2,
  step = 3,
  reset = 4,
  finalize = 5,
  sync = 6
}

export interface SqliteCommandResponse {
  error?: {
    message: string;
    code: string;
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
}

export interface SqlitePrepareResponse extends SqliteCommandResponse {
  columns?: string[];
}

export interface SqliteBind extends SqliteBaseCommand {
  type: SqliteCommandType.bind;
  id: number;
  parameters:
    | (SqliteValue | undefined)[]
    | Record<string, SqliteValue>
    | null
    | undefined;
}

export interface SqliteStep extends SqliteBaseCommand {
  type: SqliteCommandType.step;
  id: number;
  n?: number;
  all?: boolean;
  bigint?: boolean;
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
  | SqliteSync;

export type InferCommandResult<T extends SqliteCommand> =
  T extends SqlitePrepare
    ? SqlitePrepareResponse
    : T extends SqliteStep
      ? SqliteStepResponse
      : SqliteCommandResponse;

export type InferBatchResult<T extends SqliteCommand[]> = {
  [i in keyof T]: InferCommandResult<T[i]>;
};

export interface SqliteDriverConnection {
  execute<const T extends SqliteCommand[]>(
    commands: T
  ): Promise<InferBatchResult<T>>;

  onUpdate(
    listener: UpdateListener,
    options?: { tables?: string[]; batchLimit?: number }
  ): () => void;

  close(): Promise<void>;
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

export interface ReservedConnection extends SqliteDriverConnection {
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
