import { SqliteArguments, SqliteValue } from "./common.js";

export interface SqlitePrepare {
  prepare: {
    id: number;
    sql: string;
  };
}

export interface SqliteBind {
  bind: {
    id: number;
    parameters: SqliteValue[] | Record<string, SqliteValue>;
  };
}

export interface SqliteStep {
  step: {
    id: number;
    n?: number;
    all?: boolean;
    bigint?: boolean;
  };
}

export interface SqliteReset {
  reset: {
    id: number;
    clear_bindings?: boolean;
  };
}

export interface SqliteFinalize {
  finalize: {
    id: number;
  };
}

export interface SqliteSync {
  sync: {};
}
export interface SqliteChanges {
  changes: {};
}
export interface SqliteTotalChanges {
  total_changes: {};
}
export interface SqliteLastInsertRowId {
  last_insert_row_id: {};
}

export type SqliteCommand =
  | SqlitePrepare
  | SqliteBind
  | SqliteStep
  | SqliteReset
  | SqliteFinalize
  | SqliteSync
  | SqliteChanges
  | SqliteTotalChanges
  | SqliteLastInsertRowId;

export interface SqliteCommandBatch {
  commands: SqliteCommand[];
}

export type CommandResult = {};

export interface SqliteBatchResult {
  results: CommandResult[];
}

export interface SqliteDriverConnection {
  execute(commands: SqliteCommand[]): Promise<CommandResult[]>;

  run(query: string, args?: SqliteArguments): Promise<void>;
  runWithResults(query: string, args?: SqliteArguments): Promise<RunResults>;

  selectStreamed(
    query: string,
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): AsyncIterable<ResultSet>;

  selectAll(
    query: string,
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): Promise<ResultSet>;

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
  type: "insert" | "update" | "delete";
  rowId: bigint;
}

export interface ReservedConnection {
  connection: SqliteDriverConnection;
  release(): void;
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
