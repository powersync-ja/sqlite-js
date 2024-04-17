import { SqliteArguments, SqliteValue } from "./common.js";

export interface SqliteDriverConnection {
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
