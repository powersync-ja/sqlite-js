import { SqliteArguments, SqliteValue } from "./common.js";

export interface SqliteDriverConnection {
  prepare(query: string): SqliteDriverStatement;
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
}

export interface ReservedConnection {
  connection: SqliteDriverConnection;
  release(): void;
}

export interface ReserveConnectionOptions {
  readonly?: boolean;
  signal?: AbortSignal;
}

export interface SqliteDriverStatement {
  run(args?: SqliteArguments): Promise<void>;
  runWithResults(args?: SqliteArguments): Promise<RunResults>;

  selectStreamed(
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): AsyncIterable<ResultSet>;

  selectAll(
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): Promise<ResultSet>;

  dispose(): void;
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
