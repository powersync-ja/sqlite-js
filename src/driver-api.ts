import { SqliteArguments, SqliteValue } from './common';

export interface SqliteDriverConnection {
  prepare(query: string): SqliteDriverStatement;
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
  execute(args?: SqliteArguments): Promise<void>;

  stream(
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): AsyncIterable<ResultSet>;

  dispose(): void;
}

export interface ResultSet {
  columns: string[];
  rows: SqliteValue[][];
}

export interface ExecuteOptions {
  chunkSize?: number;
}
