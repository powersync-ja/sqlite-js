import {
  BatchedUpdateEvent,
  ExecuteOptions,
  PreparedQuery,
  QueryOptions,
  ReserveConnectionOptions,
  ResultSet,
  SqliteConnection,
  SqliteConnectionPool,
  SqliteQuery,
  SqliteTransaction,
  StreamedExecuteOptions,
  TablesChangedEvent,
  TransactionCloseEvent,
  TransactionOptions,
} from './api';
import { SqliteArguments, SqliteValue } from './common';
import {
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
} from './driver-api';

export class ConnectionPoolImpl implements SqliteConnectionPool {
  constructor(private driver: SqliteDriverConnectionPool) {}

  async reserveConnection<T>(
    callback: (connection: SqliteConnection) => Promise<T>,
    options?: ReserveConnectionOptions | undefined
  ): Promise<T> {
    const con = await this.driver.reserveConnection(options ?? {});
    try {
      return await callback(new ConnectionImpl(con.connection));
    } finally {
      con.release();
    }
  }
}

export class ConnectionImpl implements SqliteConnection {
  constructor(private driver: SqliteDriverConnection) {}

  async transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options: TransactionOptions
  ): Promise<T> {
    await this.driver.prepare('BEGIN').execute();
    try {
      const tx = new TransactionImpl(this);
      await callback(tx);

      await this.driver.prepare('COMMIT').execute();
    } catch (e) {
      await this.driver.prepare('ROLLBACK').execute();
    }
    throw new Error('Method not implemented.');
  }

  onUpdate(
    listener: (event: BatchedUpdateEvent) => void,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    throw new Error('Method not implemented.');
  }
  onTransactionClose(
    listener: (event: TransactionCloseEvent) => void
  ): () => void {
    throw new Error('Method not implemented.');
  }
  onTablesChanged(listener: (event: TablesChangedEvent) => void): () => void {
    throw new Error('Method not implemented.');
  }
  close(): Promise<void> {
    throw new Error('Method not implemented.');
  }
  prepare<T>(query: string): PreparedQuery<T> {
    throw new Error('Method not implemented.');
  }
  query<T>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments
  ): SqliteQuery<T> {
    throw new Error('Method not implemented.');
  }
  async execute<T>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments | undefined,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    let result: ResultSet<T> | null = null;

    const q = this.driver.prepare(query as string);

    for await (let rs of q.stream(args)) {
      if (result == null) {
        result = new ResultSetImpl(rs.columns, [...rs.rows]);
      } else {
        result.rows.push(...rs.rows);
      }
    }
    return result!;
  }

  async *executeStreamed<T>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments | undefined,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, void, unknown> {
    const q = this.driver.prepare(query as string);

    for await (let rs of q.stream(args, options)) {
      yield new ResultSetImpl(rs.columns, rs.rows);
    }
  }

  async select<T>(
    query: string | PreparedQuery<T>,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    const rs = await this.execute(query, args, options);
    return rs.rowObjects;
  }
}

export class TransactionImpl implements SqliteTransaction {
  constructor(private con: ConnectionImpl) {}

  getAutoCommit(): Promise<boolean> {
    throw new Error('Method not implemented.');
  }

  async rollback(): Promise<void> {
    await this.select('ROLLBACK');
  }

  prepare<T>(query: string): PreparedQuery<T> {
    return this.con.prepare(query);
  }

  query<T>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments
  ): SqliteQuery<T> {
    return this.con.query(query, args);
  }

  execute<T>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments | undefined,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    return this.con.execute(query, args);
  }

  executeStreamed<T>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments | undefined,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, any, unknown> {
    return this.con.executeStreamed(query, args, options);
  }

  select<T>(
    query: string | PreparedQuery<T>,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    return this.con.select(query, args, options);
  }
}

class ResultSetImpl<T> implements ResultSet<T> {
  columns: (keyof T)[];
  rows: SqliteValue[][];
  private _rowObjects: T[] | undefined;

  constructor(columns: string[], rows?: SqliteValue[][]) {
    this.columns = columns as any[] as (keyof T)[];
    this.rows = rows ?? [];
  }

  get rowObjects() {
    if (this._rowObjects == null) {
      this._rowObjects = this.rows.map((row) => {
        return Object.fromEntries(
          this.columns.map((column, i) => {
            return [column, row[i]];
          })
        ) as T;
      });
    }
    return this._rowObjects;
  }
}
