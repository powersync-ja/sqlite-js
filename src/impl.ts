import {
  BatchedUpdateEvent,
  ExecuteOptions,
  PreparedQuery,
  QueryInterface,
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
  TransactionOptions
} from './api.js';
import { SqliteArguments, SqliteValue } from './common.js';
import {
  SqliteDriverConnection,
  SqliteDriverConnectionPool
} from './driver-api.js';

export class ConnectionPoolImpl
  implements SqliteConnectionPool, QueryInterface
{
  constructor(private driver: SqliteDriverConnectionPool) {}
  prepare<T>(query: string): PreparedQuery<T> {
    throw new Error('Method not implemented.');
  }

  execute<T>(
    query: string | PreparedQuery<T>,
    args?: SqliteArguments | undefined,
    options?: (ExecuteOptions & ReserveConnectionOptions) | undefined
  ): Promise<ResultSet<T>> {
    return this.reserveConnection((con) => {
      return con.execute(query, args, options);
    });
  }

  async *executeStreamed<T>(
    query: string | PreparedQuery<T>,
    args?: SqliteArguments | undefined,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, any, unknown> {
    const con = await this.driver.reserveConnection(options ?? {});
    try {
      const c2 = new ConnectionImpl(con);
      for await (let chunk of c2.executeStreamed(query, args, options)) {
        yield chunk;
      }
    } finally {
      con.release();
    }
  }

  select<T>(
    query: string | PreparedQuery<T>,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    return this.reserveConnection(
      (con) => {
        return con.select(query, args, options);
      },
      { readonly: true }
    );
  }

  async reserveConnection<T>(
    callback: (connection: SqliteConnection) => Promise<T>,
    options?: ReserveConnectionOptions | undefined
  ): Promise<T>;

  async reserveConnection(
    options?: ReserveConnectionOptions | undefined
  ): Promise<SqliteConnection>;

  async reserveConnection<T>(
    callback:
      | ((connection: SqliteConnection) => Promise<T>)
      | ReserveConnectionOptions
      | undefined,
    options?: ReserveConnectionOptions | undefined
  ): Promise<T | SqliteConnection> {
    if (typeof callback == 'function') {
      const con = await this.driver.reserveConnection(options ?? {});
      try {
        return await callback(new ConnectionImpl(con));
      } finally {
        con.release();
      }
    } else {
      const con = await this.driver.reserveConnection(callback ?? {});
      return new ConnectionImpl(con);
    }
  }

  close(): Promise<void> {
    return this.driver.close();
  }
}

export class ConnectionImpl implements SqliteConnection {
  constructor(private driver: SqliteDriverConnection) {}

  release(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  [Symbol.dispose](): void {
    // throw new Error('Method not implemented.');
  }

  async transaction<T>(
    callback: (tx: SqliteTransaction) => Promise<T>,
    options: TransactionOptions
  ): Promise<T> {
    await run(this.driver, 'BEGIN');
    try {
      const tx = new TransactionImpl(this);
      const result = await callback(tx);

      await run(this.driver, 'COMMIT');
      return result;
    } catch (e) {
      await run(this.driver, 'ROLLBACK');
      throw e;
    }
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

    for await (let rs of this.executeStreamed<any>(query, args, options)) {
      if (result == null) {
        result = rs;
      } else {
        result.raw_rows.push(...rs.raw_rows);
        result.changes = rs.changes ?? result.changes;
        result.rowId = rs.rowId ?? result.rowId;
      }
    }
    return result!;
  }

  async *executeStreamed<T>(
    query: string | PreparedQuery<T>,
    args: SqliteArguments | undefined,
    options?: (StreamedExecuteOptions & ReserveConnectionOptions) | undefined
  ): AsyncGenerator<ResultSet<T>, void, unknown> {
    try {
      const [{ columns, error }, { error: error2 }] =
        (await this.driver.execute([
          { prepare: { id: 0, sql: query as string } },
          { bind: { id: 0, parameters: args ?? [] } }
        ])) as any[];
      if (error != null) {
        throw error;
      }
      if (error2 != null) {
        throw error2;
      }

      while (true) {
        const [{ rows, error, done }] = (await this.driver.execute([
          { step: { id: 0, n: 10, bigint: options?.bigint } }
        ])) as any[];
        if (error != null) {
          throw error;
        }
        yield new ResultSetImpl(columns, rows);
        if (done) {
          break;
        }
      }

      if (options?.includeChanges) {
        const [[changes, rowid]] = await run(
          this.driver,
          'select changes(), last_insert_rowid()'
        );
        const rs = new ResultSetImpl<T>(columns, []);
        rs.changes = changes as number;
        rs.rowId = rowid as number;
        yield rs;
      }
    } finally {
      await this.driver.execute([{ sync: {} }]);
    }
  }

  async select<T>(
    query: string | PreparedQuery<T>,
    args?: SqliteArguments | undefined,
    options?: (QueryOptions & ReserveConnectionOptions) | undefined
  ): Promise<T[]> {
    const rs = await this.execute(query, args, options);
    return rs.rows;
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
  raw_rows: SqliteValue[][];
  private _rowObjects: T[] | undefined;

  rowId?: number;
  changes?: number;

  constructor(columns: string[], rows?: SqliteValue[][]) {
    this.columns = columns as any[] as (keyof T)[];
    this.raw_rows = rows ?? [];
  }

  get rows() {
    if (this._rowObjects == null) {
      this._rowObjects = this.raw_rows.map((row) => {
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

async function run(con: SqliteDriverConnection, sql: string) {
  const [{ columns }, { rows }, { error }] = (await con.execute([
    { prepare: { id: 0, sql } },
    { step: { id: 0, all: true } },
    { sync: {} }
  ])) as any[];
  if (error != null) {
    throw error;
  }
  return rows as SqliteValue[][];
}
