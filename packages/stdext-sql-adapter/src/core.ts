import type {
  ArrayRow,
  Row,
  SqlClient,
  SqlClientPool,
  SqlConnectionOptions,
  SqlPoolClient,
  SqlPoolClientOptions,
  SqlPreparable,
  SqlPreparedStatement,
  SqlQueriable,
  SqlQueryOptions,
  SqlTransaction,
  SqlTransactionable,
  SqlTransactionOptions
} from '@stdext/sql';

import type { DatabaseOpenOptions } from './database.js';
import {
  SqliteCloseEvent,
  SqliteConnectEvent,
  SqliteEvents,
  SqliteEventTarget
} from './events.js';
import {
  SqliteConnectable,
  SqliteConnection,
  type SqliteConnectionOptions
} from './connection.js';
import { SqliteTransactionError } from './errors.js';
import { mergeQueryOptions, transformToAsyncGenerator } from './util.js';
import {
  ReservedConnection,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqliteDriverStatement,
  SqliteValue
} from '@sqlite-js/driver';

export type SqliteParameterType = SqliteValue;
export type BindValue = SqliteValue;

export interface SqliteQueryOptions extends SqlQueryOptions {}

export interface SqliteTransactionOptions extends SqlTransactionOptions {
  beginTransactionOptions: {
    behavior?: 'DEFERRED' | 'IMMEDIATE' | 'EXCLUSIVE';
  };
  commitTransactionOptions: undefined;
  rollbackTransactionOptions: {
    savepoint?: string;
  };
}

/** Various options that can be configured when opening Database connection. */
export interface SqliteClientOptions
  extends SqlConnectionOptions,
    DatabaseOpenOptions {}

export class SqlitePreparedStatement
  extends SqliteConnectable
  implements
    SqlPreparedStatement<
      SqliteConnectionOptions,
      SqliteParameterType,
      SqliteQueryOptions,
      SqliteConnection
    >
{
  readonly sql: string;
  declare readonly options: SqliteConnectionOptions & SqliteQueryOptions;

  #statementObject?: SqliteDriverStatement;
  #statementArray?: SqliteDriverStatement;

  #deallocated = false;

  constructor(
    connection: SqlitePreparedStatement['connection'],
    sql: string,
    options: SqlitePreparedStatement['options'] = {}
  ) {
    super(connection, options);
    this.sql = sql;
  }

  private _statementObject(): SqliteDriverStatement {
    this.#statementObject ??= this.connection.driver!.prepare(this.sql);
    return this.#statementObject;
  }

  private _statementArray(): SqliteDriverStatement {
    this.#statementArray ??= this.connection.driver!.prepare(this.sql, {
      rawResults: true
    });
    return this.#statementArray;
  }

  get deallocated(): boolean {
    return this.#deallocated;
  }

  async deallocate(): Promise<void> {
    this.#statementArray?.finalize();
    this.#statementObject?.finalize();
    this.#deallocated = true;
  }

  async execute(
    params?: SqliteParameterType[],
    _options?: SqliteQueryOptions | undefined
  ): Promise<number | undefined> {
    const statement = this._statementObject();
    statement.bind(params ?? []);
    const result = await statement.run();
    return result.changes;
  }

  async query<T extends Row<BindValue> = Row<BindValue>>(
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): Promise<T[]> {
    const statement = this._statementObject();
    try {
      statement.bind(params ?? []);
      const { rows } = await statement.step();
      return rows as T[];
    } finally {
      statement.reset();
    }
  }

  async queryOne<T extends Row<BindValue> = Row<BindValue>>(
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): Promise<T | undefined> {
    const all = await this.query<T>(params, options);
    return all[0];
  }

  async *queryMany<T extends Row<BindValue> = Row<BindValue>>(
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): AsyncGenerator<T> {
    const chunkSize = 100;
    const statement = this._statementObject();

    try {
      statement.bind(params ?? []);

      while (true) {
        const { rows, done } = await statement.step(chunkSize);
        if (rows != null) {
          for (let row of rows as T[]) {
            yield row;
          }
        }
        if (done) {
          break;
        }
      }
    } finally {
      statement.reset();
    }
  }

  async queryArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): Promise<T[]> {
    const statement = this._statementArray();
    try {
      statement.bind(params ?? []);
      const { rows } = await statement.step();
      return rows as T[];
    } finally {
      statement.reset();
    }
  }

  async queryOneArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): Promise<T | undefined> {
    const all = await this.queryArray<T>(params, options);
    return all[0];
  }
  async *queryManyArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): AsyncGenerator<T> {
    const chunkSize = 100;
    const statement = this._statementArray();

    try {
      statement.bind(params ?? []);

      while (true) {
        const { rows, done } = await statement.step(chunkSize);
        if (rows != null) {
          for (let row of rows as T[]) {
            yield row;
          }
        }
        if (done) {
          break;
        }
      }
    } finally {
      statement.reset();
    }
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.deallocate();
    await super[Symbol.asyncDispose]();
  }
}

/**
 * Represents a base queriable class for SQLite3.
 */
export class SqliteQueriable
  extends SqliteConnectable
  implements
    SqlQueriable<
      SqliteConnectionOptions,
      SqliteParameterType,
      SqliteQueryOptions,
      SqliteConnection
    >
{
  declare readonly options: SqliteConnectionOptions & SqliteQueryOptions;

  constructor(
    connection: SqliteQueriable['connection'],
    options: SqliteQueriable['options'] = {}
  ) {
    super(connection, options);
  }

  prepare(sql: string, options?: SqliteQueryOptions): SqlitePreparedStatement {
    return new SqlitePreparedStatement(
      this.connection,
      sql,
      mergeQueryOptions(this.options, options)
    );
  }

  execute(
    sql: string,
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): Promise<number | undefined> {
    return this.prepare(sql, options).execute(params);
  }

  query<T extends Row<BindValue> = Row<BindValue>>(
    sql: string,
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): Promise<T[]> {
    return this.prepare(sql, options).query<T>(params);
  }

  queryOne<T extends Row<BindValue> = Row<BindValue>>(
    sql: string,
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): Promise<T | undefined> {
    return this.prepare(sql, options).queryOne<T>(params);
  }

  queryMany<T extends Row<BindValue> = Row<BindValue>>(
    sql: string,
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): AsyncGenerator<T> {
    return this.prepare(sql, options).queryMany<T>(params);
  }

  queryArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    sql: string,
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): Promise<T[]> {
    return this.prepare(sql, options).queryArray<T>(params);
  }

  queryOneArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    sql: string,
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): Promise<T | undefined> {
    return this.prepare(sql, options).queryOneArray<T>(params);
  }

  queryManyArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    sql: string,
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions | undefined
  ): AsyncGenerator<T> {
    return this.connection.queryManyArray<T>(sql, params, options);
  }

  sql<T extends Row<BindValue> = Row<BindValue>>(
    strings: TemplateStringsArray,
    ...parameters: BindValue[]
  ): Promise<T[]> {
    const sql = strings.join('?');
    return this.query<T>(sql, parameters);
  }

  sqlArray<T extends ArrayRow<BindValue> = ArrayRow<BindValue>>(
    strings: TemplateStringsArray,
    ...parameters: BindValue[]
  ): Promise<T[]> {
    const sql = strings.join('?');
    return this.queryArray<T>(sql, parameters);
  }
}

export class SqlitePreparable
  extends SqliteQueriable
  implements
    SqlPreparable<
      SqliteConnectionOptions,
      SqliteParameterType,
      SqliteQueryOptions,
      SqliteConnection,
      SqlitePreparedStatement
    > {}

export class SqliteTransaction
  extends SqliteQueriable
  implements
    SqlTransaction<
      SqliteConnectionOptions,
      SqliteParameterType,
      SqliteQueryOptions,
      SqliteConnection,
      SqlitePreparedStatement,
      SqliteTransactionOptions
    >
{
  #inTransaction: boolean = true;
  get inTransaction(): boolean {
    return this.connected && this.#inTransaction;
  }

  get connected(): boolean {
    if (!this.#inTransaction) {
      throw new SqliteTransactionError(
        'Transaction is not active, create a new one using beginTransaction'
      );
    }

    return super.connected;
  }

  async commitTransaction(
    _options?: SqliteTransactionOptions['commitTransactionOptions']
  ): Promise<void> {
    try {
      await this.execute('COMMIT');
    } catch (e) {
      this.#inTransaction = false;
      throw e;
    }
  }

  async rollbackTransaction(
    options?: SqliteTransactionOptions['rollbackTransactionOptions']
  ): Promise<void> {
    try {
      if (options?.savepoint) {
        await this.execute('ROLLBACK TO ?', [options.savepoint]);
      } else {
        await this.execute('ROLLBACK');
      }
    } catch (e) {
      this.#inTransaction = false;
      throw e;
    }
  }

  async createSavepoint(name: string = `\t_bs3.\t`): Promise<void> {
    await this.execute(`SAVEPOINT ${name}`);
  }

  async releaseSavepoint(name: string = `\t_bs3.\t`): Promise<void> {
    await this.execute(`RELEASE ${name}`);
  }
}

/**
 * Represents a queriable class that can be used to run transactions.
 */
export class SqliteTransactionable
  extends SqlitePreparable
  implements
    SqlTransactionable<
      SqliteConnectionOptions,
      SqliteParameterType,
      SqliteQueryOptions,
      SqliteConnection,
      SqlitePreparedStatement,
      SqliteTransactionOptions,
      SqliteTransaction
    >
{
  async beginTransaction(
    options?: SqliteTransactionOptions['beginTransactionOptions']
  ): Promise<SqliteTransaction> {
    let sql = 'BEGIN';
    if (options?.behavior) {
      sql += ` ${options.behavior}`;
    }
    await this.execute(sql);

    return new SqliteTransaction(this.connection, this.options);
  }

  async transaction<T>(
    fn: (t: SqliteTransaction) => Promise<T>,
    options?: SqliteTransactionOptions
  ): Promise<T> {
    const transaction = await this.beginTransaction(
      options?.beginTransactionOptions
    );

    try {
      const result = await fn(transaction);
      await transaction.commitTransaction(options?.commitTransactionOptions);
      return result;
    } catch (error) {
      await transaction.rollbackTransaction(
        options?.rollbackTransactionOptions
      );
      throw error;
    }
  }
}

class SqlitePoolClient
  extends SqliteTransactionable
  implements
    SqlPoolClient<
      SqliteConnectionOptions,
      SqliteConnection,
      SqliteParameterType,
      SqliteQueryOptions,
      SqlitePreparedStatement,
      SqliteTransactionOptions,
      SqliteTransaction
    >
{
  readonly eventTarget: SqliteEventTarget;
  readonly reserved: ReservedConnection;
  readonly connectionUrl: string;

  constructor(
    connectionUrl: string,
    reserved: ReservedConnection,
    options: SqliteClientOptions = {}
  ) {
    const conn = new SqliteConnection(
      connectionUrl,
      reserved.connection,
      options
    );
    super(conn, options);
    this.reserved = reserved;
    this.connectionUrl = connectionUrl;
    this.eventTarget = new SqliteEventTarget();
  }
  disposed: boolean = false;
  async release(): Promise<void> {
    this.disposed = true;
    await this.reserved.release();
  }

  async connect(): Promise<void> {
    await this.connection.connect();
    this.eventTarget.dispatchEvent(
      new SqliteConnectEvent({ connection: this.connection } as any)
    );
  }

  async close(): Promise<void> {
    this.eventTarget.dispatchEvent(
      new SqliteCloseEvent({ connection: this.connection } as any)
    );
    await this.connection.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

/**
 * Sqlite client
 */
export class SqliteClientPool
  implements
    SqlClientPool<
      SqliteConnectionOptions,
      SqliteParameterType,
      SqliteQueryOptions,
      SqliteConnection,
      SqlitePreparedStatement,
      SqliteTransactionOptions,
      SqliteTransaction
    >
{
  readonly eventTarget: SqliteEventTarget;

  readonly connectionUrl: string;
  readonly pool: SqliteDriverConnectionPool;
  readonly options: SqliteClientOptions;
  readonly connected = true;

  constructor(
    connectionUrl: string,
    pool: SqliteDriverConnectionPool,
    options: SqliteClientOptions = {}
  ) {
    this.pool = pool;
    this.connectionUrl = connectionUrl;
    this.options = options;
    this.eventTarget = new SqliteEventTarget();
  }

  async acquire(): Promise<
    SqlPoolClient<
      SqliteConnectionOptions,
      SqliteConnection,
      SqliteValue,
      SqliteQueryOptions,
      SqlitePreparedStatement,
      SqliteTransactionOptions,
      SqliteTransaction,
      SqlPoolClientOptions
    >
  > {
    const reserved = await this.pool.reserveConnection();
    return new SqlitePoolClient(this.connectionUrl, reserved, this.options);
  }

  async connect(): Promise<void> {
    // No-op
  }

  async close(): Promise<void> {
    // TODO: this.eventTarget.dispatchEvent(new SqliteCloseEvent({}));
    await this.pool.close();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
