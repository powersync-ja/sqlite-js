// deno-lint-ignore-file require-await
import {
  ReservedConnection,
  SqliteDriverConnection,
  SqliteDriverConnectionPool
} from '@sqlite-js/driver';
import type {
  ArrayRow,
  Row,
  SqlConnectable,
  SqlConnection,
  SqlConnectionOptions
} from '@stdext/sql';
import type { SqliteParameterType, SqliteQueryOptions } from './core.js';
import { type DatabaseOpenOptions } from './database.js';

/** Various options that can be configured when opening Database connection. */
export interface SqliteConnectionOptions
  extends SqlConnectionOptions,
    DatabaseOpenOptions {}

export class SqliteConnection implements SqlConnection {
  public driver: SqliteDriverConnection | undefined;

  public readonly connectionUrl: string;

  get connected(): boolean {
    return this.driver != null;
  }

  public readonly options: SqliteConnectionOptions;

  constructor(
    connectionUrl: string,
    driver: SqliteDriverConnection | undefined,
    options?: SqliteConnectionOptions
  ) {
    this.connectionUrl = connectionUrl;
    this.driver = driver;
    this.options = options ?? {};
  }

  async connect(): Promise<void> {
    // No-op
  }

  async close(): Promise<void> {
    await this.driver?.close();
  }

  async execute(
    sql: string,
    params?: SqliteParameterType[],
    _options?: SqliteQueryOptions
  ): Promise<number | undefined> {
    using statement = this.driver!.prepare(sql);
    if (params != null) {
      statement.bind(params);
    }
    const results = await statement.run();
    return results.changes;
  }

  async *queryMany<T extends Row<any> = Row<any>>(
    sql: string,
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions
  ): AsyncGenerator<T, any, unknown> {
    using statement = this.driver!.prepare(sql, {
      bigint: this.options.int64 ?? false
    });
    if (params != null) {
      statement.bind(params);
    }
    const chunkSize = 100;

    while (true) {
      const { rows, done } = await statement.step(chunkSize);
      if (rows != null) {
        const castRows = rows as T[];
        for (let row of castRows) {
          yield row;
        }
      }
      if (done) {
        break;
      }
    }
  }

  async *queryManyArray<T extends ArrayRow<any> = ArrayRow<any>>(
    sql: string,
    params?: SqliteParameterType[],
    options?: SqliteQueryOptions
  ): AsyncGenerator<T, any, unknown> {
    using statement = this.driver!.prepare(sql, {
      bigint: this.options.int64 ?? false,
      rawResults: true
    });
    if (params != null) {
      statement.bind(params);
    }
    const chunkSize = 100;

    while (true) {
      const { rows, done } = await statement.step(chunkSize);
      if (rows != null) {
        const castRows = rows as T[];
        for (let row of castRows) {
          yield row;
        }
      }
      if (done) {
        break;
      }
    }
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}

export class SqliteReservedConnection extends SqliteConnection {
  #connect: () => Promise<ReservedConnection>;
  #reserved: ReservedConnection | undefined;

  constructor(
    connectionUrl: string,
    connect: () => Promise<ReservedConnection>,
    options?: SqliteConnectionOptions
  ) {
    super(connectionUrl, undefined, options);
    this.#connect = connect;
  }

  async connect(): Promise<void> {
    this.#reserved = await this.#connect();
    this.driver = this.#reserved.connection;
  }

  async close(): Promise<void> {
    this.driver = undefined;
    await this.#reserved?.release();
  }
}

export class SqliteConnectable
  implements SqlConnectable<SqliteConnectionOptions, SqliteConnection>
{
  readonly connection: SqliteConnection;
  readonly options: SqliteConnectionOptions;
  get connected(): boolean {
    return this.connection.connected;
  }

  constructor(
    connection: SqliteConnectable['connection'],
    options: SqliteConnectable['options'] = {}
  ) {
    this.connection = connection;
    this.options = options;
  }
  [Symbol.asyncDispose](): Promise<void> {
    return this.connection.close();
  }
}
