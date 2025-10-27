import {
  PrepareOptions,
  QueryOptions,
  SqliteArrayRow,
  SqliteChanges,
  SqliteDriverConnection,
  SqliteDriverStatement,
  SqliteObjectRow,
  SqliteParameterBinding,
  SqliteValue,
  StreamQueryOptions,
  UpdateListener
} from '@sqlite-js/driver';
import type * as bsqlite from 'better-sqlite3';
import DatabaseConstructor from 'better-sqlite3';

import { ErrorStatement, mapError } from '@sqlite-js/driver/util';
import { BetterSqliteDriverOptions } from './driver.js';

interface InternalStatement extends SqliteDriverStatement {
  readonly source: string;
  readonly persisted: boolean;
}

class BetterSqlitePreparedStatement implements InternalStatement {
  public statement: bsqlite.Statement;

  readonly persisted: boolean;

  [Symbol.dispose]: () => void = undefined as any;

  constructor(statement: bsqlite.Statement, options: PrepareOptions) {
    this.statement = statement;
    this.persisted = options.autoFinalize ?? false;

    if (typeof Symbol.dispose != 'undefined') {
      this[Symbol.dispose] = () => this.finalize();
    }
  }

  get source() {
    return this.statement.source;
  }

  async getColumns(): Promise<string[]> {
    const existing = this.statement;
    if (existing.reader) {
      const columns = existing.columns().map((c) => c.name);
      return columns;
    } else {
      return [];
    }
  }

  private checkTransaction(options: QueryOptions | undefined) {
    if (options?.requireTransaction) {
      if (!this.statement.database.inTransaction) {
        throw new Error('Transaction has been rolled back');
      }
    }
  }

  _all(
    parameters: SqliteParameterBinding,
    options: QueryOptions | undefined,
    array: boolean
  ): unknown[] {
    this.checkTransaction(options);

    const statement = this.statement;

    if (statement.reader) {
      statement.safeIntegers(options?.bigint ?? false);
      statement.raw(array);
      const rows = statement.all(sanitizeParameters(parameters));
      return rows;
    } else {
      statement.run(sanitizeParameters(parameters));
      return [];
    }
  }

  async all(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteObjectRow[]> {
    try {
      return this._all(parameters, options, false) as SqliteObjectRow[];
    } catch (e) {
      throw mapError(e);
    }
  }

  async allArray(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteArrayRow[]> {
    try {
      return this._all(parameters, options, true) as SqliteArrayRow[];
    } catch (e) {
      throw mapError(e);
    }
  }

  *_stream(
    parameters: SqliteParameterBinding,
    options: StreamQueryOptions | undefined,
    array: boolean
  ) {
    this.checkTransaction(options);

    const statement = this.statement;

    if (statement.reader) {
      statement.safeIntegers(options?.bigint ?? false);
      statement.raw(array);
    } else {
      statement.run(sanitizeParameters(parameters));
      return;
    }
    const iter = statement.iterate(sanitizeParameters(parameters));
    const maxBuffer = options?.chunkMaxRows ?? 100;
    let buffer: any[] = [];
    for (let row of iter) {
      buffer.push(row as any);
      if (buffer.length >= maxBuffer) {
        yield buffer;
        buffer = [];
      }
    }
    if (buffer.length > 0) {
      yield buffer;
    }
  }

  async *stream(
    parameters?: SqliteParameterBinding,
    options?: StreamQueryOptions
  ): AsyncIterableIterator<SqliteObjectRow[]> {
    try {
      yield* this._stream(parameters, options, false);
    } catch (e) {
      throw mapError(e);
    }
  }

  async *streamArray(
    parameters?: SqliteParameterBinding,
    options?: StreamQueryOptions
  ): AsyncIterableIterator<SqliteArrayRow[]> {
    try {
      yield* this._stream(parameters, options, true);
    } catch (e) {
      throw mapError(e);
    }
  }

  async run(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteChanges> {
    try {
      return this._run(parameters, options);
    } catch (e) {
      throw mapError(e);
    }
  }

  _run(
    parameters: SqliteParameterBinding,
    options?: QueryOptions
  ): SqliteChanges {
    this.checkTransaction(options);

    const statement = this.statement;

    statement.safeIntegers(true);
    const r = statement.run(sanitizeParameters(parameters));
    return {
      changes: r.changes,
      lastInsertRowId: r.lastInsertRowid as bigint
    };
  }

  finalize(): void {
    // TODO: cancel iterators
  }
}

export class BetterSqliteConnection implements SqliteDriverConnection {
  con: bsqlite.Database;
  private changeStatement: bsqlite.Statement;

  static open(
    path: string,
    options?: bsqlite.Options & BetterSqliteDriverOptions
  ): BetterSqliteConnection {
    const con = new DatabaseConstructor(path, options);
    con.exec('PRAGMA journal_mode = WAL');
    con.exec('PRAGMA synchronous = normal');

    if (options?.loadExtensions) {
      for (let extension of options.loadExtensions) {
        con.loadExtension(extension);
      }
    }
    return new BetterSqliteConnection(con);
  }

  constructor(con: bsqlite.Database) {
    this.con = con;

    this.changeStatement = this.con.prepare(
      'select last_insert_rowid() as l, changes() as c'
    );
    this.changeStatement.safeIntegers(true);
  }

  async getLastChanges(): Promise<SqliteChanges> {
    const r = this.changeStatement.get() as any;
    return {
      lastInsertRowId: r!.l,
      changes: Number(r!.c)
    };
  }

  async close() {
    this.con.close();
  }

  prepare(sql: string, options?: PrepareOptions): InternalStatement {
    try {
      const statement = this.con.prepare(sql);
      return new BetterSqlitePreparedStatement(statement, options ?? {});
    } catch (error) {
      return new ErrorStatement(sql, mapError(error), options ?? {});
    }
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    // Proof-of-concept implementation, based on the idea here:
    // https://github.com/WiseLibs/better-sqlite3/issues/62
    // TODO:
    // 1. Handle multiple registrations.
    // 2. Don't re-register triggers.
    // 3. De-register listener.
    // 4. Batching.
    //
    // More fundamental limitations:
    // 1. The table needs to exist before registering the listener.
    // 2. Deleting and re-creating the same will dereigster the listener for that table.

    this.con.function('_logger', function (table: any, type: any, rowid: any) {
      listener({ events: [{ table, rowId: rowid, type }] });
    });
    let tables = options?.tables;
    if (tables == null) {
      tables = this.con
        .prepare(`select name from sqlite_master where type = 'table'`)
        .all()
        .map((row) => (row as any).name as string);
    }
    for (let table of tables) {
      this.con.exec(
        `CREATE TEMPORARY TRIGGER IF NOT EXISTS _logger_notification_${table}__update AFTER UPDATE ON ${table} BEGIN SELECT _logger('${table}', 'update', NEW.rowid); END`
      );
      this.con.exec(
        `CREATE TEMPORARY TRIGGER IF NOT EXISTS _logger_notification_${table}__insert AFTER INSERT ON ${table} BEGIN SELECT _logger('${table}', 'insert', NEW.rowid); END`
      );
      this.con.exec(
        `CREATE TEMPORARY TRIGGER IF NOT EXISTS _logger_notification_${table}__delete AFTER DELETE ON ${table} BEGIN SELECT _logger('${table}', 'delete', OLD.rowid); END`
      );
    }
    return () => {};
  }
}

function sanitizeParameters(
  parameters: SqliteParameterBinding
): SqliteParameterBinding {
  if (parameters == null) {
    return [];
  } else if (Array.isArray(parameters)) {
    return parameters;
  }
  let result: Record<string, SqliteValue> = {};
  for (let key in parameters) {
    const value = parameters[key];
    let name = key;
    const prefix = key[0];
    // better-sqlite doesn't support the explicit prefix - strip it
    if (prefix == ':' || prefix == '?' || prefix == '$' || prefix == '@') {
      name = key.substring(1);
    }
    result[name] = value;
  }
  return result;
}
