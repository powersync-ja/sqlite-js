import {
  PrepareOptions,
  ResetOptions,
  SqliteChanges,
  SqliteDriverConnection,
  SqliteDriverStatement,
  SqliteParameterBinding,
  SqliteStepResult,
  SqliteValue,
  StepOptions,
  UpdateListener
} from '@sqlite-js/driver';
import type * as bsqlite from 'better-sqlite3';
import DatabaseConstructor from 'better-sqlite3';

import { ErrorStatement, mapError } from '@sqlite-js/driver/util';
import { SqliteDriverError } from '@sqlite-js/driver/worker_threads';
import { BetterSqliteDriverOptions } from './driver.js';

interface InternalStatement extends SqliteDriverStatement {
  readonly source: string;
  readonly persisted: boolean;
}

class BetterSqlitePreparedStatement implements InternalStatement {
  public statement: bsqlite.Statement;
  private options: PrepareOptions;
  private bindPositional: SqliteValue[] = [];
  private bindNamed: Record<string, SqliteValue> = {};
  private statementDone = false;
  private iterator: Iterator<unknown> | undefined = undefined;

  readonly persisted: boolean;

  [Symbol.dispose]: () => void = undefined as any;

  constructor(statement: bsqlite.Statement, options: PrepareOptions) {
    this.statement = statement;
    this.options = options;
    this.persisted = options.persist ?? false;

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

  bind(parameters: SqliteParameterBinding): void {
    if (parameters == null) {
      return;
    }
    if (Array.isArray(parameters)) {
      let bindArray = this.bindPositional;

      for (let i = 0; i < parameters.length; i++) {
        if (typeof parameters[i] != 'undefined') {
          bindArray[i] = parameters[i]!;
        }
      }
    } else {
      for (let key in parameters) {
        const value = parameters[key];
        let name = key;
        const prefix = key[0];
        // better-sqlite doesn't support the explicit prefix - strip it
        if (prefix == ':' || prefix == '?' || prefix == '$' || prefix == '@') {
          name = key.substring(1);
        }
        this.bindNamed[name] = value;
      }
    }
  }

  async step(n?: number, options?: StepOptions): Promise<SqliteStepResult> {
    try {
      return this.stepSync(n, options);
    } catch (e) {
      throw mapError(e);
    }
  }

  async run(options?: StepOptions): Promise<SqliteChanges> {
    try {
      return this.runSync(options);
    } catch (e) {
      throw mapError(e);
    }
  }

  runSync(options?: StepOptions): SqliteChanges {
    if (options?.requireTransaction) {
      if (!this.statement.database.inTransaction) {
        throw new Error('Transaction has been rolled back');
      }
    }

    const statement = this.statement;
    this.reset();

    try {
      const bindNamed = this.bindNamed;
      const bindPositional = this.bindPositional;
      const bind = [bindPositional, bindNamed].filter((b) => b != null);

      statement.safeIntegers(true);
      const r = statement.run(...bind);
      return {
        changes: r.changes,
        lastInsertRowId: r.lastInsertRowid as bigint
      };
    } finally {
      this.reset();
    }
  }

  stepSync(n?: number, options?: StepOptions): SqliteStepResult {
    const all = n == null;

    const statement = this.statement;
    if (this.statementDone) {
      return { done: true } as SqliteStepResult;
    }

    if (options?.requireTransaction) {
      if (!this.statement.database.inTransaction) {
        throw new Error('Transaction has been rolled back');
      }
    }

    const bindNamed = this.bindNamed;
    const bindPositional = this.bindPositional;
    const bind = [bindPositional, bindNamed].filter((b) => b != null);
    if (!statement.reader) {
      statement.run(...bind);
      this.statementDone = true;
      return { rows: [], done: true } as SqliteStepResult;
    }
    let iterator = this.iterator;
    const num_rows = n ?? 1;
    if (iterator == null) {
      statement.raw(this.options.rawResults ?? false);
      statement.safeIntegers(this.options.bigint ?? false);
      iterator = statement.iterate(...bind);
      this.iterator = iterator;
    }
    let rows = [];
    let isDone = false;
    for (let i = 0; i < num_rows || all; i++) {
      const { value, done } = iterator.next();
      if (done) {
        isDone = true;
        break;
      }
      rows.push(value);
    }
    if (isDone) {
      this.statementDone = true;
    }
    return { rows, done: isDone } as SqliteStepResult;
  }

  finalize(): void {
    const existingIter = this.iterator;
    if (existingIter != null) {
      existingIter.return?.();
    }
    this.iterator = undefined;
    this.statementDone = false;
  }

  reset(options?: ResetOptions): void {
    if (this.iterator) {
      const iter = this.iterator;
      iter.return!();
      this.iterator = undefined;
    }
    if (options?.clearBindings) {
      this.bindNamed = {};
      this.bindPositional = [];
    }
    this.statementDone = false;
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
