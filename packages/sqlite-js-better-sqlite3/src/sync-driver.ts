import {
  PrepareOptions,
  ResetOptions,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqliteDriverStatement,
  SqliteParameterBinding,
  SqliteRunResult,
  SqliteStepResult,
  SqliteValue,
  StepOptions,
  UpdateListener
} from '@powersync/sqlite-js-driver';
import type * as bsqlite from 'better-sqlite3';
import DatabaseConstructor from 'better-sqlite3';

import {
  mapError,
  ReadWriteConnectionPool
} from '@powersync/sqlite-js-driver/util';
import {
  InferBatchResult,
  SqliteBind,
  SqliteCommand,
  SqliteCommandResponse,
  SqliteCommandType,
  SqliteDriverError,
  SqliteFinalize,
  SqliteParse,
  SqliteParseResult,
  SqlitePrepare,
  SqliteReset,
  SqliteRun,
  SqliteStep
} from '@powersync/sqlite-js-driver/worker_threads';

export function betterSqlitePool(
  path: string,
  poolOptions?: bsqlite.Options
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return BetterSqliteConnection.open(path, {
        ...poolOptions,
        readonly: (poolOptions?.readonly ?? options?.readonly) || false
      });
    }
  });
}

interface InternalStatement extends SqliteDriverStatement {
  getColumnsSync(): string[];
  stepSync(n?: number, options?: StepOptions): SqliteStepResult;
  runSync(options?: StepOptions): SqliteRunResult;

  readonly source: string;

  readonly persisted: boolean;
}

class ErrorStatement implements InternalStatement {
  readonly error: SqliteDriverError;
  readonly source: string;
  readonly persisted: boolean;

  constructor(
    source: string,
    error: SqliteDriverError,
    options: PrepareOptions
  ) {
    this.error = error;
    this.source = source;
    this.persisted = options.persist ?? false;
  }

  getColumnsSync(): string[] {
    throw this.error;
  }
  stepSync(n?: number, options?: StepOptions): SqliteStepResult {
    throw this.error;
  }
  runSync(options?: StepOptions): SqliteRunResult {
    throw this.error;
  }
  async getColumns(): Promise<string[]> {
    throw this.error;
  }
  bind(parameters: SqliteParameterBinding): void {
    // no-op
  }
  async step(n?: number, options?: StepOptions): Promise<SqliteStepResult> {
    throw this.error;
  }

  async run(options?: StepOptions): Promise<SqliteRunResult> {
    throw this.error;
  }

  finalize(): void {
    // no-op
  }

  reset(options?: ResetOptions): void {
    // no-op
  }

  [Symbol.dispose](): void {
    // no-op
  }
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
    return this.getColumnsSync();
  }

  getColumnsSync(): string[] {
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

  async run(options?: StepOptions): Promise<SqliteRunResult> {
    try {
      return this.runSync(options);
    } catch (e) {
      throw mapError(e);
    }
  }

  runSync(options?: StepOptions): SqliteRunResult {
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
  private statements = new Map<number, InternalStatement>();

  static open(path: string, options?: bsqlite.Options): BetterSqliteConnection {
    const con = new DatabaseConstructor(path, options);
    con.exec('PRAGMA journal_mode = WAL');
    con.exec('PRAGMA synchronous = normal');
    return new BetterSqliteConnection(con);
  }

  constructor(con: bsqlite.Database) {
    this.con = con;
  }

  async close() {
    const remainingStatements = [...this.statements.values()].filter(
      (s) => !s.persisted
    );
    if (remainingStatements.length > 0) {
      const statement = remainingStatements[0];
      throw new Error(
        `${remainingStatements.length} statements not finalized. First: ${statement.source}`
      );
    }
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

  private requireStatement(id: number) {
    const statement = this.statements.get(id);
    if (statement == null) {
      throw new Error(`statement not found: ${id}`);
    }
    return statement;
  }

  private _prepare(command: SqlitePrepare) {
    const { id, sql } = command;
    const statement = this.prepare(sql, {
      bigint: command.bigint,
      persist: command.persist,
      rawResults: command.rawResults
    });
    const existing = this.statements.get(id);
    if (existing != null) {
      throw new Error(
        `Replacing statement ${id} without finalizing the previous one`
      );
    }
    this.statements.set(id, statement);
  }

  private _parse(command: SqliteParse): SqliteParseResult {
    const { id } = command;
    const statement = this.requireStatement(id);
    return { columns: statement.getColumnsSync() };
  }

  private _bind(command: SqliteBind): void {
    const { id, parameters } = command;
    const statement = this.requireStatement(id);
    statement.bind(parameters);
  }

  private _step(command: SqliteStep): SqliteStepResult {
    const { id, n, requireTransaction } = command;
    const statement = this.requireStatement(id);
    return statement.stepSync(n, { requireTransaction });
  }

  private _run(command: SqliteRun): SqliteRunResult {
    const { id, requireTransaction } = command;
    const statement = this.requireStatement(id);
    return statement.runSync({ requireTransaction });
  }

  private _reset(command: SqliteReset): void {
    const { id } = command;
    const statement = this.requireStatement(id);
    statement.reset(command);
  }

  private _finalize(command: SqliteFinalize): void {
    const { id } = command;

    const statement = this.statements.get(id);
    if (statement != null) {
      statement.finalize();
      this.statements.delete(id);
    }
  }

  private _executeCommand(command: SqliteCommand): any {
    switch (command.type) {
      case SqliteCommandType.prepare:
        return this._prepare(command);
      case SqliteCommandType.bind:
        return this._bind(command);
      case SqliteCommandType.step:
        return this._step(command);
      case SqliteCommandType.run:
        return this._run(command);
      case SqliteCommandType.reset:
        return this._reset(command);
      case SqliteCommandType.finalize:
        return this._finalize(command);
      case SqliteCommandType.parse:
        return this._parse(command);
      default:
        throw new Error(`Unknown command: ${command.type}`);
    }
  }

  async execute<const T extends SqliteCommand[]>(
    commands: T
  ): Promise<InferBatchResult<T>> {
    let results: SqliteCommandResponse[] = [];

    for (let command of commands) {
      try {
        const result = this._executeCommand(command);
        results.push({ value: result });
      } catch (e: any) {
        results.push({
          error: mapError(e)
        });
      }
    }
    return results as InferBatchResult<T>;
  }

  dispose(): void {
    // No-op
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
