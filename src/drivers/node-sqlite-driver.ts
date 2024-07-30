import type * as sqlite from 'node:sqlite';

import { SqliteValue } from '../common.js';
import {
  SqliteCommandResponse,
  InferBatchResult,
  SqliteCommand,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqlitePrepare,
  SqliteStepResponse,
  UpdateListener,
  SqliteBind,
  SqliteStep,
  SqliteReset,
  SqliteFinalize,
  SqliteCommandType,
  PrepareOptions,
  ResetOptions,
  SqliteParameterBinding,
  SqliteParseResponse,
  SqliteParse,
  SqliteDriverStatement,
  StepOptions,
  SqliteDriverError
} from '../driver-api.js';

import { ReadWriteConnectionPool } from '../driver-util.js';
import { mapError, SqliteError } from './util.js';

export function nodeSqlitePool(path: string): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      const sqlite = await import('node:sqlite');
      const db = new sqlite.DatabaseSync(path);
      return new NodeSqliteConnection(db, {
        readonly: options?.readonly,
        name: options?.name
      });
    }
  });
}

interface InternalStatement extends SqliteDriverStatement {
  getColumnsSync(): string[];
  stepSync(n?: number, options?: StepOptions): SqliteStepResponse;

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
  stepSync(n?: number, options?: StepOptions): SqliteStepResponse {
    throw this.error;
  }
  async getColumns(): Promise<string[]> {
    throw this.error;
  }
  bind(parameters: SqliteParameterBinding): void {
    // no-op
  }
  async step(n?: number, options?: StepOptions): Promise<SqliteStepResponse> {
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
class NodeSqliteSyncStatement implements InternalStatement {
  public statement: sqlite.StatementSync;
  private options: PrepareOptions;
  private bindPositional: SqliteValue[] = [];
  private bindNamed: Record<string, SqliteValue> = {};
  private statementDone = false;
  private iterator: Iterator<unknown> | undefined = undefined;

  readonly persisted: boolean;

  [Symbol.dispose]: () => void = undefined as any;

  constructor(statement: sqlite.StatementSync, options: PrepareOptions) {
    this.statement = statement;
    this.options = options;
    this.persisted = options.persist ?? false;

    if (typeof Symbol.dispose != 'undefined') {
      this[Symbol.dispose] = () => this.finalize();
    }
  }

  get source() {
    return this.statement.sourceSQL();
  }

  async getColumns(): Promise<string[]> {
    return this.getColumnsSync();
  }

  getColumnsSync(): string[] {
    // Not supported
    return [];
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
      let previous = this.bindNamed;
      this.bindNamed = { ...previous, ...parameters };
    }
  }

  async step(n?: number, options?: StepOptions): Promise<SqliteStepResponse> {
    try {
      return this.stepSync(n, options);
    } catch (e) {
      throw mapError(e);
    }
  }

  stepSync(n?: number, options?: StepOptions): SqliteStepResponse {
    const all = n == null;

    const statement = this.statement;
    if (this.statementDone) {
      return { skipped: true } as SqliteStepResponse;
    }

    if (options?.requireTransaction) {
      // TODO: implement
    }

    const bindNamed = this.bindNamed;
    const bindPositional = this.bindPositional;

    let iterator = this.iterator;
    const num_rows = n ?? 1;
    if (iterator == null) {
      if (this.options.rawResults) {
        // Not supported
      }
      if (this.options.bigint) {
        statement.setReadBigInts(true);
      }
      iterator = statement.all(bindNamed, ...bindPositional)[Symbol.iterator]();
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
    return { rows, done: isDone } as SqliteStepResponse;
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
      iter.return?.();
      this.iterator = undefined;
    }
    if (options?.clear_bindings) {
      this.bindNamed = {};
      this.bindPositional = [];
    }
    this.statementDone = false;
  }
}

export class NodeSqliteConnection implements SqliteDriverConnection {
  con: sqlite.DatabaseSync;
  statements = new Map<number, InternalStatement>();
  name: string;

  constructor(
    db: sqlite.DatabaseSync,
    options?: { readonly?: boolean; name?: string }
  ) {
    this.con = db;
    this.con.exec('PRAGMA journal_mode = WAL');
    this.con.exec('PRAGMA synchronous = normal');
    this.con.exec('PRAGMA busy_timeout = 5000');
    if (options?.readonly) {
      this.con.exec('PRAGMA query_only = true');
    }
    this.name = options?.name ?? '';
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
      return new NodeSqliteSyncStatement(statement, options ?? {});
    } catch (e) {
      return new ErrorStatement(sql, mapError(e), options ?? {});
    }
  }

  private requireStatement(id: number) {
    const statement = this.statements.get(id);
    if (statement == null) {
      throw new Error(`statement not found: ${id}`);
    }
    return statement;
  }

  private _prepare(command: SqlitePrepare): SqliteCommandResponse {
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
    return {};
  }

  private _parse(command: SqliteParse): SqliteParseResponse {
    const { id } = command;
    const statement = this.requireStatement(id);
    return { columns: statement.getColumnsSync() };
  }

  private _bind(command: SqliteBind): SqliteCommandResponse {
    const { id, parameters } = command;
    const statement = this.requireStatement(id);
    statement.bind(parameters);
    return {};
  }

  private _step(command: SqliteStep): SqliteStepResponse {
    const { id, n, requireTransaction } = command;
    const statement = this.requireStatement(id);
    return statement.stepSync(n, { requireTransaction });
  }

  private _reset(command: SqliteReset): SqliteCommandResponse {
    const { id, clear_bindings } = command;
    const statement = this.requireStatement(id);
    statement.reset(command);
    return {};
  }

  private _finalize(command: SqliteFinalize): SqliteCommandResponse {
    const { id } = command;
    const statement = this.requireStatement(id);
    statement.finalize();
    this.statements.delete(id);
    return {};
  }

  private _executeCommand(command: SqliteCommand): SqliteCommandResponse {
    switch (command.type) {
      case SqliteCommandType.prepare:
        return this._prepare(command);
      case SqliteCommandType.bind:
        return this._bind(command);
      case SqliteCommandType.step:
        return this._step(command);
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
        results.push(result);
      } catch (e: any) {
        const err = mapError(e);
        results.push({
          error: { message: err.message, stack: err.stack, code: err.code }
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

    // this.con.function('_logger', function (table: any, type: any, rowid: any) {
    //   listener({ events: [{ table, rowId: rowid, type }] });
    // });
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
