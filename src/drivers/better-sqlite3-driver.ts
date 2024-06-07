import type * as bsqlite from 'better-sqlite3';
import DatabaseConstructor from 'better-sqlite3';
import { SqliteValue } from '../common.js';
import {
  SqliteCommandResponse,
  InferBatchResult,
  SqliteCommand,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqlitePrepare,
  SqlitePrepareResponse,
  SqliteStepResponse,
  UpdateListener,
  SqliteBind,
  SqliteStep,
  SqliteReset,
  SqliteFinalize,
  SqliteCommandType
} from '../driver-api.js';

import { ReadWriteConnectionPool } from '../driver-util.js';

export function betterSqlitePool(
  path: string,
  poolOptions?: bsqlite.Options
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return new BetterSqliteConnection(path, {
        ...poolOptions,
        readonly: (poolOptions?.readonly ?? options?.readonly) || false
      });
    }
  });
}

export class BetterSqliteConnection implements SqliteDriverConnection {
  con: bsqlite.Database;
  private statements = new Map<number, bsqlite.Statement>();
  private iterators = new Map<number, Iterator<unknown>>();
  private inError: any = null;
  private bindNamed = new Map<number, Record<string, SqliteValue>>();
  private bindPositional = new Map<number, SqliteValue[]>();
  private statementDone = new Map<number, boolean>();

  constructor(path: string, options?: bsqlite.Options) {
    this.con = new DatabaseConstructor(path, options);
    this.con.exec('PRAGMA journal_mode = WAL');
    this.con.exec('PRAGMA synchronize = normal');
  }

  async close() {
    this.con.close();
  }

  private requireStatement(id: number) {
    const statement = this.statements.get(id);
    if (statement == null) {
      throw new Error(`statement not found: ${id}`);
    }
    return statement;
  }

  private prepare(command: SqlitePrepare): SqlitePrepareResponse {
    const { id, sql } = command;
    const statement = this.con.prepare(sql);
    const existing = this.statements.get(id);
    if (existing != null && id == 0) {
      // Overwrite
      this.finalize({ type: SqliteCommandType.finalize, id: id });
    } else if (existing != null) {
      throw new Error(
        `Replacing statement ${id} without finalizing the previous one`
      );
    }
    this.statements.set(id, statement);
    if (statement.reader) {
      const columns = statement.columns().map((c) => c.name);
      return { columns };
    } else {
      return { columns: [] };
    }
  }

  private bind(command: SqliteBind): SqliteCommandResponse {
    const { id, parameters } = command;
    const statement = this.requireStatement(id);
    if (parameters == null) {
      return {};
    }
    if (Array.isArray(parameters)) {
      let bindArray = this.bindPositional.get(id) ?? [];

      for (let i = 0; i < parameters.length; i++) {
        if (typeof parameters[i] != 'undefined') {
          bindArray[i] = parameters[i]!;
        }
      }
      this.bindPositional.set(id, bindArray);
    } else {
      let previous = this.bindNamed.get(id) ?? {};

      this.bindNamed.set(id, { ...previous, ...parameters });
    }
    return {};
  }

  private step(command: SqliteStep): SqliteStepResponse {
    const { id, n, all, bigint } = command;
    const statement = this.requireStatement(id);
    if (this.statementDone.has(id)) {
      return { skipped: true } as SqliteStepResponse;
    }
    const bindNamed = this.bindNamed.get(id);
    const bindPositional = this.bindPositional.get(id);
    const bind = [bindPositional, bindNamed].filter((b) => b != null);
    if (!statement.reader) {
      statement.run(...bind);
      this.statementDone.set(id, true);
      return { rows: [], done: true } as SqliteStepResponse;
    }
    let iterator = this.iterators.get(id);
    const num_rows = n ?? 1;
    if (iterator == null) {
      statement.raw();
      if (bigint) {
        statement.safeIntegers();
      }
      iterator = statement.iterate(...bind);
      this.iterators.set(id, iterator);
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
      this.statementDone.set(id, true);
    }
    return { rows, done: isDone } as SqliteStepResponse;
  }

  private reset(command: SqliteReset): SqliteCommandResponse {
    const { id, clear_bindings } = command;
    const statement = this.requireStatement(id);
    if (this.iterators.has(id)) {
      const iter = this.iterators.get(id)!;
      iter.return!();
      this.iterators.delete(id);
    }
    if (clear_bindings) {
      this.bindNamed.delete(id);
      this.bindPositional.delete(id);
    }
    this.statementDone.delete(id);
    return {};
  }

  private finalize(command: SqliteFinalize): SqliteCommandResponse {
    const { id } = command;
    this.statements.delete(id);
    this.bindNamed.delete(id);
    this.bindPositional.delete(id);
    const existingIter = this.iterators.get(id);
    if (existingIter != null) {
      existingIter.return?.();
    }
    this.iterators.delete(id);
    this.statementDone.delete(id);
    return {};
  }

  private executeCommand(command: SqliteCommand): SqliteCommandResponse {
    switch (command.type) {
      case SqliteCommandType.prepare:
        return this.prepare(command);
      case SqliteCommandType.bind:
        return this.bind(command);
      case SqliteCommandType.step:
        return this.step(command);
      case SqliteCommandType.reset:
        return this.reset(command);
      case SqliteCommandType.finalize:
        return this.finalize(command);
      default:
        throw new Error(`Unknown command: ${command.type}`);
    }
  }

  async execute<const T extends SqliteCommand[]>(
    commands: T
  ): Promise<InferBatchResult<T>> {
    // console.log('execute', commands);
    let results: SqliteCommandResponse[] = [];

    for (let command of commands) {
      if (command.type == SqliteCommandType.sync) {
        if (this.inError != null) {
          results.push({ error: this.inError });
        } else {
          results.push({});
        }
        if (this.statements.has(0)) {
          this.finalize({ type: SqliteCommandType.finalize, id: 0 });
        }
        this.inError = null;
      } else if (this.inError) {
        results.push({ skipped: true });
      } else {
        try {
          const result = this.executeCommand(command);
          results.push(result);
        } catch (e) {
          this.inError = e;
          results.push({ error: e as any });
        }
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
