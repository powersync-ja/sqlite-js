import type * as bsqlite from 'better-sqlite3';
import DatabaseConstructor from 'better-sqlite3';
import { SqliteArguments, SqliteValue } from '../common.js';
import {
  CommandResult,
  ExecuteOptions,
  ResultSet,
  RunResults,
  SqliteCommand,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  UpdateListener
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

  constructor(path: string, options?: bsqlite.Options) {
    this.con = new DatabaseConstructor(path, options);
  }

  async close() {
    this.con.close();
  }

  private async executeCommand(command: SqliteCommand): Promise<CommandResult> {
    if ('prepare' in command) {
      const { id, sql } = command.prepare;
      const statement = this.con.prepare(sql);
      const existing = this.statements.get(0);
      if (existing != null && id == 0) {
        // Overwrite
        this.bindNamed.delete(id);
        this.bindPositional.delete(id);
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
    } else if ('bind' in command) {
      const { id, parameters } = command.bind;
      const statement = this.statements.get(id)!;
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
    } else if ('step' in command) {
      const { id, n, all, bigint } = command.step;
      const statement = this.statements.get(id)!;
      const bindNamed = this.bindNamed.get(id);
      const bindPositional = this.bindPositional.get(id);
      const bind = [bindPositional, bindNamed].filter((b) => b != null);
      if (!statement.reader) {
        statement.run(...bind);
        return { rows: [], done: true };
      }

      statement.raw();
      if (bigint) {
        statement.safeIntegers();
      }

      const num_rows = n ?? 1;
      let iterator = this.iterators.get(id);
      if (iterator == null) {
        iterator = statement.iterate(...bind);
        this.iterators.set(id, iterator);
      }
      let rows = [];
      let isDone = false;
      for (let i = 0; i < num_rows || all; i++) {
        const { value, done } = iterator.next();
        if (done) {
          isDone = true;
          this.iterators.delete(id);
          break;
        }
        rows.push(value);
      }
      return { rows, done: isDone };
    } else if ('reset' in command) {
      const { id, clear_bindings } = command.reset;
      const statement = this.statements.get(id)!;
      if (this.iterators.has(id)) {
        const iter = this.iterators.get(id)!;
        while (true) {
          const { value, done } = iter.next();
          if (done) {
            break;
          }
        }
        this.iterators.delete(id);
      }
      if (clear_bindings) {
        this.bindNamed.delete(id);
        this.bindPositional.delete(id);
      }
      return {};
    } else if ('finalize' in command) {
      const { id } = command.finalize;
      const statement = this.statements.get(id)!;
      this.statements.delete(id);
      this.bindNamed.delete(id);
      this.bindPositional.delete(id);
      return {};
    } else {
      throw new Error(`Unknown command: ${Object.keys(command)[0]}`);
    }
  }

  async execute(commands: SqliteCommand[]): Promise<CommandResult[]> {
    let results: CommandResult[] = [];
    for (let command of commands) {
      if ('sync' in command) {
        if (this.inError != null) {
          results.push({ error: this.inError });
        } else {
          results.push({});
        }
        if (this.statements.has(0)) {
          this.statements.delete(0);
          this.bindNamed.delete(0);
          this.bindPositional.delete(0);
        }
        this.inError = null;
      } else if (this.inError) {
        results.push({ skip: true });
      } else {
        try {
          const result = await this.executeCommand(command);
          results.push(result);
        } catch (e) {
          this.inError = e;
          results.push({ error: e });
        }
      }
    }
    return results;
  }

  async selectAll(
    query: string,
    args?: SqliteArguments | undefined,
    options?: ExecuteOptions | undefined
  ): Promise<ResultSet> {
    const statement = this.con.prepare(query);
    const bindArgs = args == undefined ? [] : [args];
    if (!statement.reader) {
      statement.run(...bindArgs);
      return { columns: [], rows: [] };
    }
    statement.raw();
    if (options?.bigint) {
      statement.safeIntegers();
    }
    const columns = statement.columns().map((c) => c.name);
    const rows = statement.all(...bindArgs) as SqliteValue[][];
    return {
      columns,
      rows
    };
  }

  async *selectStreamed(
    query: string,
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): AsyncGenerator<ResultSet, any, undefined> {
    const bindArgs = args == undefined ? [] : [args];
    const statement = this.con.prepare(query);
    if (!statement.reader) {
      statement.run(...bindArgs);
      return;
    }
    statement.raw();
    if (options?.bigint) {
      statement.safeIntegers();
    }
    const columns = statement.columns().map((c) => c.name);
    let buffer: SqliteValue[][] = [];
    let didYield = false;
    for (let row of statement.iterate(...bindArgs)) {
      buffer.push(row as SqliteValue[]);
      if (buffer.length > (options?.chunkSize ?? 10)) {
        yield {
          columns,
          rows: buffer
        };
        didYield = true;
        buffer = [];
      }
    }
    if (buffer.length > 0 || !didYield) {
      yield {
        columns,
        rows: buffer
      };
    }
  }

  async run(query: string, args?: SqliteArguments): Promise<void> {
    const bindArgs = args == undefined ? [] : [args];
    const statement = this.con.prepare(query);
    statement.run(...bindArgs);
  }

  async runWithResults(
    query: string,
    args?: SqliteArguments
  ): Promise<RunResults> {
    const statement = this.con.prepare(query);
    const bindArgs = args == undefined ? [] : [args];
    const r = statement.run(...bindArgs);
    return {
      changes: r.changes,
      lastInsertRowId: BigInt(r.lastInsertRowid)
    };
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
