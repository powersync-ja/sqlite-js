import type * as bsqlite from "better-sqlite3";
import { SqliteArguments, SqliteValue } from "../common.js";
import {
  ExecuteOptions,
  ResultSet,
  RunResults,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  UpdateListener,
} from "../driver-api.js";
const Database = require("better-sqlite3");

import { ReadWriteConnectionPool } from "../driver-util.js";

export function betterSqlitePool(
  path: string,
  poolOptions?: bsqlite.Options
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return new BetterSqliteConnection(path, {
        ...poolOptions,
        readonly: (poolOptions?.readonly ?? options?.readonly) || false,
      });
    },
  });
}

export class BetterSqliteConnection implements SqliteDriverConnection {
  con: bsqlite.Database;

  constructor(path: string, options?: bsqlite.Options) {
    this.con = new Database(path, options);
  }

  async close() {
    this.con.close();
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
      rows,
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
          rows: buffer,
        };
        didYield = true;
        buffer = [];
      }
    }
    if (buffer.length > 0 || !didYield) {
      yield {
        columns,
        rows: buffer,
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
      lastInsertRowId: BigInt(r.lastInsertRowid),
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

    this.con.function("_logger", function (table: any, type: any, rowid: any) {
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
