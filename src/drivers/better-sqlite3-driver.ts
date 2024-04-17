import { SqliteArguments, SqliteValue } from "../common.js";
import {
  ExecuteOptions,
  ResultSet,
  RunResults,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqliteDriverStatement,
} from "../driver-api.js";
import type * as bsqlite from "better-sqlite3";
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

  prepare(query: string): SqliteDriverStatement {
    const stmt = this.con.prepare(query);
    return new BetterSqliteStatement(stmt);
  }

  async close() {
    this.con.close();
  }
}

export class BetterSqliteStatement implements SqliteDriverStatement {
  constructor(private statement: bsqlite.Statement) {}

  async selectAll(
    args?: SqliteArguments | undefined,
    options?: ExecuteOptions | undefined
  ): Promise<ResultSet> {
    const bindArgs = args == undefined ? [] : [args];
    if (!this.statement.reader) {
      this.statement.run(...bindArgs);
      return { columns: [], rows: [] };
    }
    this.statement.raw();
    if (options?.bigint) {
      this.statement.safeIntegers();
    }
    const columns = this.statement.columns().map((c) => c.name);
    const rows = this.statement.all(...bindArgs) as SqliteValue[][];
    return {
      columns,
      rows,
    };
  }

  async *selectStreamed(
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): AsyncGenerator<ResultSet, any, undefined> {
    const bindArgs = args == undefined ? [] : [args];
    if (!this.statement.reader) {
      this.statement.run(...bindArgs);
      return;
    }
    this.statement.raw();
    if (options?.bigint) {
      this.statement.safeIntegers();
    }
    const columns = this.statement.columns().map((c) => c.name);
    let buffer: SqliteValue[][] = [];
    let didYield = false;
    for (let row of this.statement.iterate(...bindArgs)) {
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

  async run(args?: SqliteArguments): Promise<void> {
    const bindArgs = args == undefined ? [] : [args];
    this.statement.run(...bindArgs);
  }

  async runWithResults(args?: SqliteArguments): Promise<RunResults> {
    const bindArgs = args == undefined ? [] : [args];
    const r = this.statement.run(...bindArgs);
    return {
      changes: r.changes,
      lastInsertRowId: BigInt(r.lastInsertRowid),
    };
  }

  dispose(): void {
    // No-op
  }
}
