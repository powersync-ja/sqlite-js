import type * as bsqlite from "better-sqlite3";
import { SqliteArguments, SqliteValue } from "../common.js";
import {
  ExecuteOptions,
  ResultSet,
  RunResults,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
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
}
