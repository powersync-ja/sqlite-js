import { SqliteArguments, SqliteValue } from "../common.js";
import {
  ExecuteOptions,
  ResultSet,
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
    console.log("options", options);
    this.con = new Database(path, options);
  }

  prepare(query: string): SqliteDriverStatement {
    const stmt = this.con.prepare(query);
    return new BetterSqliteStatement(stmt);
  }
}

export class BetterSqliteStatement implements SqliteDriverStatement {
  constructor(private statement: bsqlite.Statement) {}

  async *stream(
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
    for (let row of this.statement.all(...bindArgs)) {
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

  async execute(args?: SqliteArguments): Promise<void> {
    this.statement.run();
  }

  dispose(): void {
    // No-op
  }
}
