import { SqliteArguments, SqliteValue } from '../common';
import {
  ExecuteOptions,
  ResultSet,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqliteDriverStatement,
} from '../driver-api';
import * as bsqlite from 'better-sqlite3';
import { ReadWriteConnectionPool } from '../driver-util';

export function betterSqlitePool(
  path: string,
  poolOptions?: bsqlite.Options
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return new BetterSqliteConnection(path, {
        ...poolOptions,
        readonly: poolOptions?.readonly ?? options?.readonly,
      });
    },
  });
}

export class BetterSqliteConnection implements SqliteDriverConnection {
  con: bsqlite.Database;

  constructor(path: string, options?: bsqlite.Options) {
    this.con = new bsqlite(path, options);
  }

  prepare(query: string): SqliteDriverStatement {
    const stmt = this.con.prepare(query);
    stmt.raw();
    return new BetterSqliteStatement(stmt);
  }
}

export class BetterSqliteStatement implements SqliteDriverStatement {
  constructor(private statement: bsqlite.Statement) {}

  async *stream(
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): AsyncGenerator<ResultSet, any, undefined> {
    const columns = this.statement.columns().map((c) => c.name);
    let buffer: SqliteValue[][] = [];
    let didYield = false;
    for (let row of this.statement.iterate(args)) {
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
