import { Database, OPEN_CREATE, OPEN_READONLY, OPEN_READWRITE } from "sqlite3";
import { SqliteArguments, SqliteValue } from "../common.js";
import {
  ExecuteOptions,
  ResultSet,
  RunResults,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
} from "../driver-api.js";

import { ReadWriteConnectionPool } from "../driver-util.js";

export function sqlite3Pool(
  path: string,
  poolOptions?: { readonly?: boolean }
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return new Sqlite3Connection(path, {
        ...poolOptions,
        readonly: (poolOptions?.readonly ?? options?.readonly) || false,
      });
    },
  });
}

/**
 * This is a partial implementation, with several limitations:
 * 1. bigint is not supported
 * 2. column names are not available when zero rows are returned
 * 3. duplicate column names are not supported
 *
 * This is due to limitations in the underlying driver. Rather use
 * other drivers when possible.
 */
export class Sqlite3Connection implements SqliteDriverConnection {
  con: Database;

  constructor(path: string, options?: { readonly?: boolean }) {
    let mode = options?.readonly ? OPEN_READONLY : OPEN_CREATE | OPEN_READWRITE;
    this.con = new Database(path, mode);
  }

  async selectAll(
    query: string,
    args?: SqliteArguments | undefined,
    options?: ExecuteOptions | undefined
  ): Promise<ResultSet> {
    const bindArgs = args == undefined ? [] : [args];

    return new Promise<ResultSet>((resolve, reject) => {
      this.con.all(query, bindArgs, (error, rows) => {
        if (error) {
          reject(error);
        } else {
          resolve(transformResults(rows, options));
        }
      });
    });
  }

  async *selectStreamed(
    query: string,
    args?: SqliteArguments,
    options?: ExecuteOptions
  ): AsyncGenerator<ResultSet, any, undefined> {
    yield this.selectAll(query, args, options);
  }

  async run(query: string, args?: SqliteArguments): Promise<void> {
    await this.runWithResults(query, args);
  }

  async runWithResults(
    query: string,
    args?: SqliteArguments
  ): Promise<RunResults> {
    const bindArgs = args == undefined ? [] : [args];
    return new Promise((resolve, reject) => {
      this.con.run(query, bindArgs, function (error) {
        if (error) {
          reject(error);
        } else {
          resolve({
            changes: this.changes,
            lastInsertRowId: BigInt(this.lastID),
          });
        }
      });
    });
  }

  async close() {
    this.con.close();
  }
}

function transformResults(rows: any[], options?: ExecuteOptions): ResultSet {
  if (rows.length == 0) {
    return {
      columns: [],
      rows: [],
    };
  } else {
    const columns = Object.keys(rows[0]);
    return {
      columns,
      rows: rows.map((row) => {
        if (options?.bigint) {
          return columns.map((column) => {
            const val = row[column];
            if (typeof val == "number") {
              return BigInt(val);
            } else {
              return val;
            }
          });
        } else {
          return columns.map((column) => row[column]);
        }
      }),
    };
  }
}
