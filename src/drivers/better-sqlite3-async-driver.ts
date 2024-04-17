import type * as bsqlite from "better-sqlite3";
import * as worker_threads from "worker_threads";
import {
  ResultSet,
  RunResults,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
} from "../driver-api.js";

import { EventIterator } from "event-iterator";
import { StreamedExecuteOptions } from "../api.js";
import { SqliteArguments } from "../common.js";
import { ReadWriteConnectionPool } from "../driver-util.js";

export function betterSqliteAsyncPool(
  path: string,
  poolOptions?: bsqlite.Options
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return new BetterSqliteAsyncConnection(path, {
        ...poolOptions,
        readonly: (poolOptions?.readonly ?? options?.readonly) || false,
      });
    },
  });
}

export class BetterSqliteAsyncConnection implements SqliteDriverConnection {
  worker: worker_threads.Worker;

  constructor(path: string, options?: bsqlite.Options) {
    const worker = new worker_threads.Worker(
      require.resolve("./better-sqlite3-worker.js")
    );
    worker.postMessage(["open", { path, options }]);
    worker.addListener("error", (err) => {
      console.error("worker error", err);
    });
    this.worker = worker;
  }

  async close() {
    this.worker.postMessage(["close"]);
    await new Promise<void>((resolve, reject) => {
      this.worker.once("message", (value) => {
        resolve();
      });
    });
    await this.worker.terminate();
  }

  async run(query: string, args: SqliteArguments) {
    this.worker.postMessage(["run", { query, args }]);
    return new Promise<void>((resolve, reject) => {
      this.worker.once("message", (value) => {
        resolve();
      });
    });
  }
  async runWithResults(query: string, args: SqliteArguments) {
    this.worker.postMessage(["run", { query, args }]);
    return new Promise<RunResults>((resolve, reject) => {
      this.worker.once("message", (value) => {
        resolve(value);
      });
    });
  }
  async *selectStreamed(
    query: string,
    args: SqliteArguments,
    options: StreamedExecuteOptions
  ) {
    this.worker.postMessage(["stream", { query, args, options }]);
    const iter = new EventIterator(({ push }) => {
      this.worker.addListener("message", push);
      return () => this.worker.removeListener("message", push);
    });
    let columns: string[] = [];
    for await (let message of iter) {
      const [type, args] = message as any;
      if (type == "columns") {
        columns = args;
      } else if (type == "rows") {
        yield { columns, rows: args };
      } else if (type == "close") {
        break;
      }
    }
  }

  async selectAll(
    query: string,
    args: SqliteArguments,
    options: StreamedExecuteOptions
  ): Promise<ResultSet> {
    let results: ResultSet | undefined = undefined;
    for await (let rs of this.selectStreamed(query, args, options)) {
      if (results == null) {
        results = rs;
      } else {
        results!.rows.push(...rs.rows);
      }
    }
    return results!;
  }
}
