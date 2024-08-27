import {
  ReserveConnectionOptions,
  ReservedConnection,
  SqliteDriverConnectionPool,
  UpdateListener
} from '@sqlite-js/driver';
import {
  ConnectionPoolOptions,
  LazyConnectionPool,
  ReadWriteConnectionPool
} from '@sqlite-js/driver/util';
import { WorkerDriverConnection } from '@sqlite-js/driver/worker_threads';
import type * as bsqlite from 'better-sqlite3';

export interface BetterSqliteDriverOptions extends ConnectionPoolOptions {
  /**
   * Specify a custom path to a worker script, to customize the loading process.
   */
  workerPath?: string | URL;

  loadExtensions?: string[];
}

export class BetterSqliteDriver implements SqliteDriverConnectionPool {
  /**
   * Opens a single in-process connection.
   *
   * Uses blocking I/O.
   */
  static openInProcess(
    path: string,
    options?: bsqlite.Options
  ): BetterSqliteDriver {
    const connection = new LazyConnectionPool(async () => {
      const { BetterSqliteConnection } = await import('./sync-driver.js');
      return BetterSqliteConnection.open(path, options);
    });
    return new BetterSqliteDriver(connection);
  }

  /**
   * Opens a connection pool with non-blocking I/O using worker_threads.
   */
  static open(
    path: string,
    options?: BetterSqliteDriverOptions
  ): BetterSqliteDriver {
    const workerPath =
      options?.workerPath ?? new URL('./worker.js', import.meta.url);

    const connection = new ReadWriteConnectionPool({
      async openConnection(connectionOptions) {
        return new WorkerDriverConnection(workerPath, {
          path,
          ...options,
          readonly: connectionOptions?.readonly || false
        });
      }
    });
    return new BetterSqliteDriver(connection);
  }

  private constructor(private connection: SqliteDriverConnectionPool) {}

  reserveConnection(
    options?: ReserveConnectionOptions
  ): Promise<ReservedConnection> {
    return this.connection.reserveConnection(options);
  }

  close(): Promise<void> {
    return this.connection.close();
  }

  onUpdate(
    listener: UpdateListener,
    options?: { tables?: string[]; batchLimit?: number }
  ): () => void {
    return this.connection.onUpdate(listener, options);
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.connection[Symbol.asyncDispose]();
  }
}
