import {
  ReserveConnectionOptions,
  ReservedConnection,
  SqliteDriverConnectionPool,
  UpdateListener
} from '../driver-api.js';
import { ConnectionPoolOptions } from '../util/connection-pools.js';
import { LazyConnectionPool } from '../util/LazyConnectionPool.js';
import { ReadWriteConnectionPool } from '../util/ReadWriteConnectionPool.js';
import { WorkerDriverConnection } from '../worker_threads/worker-driver.js';
import { NodeSqliteConnection } from './impl.js';
import { loadNodeSqlite } from './node-sqlite.js';

export interface NodeSqliteDriverOptions extends ConnectionPoolOptions {
  /**
   * Specify a custom path to a worker script, to customize the loading process.
   */
  workerPath?: string | URL;
}

export class NodeSqliteDriver implements SqliteDriverConnectionPool {
  /**
   * Opens a single in-process connection.
   *
   * Uses blocking I/O.
   *
   * This requires `NODE_OPTIONS=--experimental-sqlite`.
   */
  static openInProcess(path: string): NodeSqliteDriver {
    const connection = new LazyConnectionPool(async () => {
      const sqlite = await loadNodeSqlite();
      const db = new sqlite.DatabaseSync(path);
      return new NodeSqliteConnection(db);
    });
    return new NodeSqliteDriver(connection);
  }

  /**
   * Opens a connection pool with non-blocking I/O using worker_threads.
   */
  static open(
    path: string,
    options?: NodeSqliteDriverOptions
  ): NodeSqliteDriver {
    const workerPath =
      options?.workerPath ?? new URL('./worker.js', import.meta.url);

    const connection = new ReadWriteConnectionPool(
      {
        openConnection: async (connectionOptions) => {
          return new WorkerDriverConnection(workerPath, path, {
            readonly: connectionOptions?.readonly ?? false,
            connectionName: connectionOptions?.connectionName,
            workerOptions: {
              env: {
                ...process.env,
                NODE_OPTIONS:
                  '--experimental-sqlite --disable-warning=ExperimentalWarning'
              }
            }
          });
        }
      },
      options
    );
    return new NodeSqliteDriver(connection);
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
