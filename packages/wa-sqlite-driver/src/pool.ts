import {
  ReserveConnectionOptions,
  SqliteDriverConnection,
  SqliteDriverConnectionPool
} from '@sqlite-js/driver';
import {
  LazyConnectionPool,
  ReadWriteConnectionPool
} from '@sqlite-js/driver/util';

import { WorkerDriverConnection } from './worker_threads';

export function waSqliteSingleWorker(path: string): SqliteDriverConnectionPool {
  return new LazyConnectionPool(async () => {
    const worker = new Worker(
      new URL('./wa-sqlite-worker.js', import.meta.url),
      {
        type: 'module'
      }
    );
    const connection = new WorkerDriverConnection(worker, { path });
    await connection.open();
    (connection as any).terminate = () => {
      worker.terminate();
    };
    return connection;
  });
}

export function waSqliteWorkerPool(path: string): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool(
    {
      async openConnection(
        options?: ReserveConnectionOptions & { connectionName?: string }
      ): Promise<SqliteDriverConnection> {
        const connection = new WorkerDriverConnection(
          new Worker(new URL('./wa-sqlite-worker.js', import.meta.url), {
            type: 'module'
          }),
          {
            path,
            readonly: options?.readonly ?? false,
            connectionName: options?.connectionName
          }
        );
        await connection.open();
        return connection;
      }
    },
    { maxConnections: 5 }
  );
}
