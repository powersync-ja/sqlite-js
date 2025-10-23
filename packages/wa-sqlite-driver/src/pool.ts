import { SqliteDriverConnectionPool } from '@sqlite-js/driver';
import {
  LazyConnectionPool,
  MultiConnectionPool,
  ReadWriteConnectionPool
} from '@sqlite-js/driver/util';
import {
  ReserveConnectionOptions,
  SqliteDriverConnection
} from '@sqlite-js/driver';

import { WorkerDriverConnection } from './worker_threads';
// import { WaSqliteConnection } from './wa-sqlite-driver';

// export function waSqlitePool(path: string): SqliteDriverConnectionPool {
//   return new LazyConnectionPool(async () => {
//     return await WaSqliteConnection.open(path);
//   });
// }

export function waSqliteSingleWorker(path: string): SqliteDriverConnectionPool {
  return new LazyConnectionPool(async () => {
    const connection = new WorkerDriverConnection(
      new Worker(new URL('./wa-sqlite-worker.js', import.meta.url), {
        type: 'module'
      }),
      { path }
    );
    await connection.open();
    return connection;
    // return await WaSqliteConnection.open(path);
  });
}

export function waSqliteWorkerPool(path: string): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool(
    {
      async openConnection(
        options?: ReserveConnectionOptions & { connectionName?: string }
      ): Promise<SqliteDriverConnection> {
        console.log('openConnection', options);
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
