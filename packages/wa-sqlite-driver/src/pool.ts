import { SqliteDriverConnectionPool } from '@sqlite-js/driver';
import { LazyConnectionPool } from '@sqlite-js/driver/util';
import { WorkerDriverConnection } from './worker_threads';
// import { WaSqliteConnection } from './wa-sqlite-driver';

// export function waSqlitePool(path: string): SqliteDriverConnectionPool {
//   return new LazyConnectionPool(async () => {
//     return await WaSqliteConnection.open(path);
//   });
// }

export function waSqliteWorkerPool(path: string): SqliteDriverConnectionPool {
  return new LazyConnectionPool(async () => {
    return new WorkerDriverConnection(
      new Worker(new URL('./wa-sqlite-worker.js', import.meta.url), {
        type: 'module'
      }),
      { path }
    );
    // return await WaSqliteConnection.open(path);
  });
}
