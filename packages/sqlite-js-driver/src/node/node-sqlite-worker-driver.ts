import type { SqliteDriverConnectionPool } from '../driver-api.js';
import { ReadWriteConnectionPool } from '../util/index.js';
import { WorkerDriverConnection } from '../worker_threads/worker-driver.js';

export function nodeSqliteAsyncPool(path: string): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      const con = new WorkerDriverConnection(
        import.meta.resolve('./node-sqlite-worker.js'),
        path,
        {
          readonly: options?.readonly ?? false,
          name: options?.name
        }
      );

      return con;
    }
  });
}
