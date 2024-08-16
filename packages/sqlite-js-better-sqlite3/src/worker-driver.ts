import type { SqliteDriverConnectionPool } from '@powersync/sqlite-js-driver';
import type * as bsqlite from 'better-sqlite3';

import { ReadWriteConnectionPool } from '@powersync/sqlite-js-driver/util';
import { WorkerDriverConnection } from '@powersync/sqlite-js-driver/worker_threads';

export function betterSqliteAsyncPool(
  path: string,
  poolOptions?: bsqlite.Options
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return new WorkerDriverConnection(
        new URL('./better-sqlite3-worker.js', import.meta.url),
        path,
        {
          ...poolOptions,
          readonly: (poolOptions?.readonly ?? options?.readonly) || false
        }
      );
    }
  });
}
