import type * as bsqlite from 'better-sqlite3';
import { createRequire } from 'node:module';
import { SqliteDriverConnectionPool } from '@powersync/sqlite-js-driver';

const require = createRequire(import.meta.url);

import { ReadWriteConnectionPool } from '@powersync/sqlite-js-driver/util';
import { WorkerDriverConnection } from '@powersync/sqlite-js-driver/worker_threads';

export function betterSqliteAsyncPool(
  path: string,
  poolOptions?: bsqlite.Options
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return new WorkerDriverConnection(
        require.resolve('./better-sqlite3-worker.js'),
        path,
        {
          ...poolOptions,
          readonly: (poolOptions?.readonly ?? options?.readonly) || false
        }
      );
    }
  });
}
