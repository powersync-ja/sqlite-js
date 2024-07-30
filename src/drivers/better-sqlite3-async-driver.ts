import type * as bsqlite from 'better-sqlite3';
import { createRequire } from 'node:module';
import { SqliteDriverConnectionPool } from '../driver-api.js';

const require = createRequire(import.meta.url);

import { ReadWriteConnectionPool } from '../driver-util.js';
import { AsyncDriverConnection } from './generic-async-driver.js';

export function betterSqliteAsyncPool(
  path: string,
  poolOptions?: bsqlite.Options
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return new AsyncDriverConnection(
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
