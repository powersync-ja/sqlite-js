import { betterSqliteAsyncPool } from '../../lib/drivers/better-sqlite3-async-driver.js';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeDriverTests } from './tests/driver-tests.js';
import { describeImplTests } from './tests/impl-tests.js';

describeDriverTests(
  'better-sqlite3-async-pool',
  { getColumns: true, rawResults: true },
  betterSqliteAsyncPool
);

describeImplTests(
  'better-sqlite3-async-pool',
  (path) => new ConnectionPoolImpl(betterSqliteAsyncPool(path))
);
