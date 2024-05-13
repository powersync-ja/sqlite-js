import { betterSqliteAsyncPool } from '../../lib/drivers/better-sqlite3-async-driver.js';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeDriverTests } from './tests/drivers.js';
import { describeImplTests } from './tests/impl.js';

describeDriverTests('better-sqlite3-async-pool', betterSqliteAsyncPool);

describeImplTests(
  'better-sqlite3-async-pool',
  (path) => new ConnectionPoolImpl(betterSqliteAsyncPool(path))
);
