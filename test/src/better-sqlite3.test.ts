import { betterSqlitePool } from '../../lib/drivers/better-sqlite3-driver.js';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeDriverTests } from './tests/drivers.js';
import { describeImplTests } from './tests/impl.js';

describeDriverTests('better-sqlite3', betterSqlitePool);

describeImplTests(
  'better-sqlite3',
  (path) => new ConnectionPoolImpl(betterSqlitePool(path))
);
