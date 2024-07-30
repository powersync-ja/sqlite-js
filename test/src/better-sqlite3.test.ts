import { betterSqlitePool } from '../../lib/drivers/better-sqlite3-driver.js';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeDriverTests } from './tests/driver-tests.js';
import { describeImplTests } from './tests/impl-tests.js';

describeDriverTests(
  'better-sqlite3',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  betterSqlitePool
);

describeImplTests(
  'better-sqlite3',
  (path) => new ConnectionPoolImpl(betterSqlitePool(path))
);
