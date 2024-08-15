import { betterSqliteAsyncPool } from '../../lib/better-sqlite3-async-driver.js';

import { describeDriverTests } from '@powersync/sqlite-js-driver-tests';

describeDriverTests(
  'better-sqlite3-async-pool',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  betterSqliteAsyncPool
);
