import { betterSqlitePool } from '../../lib/better-sqlite3-driver.js';
import { describeDriverTests } from '@powersync/sqlite-js-driver-tests';

describeDriverTests(
  'better-sqlite3',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  betterSqlitePool
);
