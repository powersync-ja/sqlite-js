import { betterSqlitePool } from '../../lib/sync-driver.js';
import { describeDriverTests } from '@powersync/sqlite-js-driver-tests';

describeDriverTests(
  'better-sqlite3',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  betterSqlitePool
);
