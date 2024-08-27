import { describeDriverTests } from '@sqlite-js/driver-tests';
import { BetterSqliteDriver } from '../../lib/index.js';

describeDriverTests(
  'better-sqlite3-async-pool',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  (path) => BetterSqliteDriver.open(path)
);
