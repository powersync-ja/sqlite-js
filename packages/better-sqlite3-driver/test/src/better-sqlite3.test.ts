import { BetterSqliteDriver } from '../../lib/index.js';
import { describeDriverTests } from '@sqlite-js/driver-tests';

describeDriverTests(
  'better-sqlite3',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  (path) => BetterSqliteDriver.openInProcess(path)
);
