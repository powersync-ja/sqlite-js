import { describeDriverTests } from '@sqlite-js/driver-tests';
import { BetterSqliteDriver } from '../../lib/index.js';
import { deleteDb } from './util.js';

describeDriverTests(
  'better-sqlite3-async-pool',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  async (path) => {
    await deleteDb(path);
    return BetterSqliteDriver.open(path);
  }
);
