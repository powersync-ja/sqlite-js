import { BetterSqliteDriver } from '../../lib/index.js';
import { describeDriverTests } from '@sqlite-js/driver-tests';
import { deleteDb } from './util.js';

describeDriverTests(
  'better-sqlite3',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  async (path) => {
    await deleteDb(path);
    return BetterSqliteDriver.openInProcess(path);
  }
);
