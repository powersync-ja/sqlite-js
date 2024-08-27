import { describeDriverTests } from '@sqlite-js/driver-tests';
import { waSqlitePool } from '../../lib/index.js';

describeDriverTests(
  'wa-sqlite',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  async (path) => {
    return waSqlitePool(':memory:');
  }
);
