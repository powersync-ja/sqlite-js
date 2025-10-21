import { describeDriverTests } from '@sqlite-js/driver-tests';
import { waSqliteWorkerPool } from '../../lib/index.js';

describeDriverTests(
  'wa-sqlite',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  async (path) => {
    console.log('open', path);
    return waSqliteWorkerPool(path);
  }
);
