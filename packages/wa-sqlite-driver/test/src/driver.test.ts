import { describeDriverTests } from '@sqlite-js/driver-tests';
import { waSqliteSingleWorker } from '../../lib/index.js';

describeDriverTests(
  'wa-sqlite',
  { getColumns: true, rawResults: true, allowsMissingParameters: false },
  async (path) => {
    console.log('open', path);
    return waSqliteSingleWorker(path);
  }
);
