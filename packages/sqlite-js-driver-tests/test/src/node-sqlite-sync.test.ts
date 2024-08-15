import { nodeSqlitePool } from '@powersync/sqlite-js-driver/node';
import { describeDriverTests } from '../../lib/index.js';

import { test, isMocha } from '../../lib/test.js';

if (isMocha) {
  describeDriverTests(
    'node:sqlite',
    { getColumns: false, rawResults: false, allowsMissingParameters: true },
    nodeSqlitePool
  );
} else {
  test.skip('only running in mocha');
}
