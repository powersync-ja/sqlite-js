import { NodeSqliteDriver } from '@sqlite-js/driver/node';
import { describeDriverTests } from '../../lib/index.js';

import { isMocha, test } from '../../lib/test.js';
import { deleteDb } from './util.js';

if (isMocha) {
  describeDriverTests(
    'node:sqlite direct',
    { getColumns: false, rawResults: false, allowsMissingParameters: true },
    async (path) => {
      await deleteDb(path);
      return NodeSqliteDriver.openInProcess(path);
    }
  );
} else {
  test.skip('only running in mocha');
}
