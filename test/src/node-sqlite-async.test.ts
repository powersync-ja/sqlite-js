import { nodeSqliteAsyncPool } from '../../lib/drivers/node-sqlite-async-driver.js';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeDriverTests } from './tests/driver-tests.js';
import { describeImplTests } from './tests/impl-tests.js';
import { isMocha, test } from './tests/test.js';

if (isMocha) {
  describeDriverTests(
    'node:sqlite',
    { getColumns: false, rawResults: false, allowsMissingParameters: true },
    nodeSqliteAsyncPool
  );

  describeImplTests(
    'node:sqlite',
    (path) => new ConnectionPoolImpl(nodeSqliteAsyncPool(path))
  );
} else {
  test.skip('only running in mocha');
}
