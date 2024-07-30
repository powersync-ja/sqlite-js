import { nodeSqlitePool } from '../../lib/drivers/node-sqlite-driver.js';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeDriverTests } from './tests/driver-tests.js';
import { describeImplTests } from './tests/impl-tests.js';
import { isMocha } from './tests/test.js';

if (isMocha) {
  describeDriverTests(
    'node:sqlite',
    { getColumns: false, rawResults: false },
    nodeSqlitePool
  );
}

// describeImplTests(
//   'node:sqlite',
//   (path) => new ConnectionPoolImpl(nodeSqlitePool(path))
// );
