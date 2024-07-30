import { nodeSqliteAsyncPool } from '../../lib/drivers/node-sqlite-async-driver.js';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeDriverTests } from './tests/driver-tests.js';
import { describeImplTests } from './tests/impl-tests.js';

describeDriverTests(
  'node:sqlite',
  { getColumns: false, rawResults: false, allowsMissingParameters: true },
  nodeSqliteAsyncPool
);

describeImplTests(
  'node:sqlite',
  (path) => new ConnectionPoolImpl(nodeSqliteAsyncPool(path))
);
