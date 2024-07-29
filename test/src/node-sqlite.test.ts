import test from 'node:test';
import { nodeSqlitePool } from '../../lib/drivers/node-sqlite-driver.js';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeDriverTests } from './tests/driver-tests.js';
import { describeImplTests } from './tests/impl-tests.js';

test('it works', function () {});
describeDriverTests('nodesqlite', nodeSqlitePool);

// describeImplTests(
//   'node:sqlite',
//   (path) => new ConnectionPoolImpl(nodeSqlitePool(path))
// );
