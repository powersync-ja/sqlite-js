import { nodeSqliteAsyncPool } from '@powersync/sqlite-js-driver/node';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeImplTests } from './impl-tests.js';

describeImplTests(
  'node:sqlite',
  (path) => new ConnectionPoolImpl(nodeSqliteAsyncPool(path))
);