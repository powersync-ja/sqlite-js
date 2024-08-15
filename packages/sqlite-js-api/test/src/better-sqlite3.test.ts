import { betterSqliteAsyncPool } from '@powersync/sqlite-js-better-sqlite3';

import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeImplTests } from './impl-tests.js';

describeImplTests(
  'better-sqlite3',
  (path) => new ConnectionPoolImpl(betterSqliteAsyncPool(path))
);
