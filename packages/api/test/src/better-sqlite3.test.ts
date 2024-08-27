import { BetterSqliteDriver } from '@sqlite-js/better-sqlite3-driver';

import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeImplTests } from './impl-tests.js';

describeImplTests(
  'better-sqlite3',
  (path) => new ConnectionPoolImpl(BetterSqliteDriver.open(path))
);
