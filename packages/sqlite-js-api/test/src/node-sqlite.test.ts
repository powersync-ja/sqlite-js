import { NodeSqliteDriver } from '@powersync/sqlite-js-driver/node';
import { ConnectionPoolImpl } from '../../lib/impl.js';
import { describeImplTests } from './impl-tests.js';

describeImplTests(
  'node:sqlite',
  (path) => new ConnectionPoolImpl(NodeSqliteDriver.open(path))
);
