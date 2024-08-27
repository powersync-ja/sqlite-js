import { ConnectionPoolImpl, waSqlitePool } from '../../lib/index.js';
import { describeDriverTests } from './tests/driver-tests.js';
import { describeImplTests } from './tests/impl-tests.js';

describeDriverTests(
  'wa-sqlite',
  { getColumns: true, rawResults: false, allowsMissingParameters: true },
  (path: string) => waSqlitePool(':memory:')
);

describeImplTests(
  'wa-sqlite',
  (path) => new ConnectionPoolImpl(waSqlitePool(':memory:'))
);
