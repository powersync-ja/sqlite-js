import { NodeSqliteDriver } from '@sqlite-js/driver/node';
import { describeDriverTests } from '../../lib/index.js';

describeDriverTests(
  'node:sqlite worker',
  { getColumns: false, rawResults: false, allowsMissingParameters: true },
  (path) => NodeSqliteDriver.open(path)
);
