import { nodeSqliteAsyncPool } from '@powersync/sqlite-js-driver/node';
import { describeDriverTests } from '../../lib/index.js';

describeDriverTests(
  'node:sqlite',
  { getColumns: false, rawResults: false, allowsMissingParameters: true },
  nodeSqliteAsyncPool
);
