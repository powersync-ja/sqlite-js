import { NodeSqliteDriver } from '@sqlite-js/driver/node';
import { describeDriverTests } from '../../lib/index.js';
import { deleteDb } from './util.js';

describeDriverTests(
  'node:sqlite worker',
  { getColumns: false, rawResults: false, allowsMissingParameters: true },
  async (path) => {
    await deleteDb(path);
    return NodeSqliteDriver.open(path);
  }
);
