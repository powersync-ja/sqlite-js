import * as sqlite from 'node:sqlite';
import { NodeSqliteConnection } from './node-sqlite-driver.js';
import { retriedOpen, setupDriverWorker } from './worker-driver.js';

setupDriverWorker({
  async openConnection(options) {
    return retriedOpen(() => {
      return new NodeSqliteConnection(new sqlite.DatabaseSync(options.path), {
        readonly: options.readonly,
        name: options.connectionName
      });
    }, 2_000);
  }
});
