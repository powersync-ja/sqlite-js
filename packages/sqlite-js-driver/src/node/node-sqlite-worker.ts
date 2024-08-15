import { NodeSqliteConnection } from './node-sqlite-driver.js';
import { retriedOpen, setupDriverWorker } from '../worker_threads/setup.js';
import { loadNodeSqlite } from './node-sqlite.js';

setupDriverWorker({
  async openConnection(options) {
    const sqlite = await loadNodeSqlite();
    return retriedOpen(() => {
      return new NodeSqliteConnection(new sqlite.DatabaseSync(options.path), {
        readonly: options.readonly,
        name: options.connectionName
      });
    }, 2_000);
  }
});
