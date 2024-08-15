import { BetterSqliteConnection } from './better-sqlite3-driver.js';
import { retriedOpen, setupDriverWorker } from './worker-driver.js';

setupDriverWorker({
  async openConnection(options) {
    return retriedOpen(() => {
      return BetterSqliteConnection.open(options.path, options);
    }, 2_000);
  }
});
