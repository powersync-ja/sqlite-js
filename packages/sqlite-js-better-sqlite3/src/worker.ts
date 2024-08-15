import { BetterSqliteConnection } from './sync-driver.js';
import {
  retriedOpen,
  setupDriverWorker
} from '@powersync/sqlite-js-driver/worker_threads/setup';

setupDriverWorker({
  async openConnection(options) {
    return retriedOpen(() => {
      return BetterSqliteConnection.open(options.path, options);
    }, 2_000);
  }
});
