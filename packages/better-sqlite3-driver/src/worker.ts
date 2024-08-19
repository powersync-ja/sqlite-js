import { BetterSqliteDriverOptions } from './driver.js';
import { BetterSqliteConnection } from './sync-driver.js';
import {
  retriedOpen,
  setupDriverWorker,
  ConnectionOptions
} from '@sqlite-js/driver/worker_threads/setup';

setupDriverWorker({
  async openConnection(options: ConnectionOptions & BetterSqliteDriverOptions) {
    return retriedOpen(() => {
      return BetterSqliteConnection.open(options.path, options);
    }, 2_000);
  }
});
