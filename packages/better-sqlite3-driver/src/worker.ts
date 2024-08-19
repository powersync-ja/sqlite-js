import { BetterSqliteDriverOptions } from './driver.js';
import { BetterSqliteConnection } from './sync-driver.js';
import {
  retriedOpen,
  setupDriverWorker,
  WorkerDriverConnectionOptions
} from '@sqlite-js/driver/worker_threads/setup';

setupDriverWorker({
  async openConnection(
    options: WorkerDriverConnectionOptions & BetterSqliteDriverOptions
  ) {
    return retriedOpen(() => {
      return BetterSqliteConnection.open(options.path, options);
    }, 2_000);
  }
});
