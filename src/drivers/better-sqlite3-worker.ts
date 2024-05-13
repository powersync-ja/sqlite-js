import { BetterSqliteConnection } from './better-sqlite3-driver.js';

import * as worker_threads from 'worker_threads';

const port = worker_threads.parentPort;
if (port != null) {
  let db: BetterSqliteConnection | null = null;

  port.addListener('message', async (value) => {
    const [message, id, args] = value;

    if (message == 'open') {
      db = new BetterSqliteConnection(args.path);
      port.postMessage({ id });
    } else if (message == 'close') {
      db?.close();
      port.postMessage({ id });
    } else if (message == 'execute') {
      const commands = args;

      const results = await db!.execute(commands);
      port.postMessage({
        id,
        value: results
      });
    } else {
      throw new Error(`Unknown message: ${message}`);
    }
  });

  port.postMessage({ id: 0, value: 'ready' });
}
