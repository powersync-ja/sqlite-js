import { isErrorResponse } from './async-commands.js';
import { BetterSqliteConnection } from './better-sqlite3-driver.js';

import * as worker_threads from 'worker_threads';

const port = worker_threads.parentPort;
if (port != null) {
  let db: BetterSqliteConnection | null = null;

  port.addListener('message', async (value) => {
    const [message, id, args] = value;

    if (message == 'open') {
      db = new BetterSqliteConnection(args.path, args.options);
      port.postMessage({ id });
    } else if (message == 'close') {
      try {
        await db!.close();
        port.postMessage({ id });
      } catch (e: any) {
        port.postMessage({ id, value: { error: { message: e.message } } });
      }
    } else if (message == 'execute') {
      const commands = args;

      const results = (await db!.execute(commands)).map((r) => {
        if (isErrorResponse(r)) {
          const error = r.error;
          return {
            error: {
              code: error.code,
              message: error.message,
              stack: error.stack
            }
          };
        } else {
          return r;
        }
      });
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
