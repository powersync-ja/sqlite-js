import * as sqlite from 'node:sqlite';
import * as worker_threads from 'worker_threads';
import { SqliteDriverError } from '../driver-api.js';
import { NodeSqliteConnection } from './node-sqlite-driver.js';
import { Deferred } from '../deferred.js';

const port = worker_threads.parentPort;
if (port != null) {
  let db: NodeSqliteConnection | null = null;
  let opened = new Deferred<void>();

  port.addListener('message', async (value) => {
    const [message, id, args] = value;

    if (message == 'open') {
      const options = args.options;
      for (let i = 0; i < 10; i++) {
        try {
          db = new NodeSqliteConnection(new sqlite.DatabaseSync(args.path), {
            readonly: options.readonly,
            name: options.name
          });
          break;
        } catch (e) {
          if (i == 9) {
            opened.reject(e);
            throw e;
          }
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      }
      opened.resolve();
      port.postMessage({ id });
    } else if (message == 'close') {
      await opened.promise;
      try {
        await db!.close();
        port.postMessage({ id });
      } catch (e: any) {
        port.postMessage({ id, value: { error: { message: e.message } } });
      }
    } else if (message == 'execute') {
      await opened.promise;
      if (db == null) {
        throw new Error('database is not open');
      }
      const commands = args;

      const results = (await db!.execute(commands)).map((r) => {
        const error = (r as any)?.error as SqliteDriverError | undefined;
        if (error) {
          return {
            error: {
              code: error.code,
              message: error.message,
              stack: error.message
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
