import { Deferred } from '../deferred.js';
import { isErrorResponse, WorkerDriver } from './async-commands.js';
import { setTimeout } from 'timers/promises';

import * as worker_threads from 'worker_threads';
import type { WorkerDriverConnectionOptions } from './worker-driver.js';
import { SqliteDriverConnection } from '../driver-api.js';
import { WorkerConnectionAdapter } from './WorkerDriverAdapter.js';

export type { WorkerDriverConnectionOptions };

export interface WorkerDriverConfig {
  openConnection: (
    options: WorkerDriverConnectionOptions
  ) => Promise<SqliteDriverConnection>;
}

export function setupDriverWorker(config: WorkerDriverConfig) {
  if (worker_threads.parentPort != null) {
    return setupDriverPort(worker_threads.parentPort, config);
  }
}

export function setupDriverPort(
  port: worker_threads.MessagePort,
  config: WorkerDriverConfig
) {
  let db: WorkerDriver | null = null;
  let opened = new Deferred<void>();

  const listener = async (value: any) => {
    const [message, id, args] = value;

    if (message == 'open') {
      try {
        const connection = await config.openConnection(
          args as WorkerDriverConnectionOptions
        );
        db = new WorkerConnectionAdapter(connection);
        port.postMessage({ id });
        opened.resolve();
      } catch (e: any) {
        opened.reject(e);
        port.postMessage({ id, value: { error: { message: e.message } } });
      }
    } else if (message == 'close') {
      try {
        await opened.promise;
        await db?.close();
        port.postMessage({ id });
      } catch (e: any) {
        port.postMessage({ id, value: { error: { message: e.message } } });
      }
    } else if (message == 'execute') {
      await opened.promise;
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
  };

  port.addListener('message', listener);
  port.postMessage({ id: 0, value: 'ready' });

  return () => {
    port.removeListener('message', listener);
  };
}

export async function retriedOpen(
  open: () => SqliteDriverConnection,
  timeout: number
) {
  const endTime = performance.now() + timeout;
  let delay = 1;
  while (true) {
    try {
      return open();
    } catch (e) {
      console.error(e);
      if (performance.now() >= endTime) {
        throw e;
      }
      await setTimeout(delay);
      delay = Math.min(delay * 1.5, 60);
    }
  }
}
