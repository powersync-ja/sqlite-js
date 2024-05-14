import type * as bsqlite from 'better-sqlite3';
import * as worker_threads from 'worker_threads';
import {
  CommandResult,
  SqliteCommand,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  UpdateListener
} from '../driver-api.js';

import { ReadWriteConnectionPool } from '../driver-util.js';

export function betterSqliteAsyncPool(
  path: string,
  poolOptions?: bsqlite.Options
): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      return new BetterSqliteAsyncConnection(path, {
        ...poolOptions,
        readonly: (poolOptions?.readonly ?? options?.readonly) || false
      });
    }
  });
}

export class BetterSqliteAsyncConnection implements SqliteDriverConnection {
  worker: worker_threads.Worker;
  private callbacks = new Map<number, (value: any) => void>();
  private nextCallbackId = 1;
  private ready: Promise<void>;
  private closing = false;

  constructor(path: string, options?: bsqlite.Options) {
    const worker = new worker_threads.Worker(
      require.resolve('./better-sqlite3-worker.js')
    );
    this.post('open', { path, options });
    worker.addListener('error', (err) => {
      console.error('worker error', err);
    });
    this.ready = new Promise<void>((resolve) => {
      worker.addListener('message', (event) => {
        const { id, value } = event;
        if (id == 0) {
          resolve();
          return;
        }
        const callback = this.callbacks.get(id);
        if (callback == null) {
          throw new Error(`No callback with id ${id}`);
        }
        this.callbacks.delete(id);
        callback(value);
      });
    });
    this.worker = worker;
  }

  private registerCallback(callback: (value: any) => void) {
    const id = this.nextCallbackId++;
    this.callbacks.set(id, callback);
    return id;
  }

  private async post<T>(command: string, args: any) {
    await this.ready;
    let id: number;
    const p = new Promise<T>((resolve) => {
      id = this.registerCallback(resolve);
    });
    this.worker.postMessage([command, id!, args]);
    return p;
  }

  async close() {
    if (this.closing) {
      return;
    }
    this.closing = true;
    await this.post('close', {});
    await this.worker.terminate();
  }

  async execute(commands: SqliteCommand[]): Promise<CommandResult[]> {
    return await this.post('execute', commands);
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    throw new Error('Not implemented');
  }
}
