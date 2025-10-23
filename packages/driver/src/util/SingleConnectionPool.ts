import {
  SqliteDriverConnectionPool,
  ReservedConnection,
  SqliteDriverConnection,
  ReserveConnectionOptions,
  UpdateListener
} from '../driver-api.js';
import { QueuedItem, ReservedConnectionImpl } from './connection-pools.js';

/**
 * Provides lock management for a single connection.
 */
export class SingleConnectionPool implements SqliteDriverConnectionPool {
  private queue: QueuedItem[] = [];
  private inUse: ReservedConnection | null = null;

  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;

  constructor(private connection: SqliteDriverConnection) {
    if (typeof Symbol.asyncDispose != 'undefined') {
      this[Symbol.asyncDispose] = () => this.close();
    }
  }

  async close() {
    await this.connection.close();
  }

  async reserveConnection(
    options?: ReserveConnectionOptions
  ): Promise<ReservedConnection> {
    console.log('single reserveConnection', this.connection.lock);
    if (options?.signal?.aborted) {
      throw new Error('Aborted');
    }
    const reserved: ReservedConnection = new ReservedConnectionImpl(
      this.connection,
      async () => {
        // TODO: sync
        if (this.inUse === reserved) {
          reserved.connection.release?.();
          this.inUse = null;
          Promise.resolve().then(() => this.next());
        }
      }
    );

    if (this.inUse == null) {
      this.inUse = reserved;
      await reserved.connection.lock?.(
        options?.readonly ? 'shared' : 'exclusive'
      );
      return reserved;
    } else {
      const promise = new Promise<ReservedConnection>((resolve, reject) => {
        const item: QueuedItem = {
          reserved,
          resolve,
          reject
        };
        this.queue.push(item);
        options?.signal?.addEventListener(
          'abort',
          () => {
            item.reserved = null;
            item.reject(new Error('Aborted'));
          },
          { once: true }
        );
      });

      return promise.then(async (r) => {
        this.inUse = reserved;
        await reserved.connection.lock?.(
          options?.readonly ? 'shared' : 'exclusive'
        );
        return r;
      });
    }
  }

  private next() {
    while (true) {
      const item = this.queue.shift();
      if (item == null) {
        break;
      }

      if (item.reserved == null) {
        // Aborted
        continue;
      }

      item.resolve(item.reserved);
      break;
    }
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    return this.connection.onUpdate(listener, options);
  }
}
