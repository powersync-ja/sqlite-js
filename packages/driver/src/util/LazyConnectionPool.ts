import {
  ReserveConnectionOptions,
  ReservedConnection,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  UpdateListener
} from '../driver-api.js';
import { SingleConnectionPool } from './SingleConnectionPool.js';

/**
 * Manages locks for a single connection, created asynchronously.
 */
export class LazyConnectionPool implements SqliteDriverConnectionPool {
  private initPromise: Promise<void>;
  private connection?: SingleConnectionPool;

  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;
  constructor(open: () => Promise<SqliteDriverConnection>) {
    if (typeof Symbol.asyncDispose != 'undefined') {
      this[Symbol.asyncDispose] = () => this.close();
    }

    this.initPromise = open().then((c) => {
      this.connection = new SingleConnectionPool(c);
    });
  }

  async reserveConnection(
    options?: ReserveConnectionOptions
  ): Promise<ReservedConnection> {
    await this.initPromise;
    return this.connection!.reserveConnection(options);
  }

  async close(): Promise<void> {
    await this.initPromise;
    await this.connection!.close();
  }

  onUpdate(
    listener: UpdateListener,
    options?: { tables?: string[]; batchLimit?: number }
  ): () => void {
    throw new Error('Method not implemented.');
  }
}
