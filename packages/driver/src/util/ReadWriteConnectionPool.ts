import {
  SqliteDriverConnectionPool,
  ReserveConnectionOptions,
  ReservedConnection,
  UpdateListener
} from '../driver-api.js';
import { DriverFactory, ConnectionPoolOptions } from './connection-pools.js';
import { MultiConnectionPool } from './MultiConnectionPool.js';
import { SingleConnectionPool } from './SingleConnectionPool.js';

/**
 * A connection pool with a single write connection, and multiple read
 * connections.
 */
export class ReadWriteConnectionPool implements SqliteDriverConnectionPool {
  private writePool?: SqliteDriverConnectionPool;
  private readPool: SqliteDriverConnectionPool;

  private initPromise: Promise<void>;
  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;

  constructor(factory: DriverFactory, options?: ConnectionPoolOptions) {
    this.readPool = new MultiConnectionPool(factory, options);

    this.initPromise = factory
      .openConnection({ readonly: false, connectionName: 'writer' })
      .then((con) => {
        this.writePool = new SingleConnectionPool(con);
      });

    if (typeof Symbol.asyncDispose != 'undefined') {
      this[Symbol.asyncDispose] = () => this.close();
    }
  }

  async reserveConnection(
    options?: ReserveConnectionOptions
  ): Promise<ReservedConnection> {
    await this.initPromise;

    if (options?.readonly) {
      return this.readPool.reserveConnection(options);
    } else {
      return this.writePool!.reserveConnection(options);
    }
  }

  async close() {
    await this.readPool.close();
    await this.writePool?.close();
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    return this.writePool!.onUpdate(listener, options);
  }
}
