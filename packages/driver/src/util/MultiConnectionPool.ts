import {
  SqliteDriverConnectionPool,
  SqliteDriverConnection,
  ReserveConnectionOptions,
  ReservedConnection,
  UpdateListener
} from '../driver-api.js';
import {
  QueuedPoolItem,
  DriverFactory,
  ConnectionPoolOptions,
  ReservedConnectionImpl
} from './connection-pools.js';

/**
 * A connection pool with multiple connections.
 */
export class MultiConnectionPool implements SqliteDriverConnectionPool {
  private _allConnections = new Set<SqliteDriverConnection>();
  private _availableReadConnections: SqliteDriverConnection[] = [];
  private _queue: QueuedPoolItem[] = [];
  private _maxConnections: number;

  private options: ConnectionPoolOptions;

  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;

  constructor(
    private factory: DriverFactory,
    options?: ConnectionPoolOptions
  ) {
    if (typeof Symbol.asyncDispose != 'undefined') {
      this[Symbol.asyncDispose] = () => this.close();
    }
    this._maxConnections = options?.maxConnections ?? 2;
    this.options = options ?? {};
  }

  reserveConnection(
    options?: ReserveConnectionOptions | undefined
  ): Promise<ReservedConnection> {
    const promise = new Promise<ReservedConnection>((resolve, reject) => {
      this._queue.push({
        resolve,
        reject
      });
    });

    Promise.resolve().then(() => this.next());

    return promise;
  }

  private async expandPool(
    options?: ReserveConnectionOptions
  ): Promise<SqliteDriverConnection> {
    const connection = await this.factory.openConnection({
      ...this.options,
      ...options,
      connectionName: `connection-${this._allConnections.size + 1}`
    });
    this._allConnections.add(connection);
    return connection;
  }

  private async next() {
    if (this._queue.length == 0) {
      // Nothing queued
      return;
    }

    if (
      this._availableReadConnections.length == 0 &&
      this._allConnections.size >= this._maxConnections
    ) {
      // No connections available
      return;
    }

    const item = this._queue.shift()!;

    let connection: SqliteDriverConnection;
    if (this._availableReadConnections.length == 0) {
      // FIXME: prevent opening more than the max
      connection = await this.expandPool();
    } else {
      connection = this._availableReadConnections.shift()!;
    }

    item.resolve(
      new ReservedConnectionImpl(connection, async () => {
        /// TODO: sync
        this._availableReadConnections.push(connection);
        Promise.resolve().then(() => this.next());
      })
    );
  }

  async close() {
    // TODO: Wait for statements to finish
    for (let con of this._allConnections) {
      await con.close();
    }
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    // No-op
    return () => {};
  }
}
