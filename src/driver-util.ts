import {
  PrepareOptions,
  ReserveConnectionOptions,
  ReservedConnection,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqliteDriverStatement,
  UpdateListener
} from './driver-api.js';

import * as os from 'node:os';
interface QueuedItem {
  reserved: ReservedConnection | null;
  resolve: (reserved: ReservedConnection) => void;
  reject: (err: any) => void;
}

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

  reserveConnection(
    options?: ReserveConnectionOptions
  ): Promise<ReservedConnection> {
    if (options?.signal?.aborted) {
      throw new Error('Aborted');
    }
    const reserved: ReservedConnection = new ReservedConnectionImpl(
      this.connection,
      async () => {
        // TODO: sync
        if (this.inUse === reserved) {
          this.inUse = null;
          Promise.resolve().then(() => this.next());
        }
      }
    );

    if (this.inUse == null) {
      this.inUse = reserved;
      return Promise.resolve(reserved);
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

      return promise;
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

export interface DriverFactory {
  openConnection(
    options?: ReserveConnectionOptions & { name?: string }
  ): Promise<SqliteDriverConnection>;
}

interface QueuedPoolItem {
  resolve: (reserved: ReservedConnection) => void;
  reject: (err: any) => void;
}

class ReservedConnectionImpl implements ReservedConnection {
  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;

  constructor(
    public connection: SqliteDriverConnection,
    public release: () => Promise<void>
  ) {
    if (typeof Symbol.asyncDispose != 'undefined') {
      this[Symbol.asyncDispose] = release;
    }
  }

  /** Proxied to the underlying connection */
  prepare(sql: string, options?: PrepareOptions): SqliteDriverStatement {
    return this.connection.prepare(sql, options);
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    return this.connection.onUpdate(listener, options);
  }

  close(): Promise<void> {
    return this.connection.close();
  }
}

class MultiConnectionPool implements SqliteDriverConnectionPool {
  private _allConnections = new Set<SqliteDriverConnection>();
  private _availableReadConnections: SqliteDriverConnection[] = [];
  private _queue: QueuedPoolItem[] = [];
  private _maxConnections: number = os.cpus().length;

  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;

  constructor(private factory: DriverFactory) {
    if (typeof Symbol.asyncDispose != 'undefined') {
      this[Symbol.asyncDispose] = () => this.close();
    }
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
      ...options,
      name: `connection-${this._allConnections.size + 1}`
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

export class ReadWriteConnectionPool implements SqliteDriverConnectionPool {
  private writePool?: SqliteDriverConnectionPool;
  private readPool: SqliteDriverConnectionPool;

  private initPromise: Promise<void>;
  [Symbol.asyncDispose]: () => Promise<void> = undefined as any;

  constructor(private factory: DriverFactory) {
    this.readPool = new MultiConnectionPool(factory);

    this.initPromise = factory
      .openConnection({ readonly: false, name: 'writer' })
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
