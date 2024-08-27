import {
  PrepareOptions,
  ReserveConnectionOptions,
  ReservedConnection,
  SqliteDriverConnection,
  SqliteDriverStatement,
  UpdateListener
} from '../driver-api.js';

export interface QueuedItem {
  reserved: ReservedConnection | null;
  resolve: (reserved: ReservedConnection) => void;
  reject: (err: any) => void;
}

export interface DriverFactory {
  openConnection(
    options?: ReserveConnectionOptions & { connectionName?: string }
  ): Promise<SqliteDriverConnection>;
}

export interface QueuedPoolItem {
  resolve: (reserved: ReservedConnection) => void;
  reject: (err: any) => void;
}

export class ReservedConnectionImpl implements ReservedConnection {
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

export interface ConnectionPoolOptions {
  maxConnections?: number;
}
