import { BetterSqliteDriver } from '@sqlite-js/better-sqlite3-driver';
import { SqliteClientPool, SqliteClientOptions, SqliteClient } from './core.js';
import { fileURLToPath } from 'node:url';

export class BetterSqliteClient extends SqliteClient {
  constructor(connectionUrl: string | URL, options: SqliteClientOptions = {}) {
    if (typeof connectionUrl != 'string') {
      connectionUrl = fileURLToPath(connectionUrl);
    }
    const pool = BetterSqliteDriver.openInProcess(connectionUrl);
    super(connectionUrl, pool, options);
  }
}

export class BetterSqliteClientPool extends SqliteClientPool {
  constructor(connectionUrl: string | URL, options: SqliteClientOptions = {}) {
    if (typeof connectionUrl != 'string') {
      connectionUrl = fileURLToPath(connectionUrl);
    }
    const pool = BetterSqliteDriver.open(connectionUrl);
    super(connectionUrl, pool, options);
  }
}
