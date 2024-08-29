import type * as sqlite from './node-sqlite.js';

import {
  PrepareOptions,
  QueryOptions,
  SqliteArrayRow,
  SqliteChanges,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqliteDriverStatement,
  SqliteObjectRow,
  SqliteParameterBinding,
  StreamQueryOptions,
  UpdateListener
} from '../driver-api.js';

import {
  ErrorStatement,
  mapError,
  ReadWriteConnectionPool
} from '../util/index.js';
import { loadNodeSqlite } from './node-sqlite.js';

export function nodeSqlitePool(path: string): SqliteDriverConnectionPool {
  return new ReadWriteConnectionPool({
    async openConnection(options) {
      const sqlite = await loadNodeSqlite();
      const db = new sqlite.DatabaseSync(path);
      return new NodeSqliteConnection(db, {
        readonly: options?.readonly,
        name: options?.connectionName
      });
    }
  });
}

interface InternalStatement extends SqliteDriverStatement {
  readonly source: string;

  readonly persisted: boolean;
}

class NodeSqliteSyncStatement implements InternalStatement {
  public statement: sqlite.StatementSync;

  readonly persisted: boolean;

  [Symbol.dispose]: () => void = undefined as any;

  constructor(statement: sqlite.StatementSync, options: PrepareOptions) {
    this.statement = statement;
    this.persisted = options.autoFinalize ?? false;

    if (typeof Symbol.dispose != 'undefined') {
      this[Symbol.dispose] = () => this.finalize();
    }
  }

  get source() {
    return this.statement.sourceSQL();
  }

  async getColumns(): Promise<string[]> {
    // Not supported
    return [];
  }

  async run(
    parameters: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteChanges> {
    try {
      if (options?.requireTransaction) {
        // TODO: Implement
      }

      const statement = this.statement;
      statement.setReadBigInts(true);
      const r = statement.run(...convertParameters(parameters));
      return {
        changes: Number(r.changes),
        lastInsertRowId: r.lastInsertRowid as bigint
      };
    } catch (e) {
      throw mapError(e);
    }
  }

  async all(
    parameters: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteObjectRow[]> {
    try {
      if (options?.requireTransaction) {
        // TODO: Implement
      }

      const statement = this.statement;
      statement.setReadBigInts(options?.bigint ?? false);
      const rows = statement.all(...convertParameters(parameters));
      return rows;
    } catch (e) {
      throw mapError(e);
    }
  }

  allArray(
    parameters: SqliteParameterBinding,
    options: QueryOptions
  ): Promise<SqliteArrayRow[]> {
    throw new Error('array rows are not supported');
  }

  async *stream(
    parameters: SqliteParameterBinding,
    options: StreamQueryOptions
  ): AsyncIterableIterator<SqliteObjectRow[]> {
    const rows = await this.all(parameters, options);
    yield rows;
  }

  streamArray(
    parameters: SqliteParameterBinding,
    options: StreamQueryOptions
  ): AsyncIterableIterator<SqliteArrayRow[]> {
    throw new Error('array rows are not supported');
  }

  finalize(): void {
    // We don't use any iterators internally - nothing to cancel here
  }
}

export class NodeSqliteConnection implements SqliteDriverConnection {
  con: sqlite.DatabaseSync;
  name: string;

  changeStatement: sqlite.StatementSync;

  constructor(
    db: sqlite.DatabaseSync,
    options?: { readonly?: boolean; name?: string }
  ) {
    this.con = db;
    this.con.exec('PRAGMA journal_mode = WAL');
    this.con.exec('PRAGMA synchronous = normal');
    this.con.exec('PRAGMA busy_timeout = 5000');
    if (options?.readonly) {
      this.con.exec('PRAGMA query_only = true');
    }
    this.name = options?.name ?? '';
    this.changeStatement = this.con.prepare(
      'select last_insert_rowid() as l, changes() as c'
    );
    this.changeStatement.setReadBigInts(true);
  }

  async getLastChanges(): Promise<SqliteChanges> {
    const r = this.changeStatement.get() as any;
    return {
      lastInsertRowId: r!.l,
      changes: Number(r!.c)
    };
  }

  async close() {
    this.con.close();
  }

  prepare(sql: string, options?: PrepareOptions): InternalStatement {
    try {
      const statement = this.con.prepare(sql);
      return new NodeSqliteSyncStatement(statement, options ?? {});
    } catch (e) {
      return new ErrorStatement(sql, mapError(e), options ?? {});
    }
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    throw new Error('not supported yet');
  }
}

function convertParameters(parameters: SqliteParameterBinding): any[] {
  if (parameters == null) {
    return [];
  } else if (Array.isArray(parameters)) {
    return parameters;
  } else {
    return [parameters];
  }
}
