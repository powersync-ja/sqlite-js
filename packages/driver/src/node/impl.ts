import type * as sqlite from './node-sqlite.js';

import {
  PrepareOptions,
  ResetOptions,
  SqliteChanges,
  SqliteDriverConnection,
  SqliteDriverConnectionPool,
  SqliteDriverStatement,
  SqliteParameterBinding,
  SqliteRow,
  SqliteStepResult,
  SqliteValue,
  StepOptions,
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
  private options: PrepareOptions;
  private bindPositional: SqliteValue[] = [];
  private bindNamed: Record<string, SqliteValue> = {};
  private statementDone = false;
  private iterator: Iterator<unknown> | undefined = undefined;

  readonly persisted: boolean;

  [Symbol.dispose]: () => void = undefined as any;

  constructor(statement: sqlite.StatementSync, options: PrepareOptions) {
    this.statement = statement;
    this.options = options;
    this.persisted = options.persist ?? false;

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

  bind(parameters: SqliteParameterBinding): void {
    if (parameters == null) {
      return;
    }
    if (Array.isArray(parameters)) {
      let bindArray = this.bindPositional;

      for (let i = 0; i < parameters.length; i++) {
        if (typeof parameters[i] != 'undefined') {
          bindArray[i] = parameters[i]!;
        }
      }
    } else {
      let previous = this.bindNamed;
      this.bindNamed = { ...previous, ...parameters };
    }
  }

  async run(options?: StepOptions): Promise<SqliteChanges> {
    try {
      if (options?.requireTransaction) {
        // TODO: Implement
      }

      const statement = this.statement;
      this.reset();

      try {
        const bindNamed = this.bindNamed;
        const bindPositional = this.bindPositional;

        statement.setReadBigInts(true);
        const r = statement.run(bindNamed, ...bindPositional);
        return {
          changes: Number(r.changes),
          lastInsertRowId: r.lastInsertRowid as bigint
        };
      } finally {
        this.reset();
      }
    } catch (e) {
      throw mapError(e);
    }
  }

  async step(n?: number, options?: StepOptions): Promise<SqliteStepResult> {
    try {
      const all = n == null;

      const statement = this.statement;
      if (this.statementDone) {
        return { done: true };
      }

      if (options?.requireTransaction) {
        // TODO: implement
      }

      const bindNamed = this.bindNamed;
      const bindPositional = this.bindPositional;

      let iterator = this.iterator;
      const num_rows = n ?? 1;
      if (iterator == null) {
        if (this.options.rawResults) {
          // Not supported
        }
        if (this.options.bigint) {
          statement.setReadBigInts(true);
        }
        iterator = statement
          .all(bindNamed, ...bindPositional)
          [Symbol.iterator]();
        this.iterator = iterator;
      }
      let rows: SqliteRow[] = [];
      let isDone = false;
      for (let i = 0; i < num_rows || all; i++) {
        const { value, done } = iterator.next();
        if (done) {
          isDone = true;
          break;
        }
        rows.push(value as SqliteRow);
      }
      if (isDone) {
        this.statementDone = true;
      }
      return { rows, done: isDone };
    } catch (e) {
      throw mapError(e);
    }
  }

  finalize(): void {
    const existingIter = this.iterator;
    if (existingIter != null) {
      existingIter.return?.();
    }
    this.iterator = undefined;
    this.statementDone = false;
  }

  reset(options?: ResetOptions): void {
    if (this.iterator) {
      const iter = this.iterator;
      iter.return?.();
      this.iterator = undefined;
    }
    if (options?.clearBindings) {
      this.bindNamed = {};
      this.bindPositional = [];
    }
    this.statementDone = false;
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
