import * as SQLite from 'wa-sqlite';
import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite-async.mjs';
import {
  PrepareOptions,
  QueryOptions,
  SqliteArrayRow,
  SqliteChanges,
  SqliteDriverConnection,
  SqliteDriverStatement,
  SqliteObjectRow,
  SqliteParameterBinding,
  StreamQueryOptions,
  UpdateListener
} from '@sqlite-js/driver';
import { SqliteError } from '@sqlite-js/driver';
import * as mutex from 'async-mutex';

export const module = await SQLiteESMFactory();
export const sqlite3 = SQLite.Factory(module);

const globalMutex = new mutex.Mutex();

async function withMutex<T>(fn: () => Promise<T> | T): Promise<T> {
  return globalMutex.runExclusive(fn);
}

function toSqliteError(error: any): SqliteError {
  return new SqliteError({
    code: 'SQLITE_ERROR',
    message: error?.message ?? String(error)
  });
}

class StatementImpl implements SqliteDriverStatement {
  private statementRef?: number;
  private columns: string[] = [];
  private finalized = false;
  private prepared = false;

  readonly persisted: boolean;

  constructor(
    private db: number,
    private connection: WaSqliteConnection,
    public source: string,
    options: PrepareOptions
  ) {
    this.persisted = options.autoFinalize ?? false;
  }

  private async prepareStatementIfNeeded(): Promise<void> {
    if (this.prepared || this.finalized) {
      if (this.statementRef == null) {
        throw new SqliteError({
          code: 'SQLITE_ERROR',
          message: 'Statement has been finalized'
        });
      }
      return;
    }
    const statementsIter = sqlite3.statements(this.db, this.source, {
      unscoped: true
    });
    try {
      for await (let statement of statementsIter) {
        this.statementRef = statement;
        this.columns = sqlite3.column_names(statement);
        this.prepared = true;
        return;
      }
    } catch (error) {
      throw toSqliteError(error);
    }
    throw new SqliteError({
      code: 'SQLITE_ERROR',
      message: `No SQL statements in: ${this.source}`
    });
  }

  private resetStatement(): void {
    if (this.statementRef == null) {
      return;
    }
    sqlite3.reset(this.statementRef);
  }

  private clearBindings(): void {
    if (this.statementRef == null) {
      return;
    }
    const count = sqlite3.bind_parameter_count(this.statementRef);
    for (let i = 0; i < count; i++) {
      sqlite3.bind_null(this.statementRef, i + 1);
    }
  }

  private bindParameters(parameters: SqliteParameterBinding | undefined): void {
    if (this.statementRef == null || parameters == null) {
      return;
    }

    if (Array.isArray(parameters)) {
      const count = sqlite3.bind_parameter_count(this.statementRef);
      // Bind any named parameters that correspond to positional indices
      for (let i = 0; i < count; i++) {
        const name = sqlite3.bind_parameter_name(this.statementRef, i + 1);
        if (name === '') {
          const value = parameters[i];
          if (typeof value !== 'undefined') {
            sqlite3.bind(this.statementRef, i + 1, value);
          }
        }
      }
      for (let i = 0; i < parameters.length; i++) {
        const value = parameters[i];
        if (typeof value !== 'undefined') {
          sqlite3.bind(this.statementRef, i + 1, value);
        }
      }
    } else {
      const count = sqlite3.bind_parameter_count(this.statementRef);
      for (let i = 0; i < count; i++) {
        const name = sqlite3.bind_parameter_name(this.statementRef, i + 1);
        if (name === '') {
          continue;
        }
        let key = name;
        if (!(key in parameters) && name.length > 1) {
          key = name.substring(1);
        }
        const value = (parameters as Record<string, any>)[key];
        if (typeof value !== 'undefined') {
          sqlite3.bind(this.statementRef, i + 1, value);
        }
      }
    }
  }

  private mapValue(
    value: unknown,
    options?: QueryOptions | StreamQueryOptions
  ): unknown {
    const useBigint = options?.bigint ?? false;
    if (typeof value === 'number') {
      if (useBigint && Number.isInteger(value)) {
        return BigInt(value);
      }
      return value;
    }
    if (typeof value === 'bigint' && !useBigint) {
      const num = Number(value);
      // if (!Number.isSafeInteger(num)) {
      //   return value;
      // }
      return num;
    }
    return value;
  }

  private mapRow(
    row: any[],
    options: QueryOptions | StreamQueryOptions | undefined,
    asArray: boolean
  ): SqliteObjectRow | SqliteArrayRow {
    if (asArray) {
      return row.map((value) => this.mapValue(value, options)) as SqliteArrayRow;
    }
    const entries = this.columns.map((column, index) => [
      column,
      this.mapValue(row[index], options)
    ]);
    return Object.fromEntries(entries) as SqliteObjectRow;
  }

  async all(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteObjectRow[]> {
    return withMutex(async () => {
      try {
        await this.prepareStatementIfNeeded();
        const stmt = this.statementRef!;
        this.resetStatement();
        this.clearBindings();
        this.bindParameters(parameters);
        const rows: SqliteObjectRow[] = [];
        while ((await sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
          const row = sqlite3.row(stmt);
          rows.push(this.mapRow(row, options, false) as SqliteObjectRow);
        }
        return rows;
      } catch (error) {
        throw toSqliteError(error);
      } finally {
        this.resetStatement();
      }
    });
  }

  async allArray(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteArrayRow[]> {
    return withMutex(async () => {
      try {
        await this.prepareStatementIfNeeded();
        const stmt = this.statementRef!;
        this.resetStatement();
        this.clearBindings();
        this.bindParameters(parameters);
        const rows: SqliteArrayRow[] = [];
        while ((await sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
          const row = sqlite3.row(stmt);
          rows.push(this.mapRow(row, options, true) as SqliteArrayRow);
        }
        return rows;
      } catch (error) {
        throw toSqliteError(error);
      } finally {
        this.resetStatement();
      }
    });
  }

  async *stream(
    parameters?: SqliteParameterBinding,
    options?: StreamQueryOptions
  ): AsyncIterableIterator<SqliteObjectRow[]> {
    const rows = await this.all(parameters, options);
    if (rows.length === 0) {
      return;
    }
    const chunkSize = options?.chunkMaxRows ?? rows.length;
    const effectiveChunk = chunkSize > 0 ? chunkSize : rows.length;
    for (let i = 0; i < rows.length; i += effectiveChunk) {
      yield rows.slice(i, i + effectiveChunk);
    }
  }

  async *streamArray(
    parameters?: SqliteParameterBinding,
    options?: StreamQueryOptions
  ): AsyncIterableIterator<SqliteArrayRow[]> {
    const rows = await this.allArray(parameters, options);
    if (rows.length === 0) {
      return;
    }
    const chunkSize = options?.chunkMaxRows ?? rows.length;
    const effectiveChunk = chunkSize > 0 ? chunkSize : rows.length;
    for (let i = 0; i < rows.length; i += effectiveChunk) {
      yield rows.slice(i, i + effectiveChunk);
    }
  }

  async getColumns(): Promise<string[]> {
    return withMutex(async () => {
      await this.prepareStatementIfNeeded();
      return this.columns;
    });
  }

  async run(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteChanges> {
    return withMutex(async () => {
      try {
        await this.prepareStatementIfNeeded();
        const stmt = this.statementRef!;
        this.resetStatement();
        this.clearBindings();
        this.bindParameters(parameters);
        while ((await sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
          // Exhaust results
        }
        const changes = sqlite3.changes(this.db);
        const lastInsertRowId = 0n;
        return { changes, lastInsertRowId };
      } catch (error) {
        throw toSqliteError(error);
      } finally {
        this.resetStatement();
      }
    });
  }

  finalize(): void {
    void withMutex(() => {
      this.finalizeInternal();
    });
  }

  finalizeForClose(): void {
    this.finalizeInternal();
  }

  private finalizeInternal() {
    if (this.finalized) {
      return;
    }
    this.finalized = true;
    if (this.statementRef != null) {
      sqlite3.finalize(this.statementRef);
      this.statementRef = undefined;
    }
    this.connection.unregisterStatement(this);
  }

  [Symbol.dispose](): void {
    this.finalize();
  }
}

export class WaSqliteConnection implements SqliteDriverConnection {
  private statements = new Set<StatementImpl>();

  constructor(
    private db: number,
    public path: string
  ) {}

  static async open(filename: string): Promise<WaSqliteConnection> {
    const db = await sqlite3.open_v2(filename);
    return new WaSqliteConnection(db, filename);
  }

  registerStatement(statement: StatementImpl) {
    this.statements.add(statement);
  }

  unregisterStatement(statement: StatementImpl) {
    this.statements.delete(statement);
  }

  async close() {
    await withMutex(async () => {
      for (let statement of Array.from(this.statements)) {
        statement.finalizeForClose();
      }
      this.statements.clear();
      await new Promise((resolve) => setTimeout(resolve, 100));
      await sqlite3.close(this.db);
    });
  }

  prepare(sql: string, options?: PrepareOptions): StatementImpl {
    const statement = new StatementImpl(this.db, this, sql, options ?? {});
    this.registerStatement(statement);
    return statement;
  }

  onUpdate(
    _listener: UpdateListener,
    _options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    throw new Error('not implemented');
  }
}
