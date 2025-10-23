import * as SQLite from '@journeyapps/wa-sqlite';
import SQLiteESMFactory from '@journeyapps/wa-sqlite/dist/wa-sqlite-async.mjs';
import {
  PrepareOptions,
  ResetOptions,
  SqliteChanges,
  SqliteDriverConnection,
  SqliteDriverStatement,
  SqliteError,
  SqliteParameterBinding,
  SqliteRow,
  SqliteStepResult,
  StepOptions,
  UpdateListener
} from '@sqlite-js/driver';
import * as mutex from 'async-mutex';
import { OPFSCoopSyncVFS2 } from './OPFSCoopSyncVFS2';

// Initialize SQLite.
export const module = await SQLiteESMFactory();
export const sqlite3 = SQLite.Factory(module);

const m = new mutex.Mutex();

class StatementImpl implements SqliteDriverStatement {
  private preparePromise: Promise<{ error: SqliteError | null }>;
  private bindPromise?: Promise<{ error: SqliteError | null }>;
  private columns: string[] = [];

  private statementRef?: number;
  private done = false;

  constructor(
    private db: number,
    private con: WaSqliteConnection,
    public source: string,
    public options: PrepareOptions
  ) {
    this.preparePromise = this.prepare();
  }

  async prepare() {
    return await m.runExclusive(() => this._prepare());
  }

  private async getStatement() {
    const statementsIter = sqlite3.statements(this.db, this.source, {
      unscoped: true
    });
    for await (let statement of statementsIter) {
      return statement;
    }
    throw new Error(`No SQL statements in: ${this.source}`);
  }

  async _prepare() {
    try {
      const statement = await this.getStatement();
      this.statementRef = statement;
      this.columns = sqlite3.column_names(statement);
      return { error: null };
    } catch (e: any) {
      return {
        error: new SqliteError({
          code: 'SQLITE_ERROR',
          message: e.message
        })
      };
    }
  }

  private async _waitForPrepare() {
    const { error } = await (this.bindPromise ?? this.preparePromise);
    if (error) {
      throw error;
    }
  }

  async getColumns(): Promise<string[]> {
    await this._waitForPrepare();
    return sqlite3.column_names(this.statementRef!);
  }

  bind(parameters: SqliteParameterBinding): void {
    this.bindPromise = this.preparePromise.then(async (result) => {
      if (result.error) {
        return result;
      }
      await m.runExclusive(() => this.bindImpl(parameters));
      return { error: null };
    });
  }

  bindImpl(parameters: SqliteParameterBinding): void {
    if (Array.isArray(parameters)) {
      const count = sqlite3.bind_parameter_count(this.statementRef!);
      let pi = 0;
      for (let i = 0; i < count; i++) {
        const name = sqlite3.bind_parameter_name(this.statementRef!, i + 1);
        if (name == '') {
          const value = parameters[pi];
          pi++;
          if (typeof value != 'undefined') {
            sqlite3.bind(this.statementRef!, i + 1, value);
          }
        }
      }

      for (let i = 0; i < parameters.length; i++) {
        const value = parameters[i];
        if (typeof value !== 'undefined') {
          sqlite3.bind(this.statementRef!, i + 1, value);
        }
      }
    } else if (parameters != null) {
      const count = sqlite3.bind_parameter_count(this.statementRef!);
      for (let i = 0; i < count; i++) {
        const name = sqlite3.bind_parameter_name(this.statementRef!, i + 1);
        if (name != '') {
          if (name in parameters) {
            const value = parameters[name];
            sqlite3.bind(this.statementRef!, i + 1, value);
          } else if (name.substring(1) in parameters) {
            // Removes the prefix of ? : @ $
            const value = parameters[name.substring(1)];
            sqlite3.bind(this.statementRef!, i + 1, value);
          }
        }
      }
    }
  }

  async step(n?: number, options?: StepOptions): Promise<SqliteStepResult> {
    await this._waitForPrepare();

    return await m.runExclusive(() => this._step(n, options));
  }

  async _step(n?: number, options?: StepOptions): Promise<SqliteStepResult> {
    try {
      if (this.done) {
        return { done: true };
      }

      const stmt = this.statementRef!;

      let rows: SqliteRow[] = [];

      const mapValue = (value: any) => {
        if (typeof value == 'number') {
          return this.options.bigint ? BigInt(value) : value;
        } else if (typeof value == 'bigint') {
          return this.options.bigint ? value : Number(value);
        } else {
          return value;
        }
      };
      const mapRow = this.options.rawResults
        ? (row: any) => row.map(mapValue)
        : (row: any[]) => {
            return Object.fromEntries(
              this.columns.map((c, i) => [c, mapValue(row[i])])
            );
          };
      if (n == null) {
        while ((await sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {
          const row = sqlite3.row(stmt);
          rows.push(mapRow(row));
        }
        this.done = true;
        return { rows: rows, done: true };
      } else {
        while (
          rows.length < n &&
          (await sqlite3.step(stmt)) === SQLite.SQLITE_ROW
        ) {
          const row = sqlite3.row(stmt);
          rows.push(mapRow(row));
        }
        const done = rows.length < n;
        this.done = done;
        return { rows: rows, done: done };
      }
    } catch (e: any) {
      throw new SqliteError({
        code: 'SQLITE_ERROR',
        message: e.message
      });
    }
  }

  async _finalize() {
    console.log('finalizing...', this.con.path);
    // Wait for these to complete, but ignore any errors.
    // TODO: also wait for run/step to complete
    await this.preparePromise;
    await this.bindPromise;

    if (this.statementRef) {
      sqlite3.finalize(this.statementRef);
      this.statementRef = undefined;
    }
  }

  finalize(): void {
    m.runExclusive(() => this._finalize());
  }

  reset(options?: ResetOptions): void {
    this.preparePromise.finally(() => {
      this.done = false;
      sqlite3.reset(this.statementRef!);

      if (options?.clearBindings) {
        // No native clear_bidings?
        const count = sqlite3.bind_parameter_count(this.statementRef!);
        for (let i = 0; i < count; i++) {
          sqlite3.bind_null(this.statementRef!, i + 1);
        }
      }
    });
  }

  async run(options?: StepOptions): Promise<SqliteChanges> {
    return await m.runExclusive(() => this._run(options));
  }

  async _run(options?: StepOptions): Promise<SqliteChanges> {
    await this.preparePromise;

    try {
      this.reset();
      const stmt = this.statementRef!;
      while ((await sqlite3.step(stmt)) === SQLite.SQLITE_ROW) {}

      const changes = sqlite3.changes(this.db);
      const lastInsertRowId = BigInt(sqlite3.last_insert_id(this.db));

      return { changes, lastInsertRowId };
    } catch (e: any) {
      throw new SqliteError({
        code: 'SQLITE_ERROR',
        message: e.message
      });
    } finally {
      this.reset();
    }
  }

  [Symbol.dispose](): void {
    this.finalize();
  }
}

export class WaSqliteConnection implements SqliteDriverConnection {
  db: number;
  vfs: OPFSCoopSyncVFS2;

  statements = new Set<StatementImpl>();
  lockDisposer: Disposable | null = null;

  static async open(
    filename: string,
    vfs: OPFSCoopSyncVFS2
  ): Promise<WaSqliteConnection> {
    // Open the database.
    const db = await sqlite3.open_v2(filename);
    return new WaSqliteConnection(db, vfs, filename);
  }

  constructor(
    db: number,
    vfs: OPFSCoopSyncVFS2,
    public path: string
  ) {
    this.db = db;
    this.vfs = vfs;
  }

  async lock() {
    this.lockDisposer = await this.vfs.prelock(this.path);
  }

  release() {
    this.lockDisposer[Symbol.dispose]();
    this.lockDisposer = null;
  }

  async close() {
    await m.runExclusive(async () => {
      console.log('closing...', this.path);
      for (let statement of this.statements) {
        if (statement.options.persist) {
          statement.finalize();
        }
      }

      await new Promise((resolve) => setTimeout(resolve, 100));
      await sqlite3.close(this.db);
    });
  }

  async getLastChanges(): Promise<SqliteChanges> {
    const changes = sqlite3.changes(this.db);
    const lastInsertRowId = BigInt(sqlite3.last_insert_id(this.db));

    return { changes, lastInsertRowId };
  }

  prepare(sql: string, options?: PrepareOptions): StatementImpl {
    const st = new StatementImpl(this.db, this, sql, options ?? {});
    // TODO: cleanup on finalize
    this.statements.add(st);
    return st;
  }

  dispose(): void {
    // No-op
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    throw new Error('not implemented');
  }
}
