import {
  PrepareOptions,
  QueryOptions,
  SqliteArrayRow,
  SqliteChanges,
  SqliteDriverStatement,
  SqliteObjectRow,
  SqliteParameterBinding,
  StreamQueryOptions
} from '../driver-api.js';
import { SqliteDriverError } from '../worker_threads/async-commands.js';

/**
 * Represents a statement that failed on prepare.
 *
 * Since the error can only be surfaced at step(), run() or getColumns(),
 * this statement is just holds on to the error until one of those are called.
 */
export class ErrorStatement implements SqliteDriverStatement {
  readonly error: SqliteDriverError;
  readonly source: string;
  readonly persisted: boolean;

  constructor(
    source: string,
    error: SqliteDriverError,
    options: PrepareOptions
  ) {
    this.error = error;
    this.source = source;
    this.persisted = options.autoFinalize ?? false;
  }

  async all(
    parameters: SqliteParameterBinding,
    options: QueryOptions
  ): Promise<SqliteObjectRow[]> {
    throw this.error;
  }
  async allArray(
    parameters: SqliteParameterBinding,
    options: QueryOptions
  ): Promise<SqliteArrayRow[]> {
    throw this.error;
  }
  async *stream(
    parameters: SqliteParameterBinding,
    options: StreamQueryOptions
  ): AsyncIterator<SqliteObjectRow[]> {
    throw this.error;
  }
  async *streamArray(
    parameters: SqliteParameterBinding,
    options: StreamQueryOptions
  ): AsyncIterator<SqliteArrayRow[]> {
    throw this.error;
  }

  async getColumns(): Promise<string[]> {
    throw this.error;
  }
  async run(
    parameters: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteChanges> {
    throw this.error;
  }

  finalize(): void {
    // no-op
  }

  [Symbol.dispose](): void {
    // no-op
  }
}
