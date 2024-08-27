import {
  PrepareOptions,
  ResetOptions,
  SqliteChanges,
  SqliteDriverStatement,
  SqliteParameterBinding,
  SqliteStepResult,
  StepOptions
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
    this.persisted = options.persist ?? false;
  }

  async getColumns(): Promise<string[]> {
    throw this.error;
  }
  bind(parameters: SqliteParameterBinding): void {
    // no-op
  }
  async step(n?: number, options?: StepOptions): Promise<SqliteStepResult> {
    throw this.error;
  }

  async run(options?: StepOptions): Promise<SqliteChanges> {
    throw this.error;
  }

  finalize(): void {
    // no-op
  }

  reset(options?: ResetOptions): void {
    // no-op
  }

  [Symbol.dispose](): void {
    // no-op
  }
}
