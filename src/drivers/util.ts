import { SqliteDriverError } from '../driver-api.js';

export function mapError(error: unknown): SqliteDriverError {
  const e = error as any;
  return {
    code: e.code ?? 'SQLITE_ERROR',
    message: e.message!,
    stack: e.stack
  };
}

export class SqliteError extends Error {
  code: string;
  cause: SqliteDriverError;

  constructor(cause: SqliteDriverError) {
    super(cause.message);
    this.code = cause.code;
    this.cause = cause;

    if (cause.stack) {
      this.stack = cause.stack;
    }
  }
}
