import { SqliteDriverError } from '../driver-api.js';

export function mapError(error: unknown): SqliteError {
  const e = error as any;
  let code = e.code ?? 'SQLITE_ERROR';
  if (code == 'ERR_SQLITE_ERROR') {
    code = 'SQLITE_ERROR';
  }
  return new SqliteError({
    code,
    message: e.message!,
    stack: e.stack
  });
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
