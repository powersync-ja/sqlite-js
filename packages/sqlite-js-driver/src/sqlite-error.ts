export interface SerializedDriverError {
  message: string;
  code: string;
  stack?: string;
}

export class SqliteError extends Error {
  code: string;
  cause: SerializedDriverError;

  constructor(cause: SerializedDriverError) {
    super(cause.message);
    this.code = cause.code;
    this.cause = cause;

    if (cause.stack) {
      this.stack = cause.stack;
    }
  }
}

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
