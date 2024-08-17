import { SqliteError } from '../sqlite-error.js';

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
