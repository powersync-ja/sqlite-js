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
