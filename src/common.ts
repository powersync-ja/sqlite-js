export type SqliteValue = null | string | number | bigint | Uint8Array;
export type SqliteArguments =
  | SqliteValue[]
  | Record<string, SqliteValue>
  | null;
