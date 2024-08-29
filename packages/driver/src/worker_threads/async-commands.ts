import {
  SqliteParameterBinding,
  SqliteChanges,
  QueryOptions,
  StreamQueryOptions,
  SqliteArrayRow,
  SqliteObjectRow
} from '../driver-api.js';
import { SerializedDriverError } from '../sqlite-error.js';

export const enum SqliteCommandType {
  prepare = 1,
  finalize = 5,
  parse = 7,
  run = 8,
  query = 9
}

export type SqliteDriverError = SerializedDriverError;

export type SqliteCommandResponse = SqliteErrorResponse | SqliteValueResponse;

export interface SqliteErrorResponse {
  error: SqliteDriverError;
}

export interface SqliteValueResponse<T = unknown> {
  value: T;
}

export interface SqliteBaseCommand {
  type: SqliteCommandType;
}

export interface SqlitePrepare extends SqliteBaseCommand {
  type: SqliteCommandType.prepare;
  id: number;
  sql: string;
  autoFinalize?: boolean;
}

export interface SqliteParseResult {
  columns: string[];
}

export interface SqliteParse extends SqliteBaseCommand {
  type: SqliteCommandType.parse;
  id: number;
}

export interface SqliteRun extends SqliteBaseCommand {
  type: SqliteCommandType.run;
  id: number;
  parameters?: SqliteParameterBinding;
  options?: QueryOptions;
}

export interface SqliteQueryResult {
  rows: SqliteArrayRow[] | SqliteObjectRow[];
}

export interface SqliteQuery extends SqliteBaseCommand {
  type: SqliteCommandType.query;
  id: number;
  parameters?: SqliteParameterBinding;
  options?: StreamQueryOptions;
  array?: boolean;
}

export interface SqliteFinalize extends SqliteBaseCommand {
  type: SqliteCommandType.finalize;
  id: number;
}

export type SqliteCommand =
  | SqlitePrepare
  | SqliteRun
  | SqliteFinalize
  | SqliteQuery
  | SqliteParse;

export type InferCommandResult<T extends SqliteCommand> = T extends SqliteRun
  ? SqliteChanges
  : T extends SqliteQuery
    ? SqliteQueryResult
    : T extends SqliteParse
      ? SqliteParseResult
      : void;

export type InferBatchResult<T extends SqliteCommand[]> = {
  [i in keyof T]:
    | SqliteErrorResponse
    | SqliteValueResponse<InferCommandResult<T[i]>>;
};

export function isErrorResponse(
  response: SqliteCommandResponse
): response is SqliteErrorResponse {
  return (response as SqliteErrorResponse).error != null;
}

export interface WorkerDriver {
  execute(commands: SqliteCommand[]): Promise<SqliteCommandResponse[]>;
  close(): Promise<void>;
}
