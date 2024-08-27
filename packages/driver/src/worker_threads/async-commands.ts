import {
  SqliteParameterBinding,
  SqliteChanges,
  SqliteStepResult
} from '../driver-api.js';
import { SerializedDriverError } from '../sqlite-error.js';

export enum SqliteCommandType {
  prepare = 1,
  bind = 2,
  step = 3,
  reset = 4,
  finalize = 5,
  sync = 6,
  parse = 7,
  run = 8,
  changes = 9
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
  bigint?: boolean;
  persist?: boolean;
  rawResults?: boolean;
}

export interface SqliteParseResult {
  columns: string[];
}

export interface SqliteBind extends SqliteBaseCommand {
  type: SqliteCommandType.bind;
  id: number;
  parameters: SqliteParameterBinding;
}

export interface SqliteParse extends SqliteBaseCommand {
  type: SqliteCommandType.parse;
  id: number;
}

export interface SqliteStep extends SqliteBaseCommand {
  type: SqliteCommandType.step;
  id: number;
  n?: number;
  requireTransaction?: boolean;
}

export interface SqliteRun extends SqliteBaseCommand {
  type: SqliteCommandType.run;
  id: number;
  requireTransaction?: boolean;
}

export interface SqliteReset extends SqliteBaseCommand {
  type: SqliteCommandType.reset;
  id: number;
  clearBindings?: boolean;
}

export interface SqliteFinalize extends SqliteBaseCommand {
  type: SqliteCommandType.finalize;
  id: number;
}

export interface SqliteSync {
  type: SqliteCommandType.sync;
}

export interface SqliteGetChanges {
  type: SqliteCommandType.changes;
}

export type SqliteCommand =
  | SqlitePrepare
  | SqliteBind
  | SqliteStep
  | SqliteRun
  | SqliteReset
  | SqliteFinalize
  | SqliteSync
  | SqliteParse
  | SqliteGetChanges;

export type InferCommandResult<T extends SqliteCommand> = T extends SqliteRun
  ? SqliteChanges
  : T extends SqliteStep
    ? SqliteStepResult
    : T extends SqliteParse
      ? SqliteParseResult
      : T extends SqliteGetChanges
        ? SqliteChanges
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
