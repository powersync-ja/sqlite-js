import {
  SqliteChanges,
  SqliteDriverConnection,
  SqliteDriverStatement,
  SqliteStepResult,
  UpdateListener
} from '../driver-api.js';
import { mapError } from '../util/errors.js';
import {
  InferBatchResult,
  SqliteBind,
  SqliteCommand,
  SqliteCommandResponse,
  SqliteCommandType,
  SqliteFinalize,
  SqliteParse,
  SqliteParseResult,
  SqlitePrepare,
  SqliteReset,
  SqliteRun,
  SqliteStep,
  WorkerDriver
} from './async-commands.js';

export class WorkerConnectionAdapter implements WorkerDriver {
  constructor(public connnection: SqliteDriverConnection) {}

  statements = new Map<number, SqliteDriverStatement>();

  async close() {
    await this.connnection.close();
  }

  private requireStatement(id: number) {
    const statement = this.statements.get(id);
    if (statement == null) {
      throw new Error(`statement not found: ${id}`);
    }
    return statement;
  }

  private _prepare(command: SqlitePrepare): void {
    const { id, sql } = command;

    const existing = this.statements.get(id);
    if (existing != null) {
      throw new Error(
        `Replacing statement ${id} without finalizing the previous one`
      );
    }

    const statement = this.connnection.prepare(sql, {
      bigint: command.bigint,
      persist: command.persist,
      rawResults: command.rawResults
    });
    this.statements.set(id, statement);
  }

  private async _parse(command: SqliteParse): Promise<SqliteParseResult> {
    const { id } = command;
    const statement = this.requireStatement(id);
    return { columns: await statement.getColumns() };
  }

  private _bind(command: SqliteBind): void {
    const { id, parameters } = command;
    const statement = this.requireStatement(id);
    statement.bind(parameters);
  }

  private _step(command: SqliteStep): Promise<SqliteStepResult> {
    const { id, n, requireTransaction } = command;
    const statement = this.requireStatement(id);
    return statement.step(n, { requireTransaction });
  }

  private _run(command: SqliteRun): Promise<SqliteChanges> {
    const { id } = command;
    const statement = this.requireStatement(id);
    return statement.run(command);
  }

  private _reset(command: SqliteReset): void {
    const { id } = command;
    const statement = this.requireStatement(id);
    statement.reset(command);
  }

  private _finalize(command: SqliteFinalize): void {
    const { id } = command;
    const statement = this.requireStatement(id);
    statement.finalize();
    this.statements.delete(id);
  }

  private async _executeCommand(command: SqliteCommand): Promise<any> {
    switch (command.type) {
      case SqliteCommandType.prepare:
        return this._prepare(command);
      case SqliteCommandType.bind:
        return this._bind(command);
      case SqliteCommandType.step:
        return this._step(command);
      case SqliteCommandType.run:
        return this._run(command);
      case SqliteCommandType.reset:
        return this._reset(command);
      case SqliteCommandType.finalize:
        return this._finalize(command);
      case SqliteCommandType.parse:
        return this._parse(command);
      case SqliteCommandType.changes:
        return this.connnection.getLastChanges();
      default:
        throw new Error(`Unknown command: ${command.type}`);
    }
  }

  async execute<const T extends SqliteCommand[]>(
    commands: T
  ): Promise<InferBatchResult<T>> {
    let results: SqliteCommandResponse[] = [];

    for (let command of commands) {
      try {
        const result = await this._executeCommand(command);
        results.push({ value: result });
      } catch (e: any) {
        const err = mapError(e);
        results.push({
          error: { message: err.message, stack: err.stack, code: err.code }
        });
      }
    }
    return results as InferBatchResult<T>;
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
  ): () => void {
    throw new Error('Not implemented yet');
  }
}
