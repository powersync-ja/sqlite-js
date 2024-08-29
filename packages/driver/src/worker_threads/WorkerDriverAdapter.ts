import {
  SqliteChanges,
  SqliteDriverConnection,
  SqliteDriverStatement,
  UpdateListener
} from '../driver-api.js';
import { mapError } from '../util/errors.js';
import {
  InferBatchResult,
  SqliteCommand,
  SqliteCommandResponse,
  SqliteCommandType,
  SqliteFinalize,
  SqliteParse,
  SqliteParseResult,
  SqlitePrepare,
  SqliteQuery,
  SqliteQueryResult,
  SqliteRun,
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
      autoFinalize: command.autoFinalize
    });
    this.statements.set(id, statement);
  }

  private async _parse(command: SqliteParse): Promise<SqliteParseResult> {
    const { id } = command;
    const statement = this.requireStatement(id);
    return { columns: await statement.getColumns() };
  }

  private _run(command: SqliteRun): Promise<SqliteChanges> {
    const { id } = command;
    const statement = this.requireStatement(id);
    return statement.run(command.parameters, command.options);
  }

  private async _query(command: SqliteQuery): Promise<SqliteQueryResult> {
    const { id } = command;
    const statement = this.requireStatement(id);
    if (command.array) {
      const results = await statement.allArray(
        command.parameters,
        command.options
      );
      return { rows: results };
    } else {
      const results = await statement.all(command.parameters, command.options);
      return { rows: results };
    }
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
      case SqliteCommandType.query:
        return this._query(command);
      case SqliteCommandType.run:
        return this._run(command);
      case SqliteCommandType.finalize:
        return this._finalize(command);
      case SqliteCommandType.parse:
        return this._parse(command);
      default:
        throw new Error(`Unknown command: ${(command as SqliteCommand).type}`);
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
