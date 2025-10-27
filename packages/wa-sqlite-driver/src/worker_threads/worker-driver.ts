import {
  PrepareOptions,
  QueryOptions,
  SqliteArrayRow,
  SqliteChanges,
  SqliteDriverConnection,
  SqliteDriverStatement,
  SqliteObjectRow,
  SqliteParameterBinding,
  StreamQueryOptions,
  UpdateListener
} from '@sqlite-js/driver';

import { Deferred } from './deferred.js';
import { SqliteError } from '@sqlite-js/driver';
import {
  InferBatchResult,
  InferCommandResult,
  isErrorResponse,
  SqliteCommand,
  SqliteCommandType,
  SqliteDriverError
} from '@sqlite-js/driver/worker/protocol';

export interface WorkerDriverConnectionOptions {
  path: string;
  connectionName?: string;
  readonly?: boolean;
  workerOptions?: WorkerOptions;
}

interface CommandQueueItem {
  cmd: SqliteCommand;
  resolve?: (r: any) => void;
  reject?: (e: SqliteDriverError) => void;
}

/**
 * Driver connection using Web Workers.
 */
export class WorkerDriverConnection implements SqliteDriverConnection {
  worker: Worker;
  private callbacks = new Map<number, (value: any) => void>();
  private nextCallbackId = 1;
  private ready: Promise<void>;
  private closing = false;
  private nextId = 1;
  private options: WorkerDriverConnectionOptions;

  private buffer: CommandQueueItem[] = [];
  private inProgress = 0;

  constructor(worker: Worker, options: WorkerDriverConnectionOptions) {
    this.worker = worker;
    this.options = options;

    worker.addEventListener('error', (err) => {
      console.error('worker error', err.message, err);
    });

    this.ready = new Promise<void>((resolve) => {
      worker.addEventListener('message', (event) => {
        const { id, value } = event.data;
        if (id === 0) {
          resolve();
          return;
        }
        const callback = this.callbacks.get(id);
        if (callback == null) {
          throw new Error(`No callback with id ${id}`);
        }
        this.callbacks.delete(id);
        callback(value);
      });
    });
  }

  open() {
    return this.post('open', this.options);
  }

  prepare(sql: string, options?: PrepareOptions): WorkerDriverStatement {
    const id = this.nextId++;
    this.buffer.push({
      cmd: {
        type: SqliteCommandType.prepare,
        id,
        sql,
        autoFinalize: options?.autoFinalize
      }
    });
    this._maybeFlush();
    return new WorkerDriverStatement(this, id);
  }

  async close() {
    if (this.closing) {
      return;
    }
    this.closing = true;
    await this._flush();
    const r: any = await this.post('close', {});
    if (r?.error) {
      throw r.error;
    }
    await this.worker.terminate();
  }

  _push<T extends SqliteCommand>(cmd: T): Promise<InferCommandResult<T>> {
    const deferred = new Deferred<any>();
    this.buffer.push({ cmd, resolve: deferred.resolve, reject: deferred.reject });
    this._maybeFlush();
    return deferred.promise as Promise<InferCommandResult<T>>;
  }

  _send(cmd: SqliteCommand): void {
    this.buffer.push({ cmd });
    this._maybeFlush();
  }

  private registerCallback(callback: (value: any) => void) {
    const id = this.nextCallbackId++;
    this.callbacks.set(id, callback);
    return id;
  }

  private async post<T>(command: string, args: any): Promise<T> {
    await this.ready;
    let id: number;
    const p = new Promise<T>((resolve) => {
      id = this.registerCallback(resolve);
    });
    this.worker.postMessage([command, id!, args]);
    const result = await p;
    const error = (result as any)?.error;
    if (error != null) {
      return {
        error: new SqliteError(error)
      } as any;
    }
    return result;
  }

  private async _flush() {
    const commands = this.buffer;
    if (commands.length === 0) {
      return;
    }
    this.buffer = [];
    const responses = await this._execute(commands.map((c) => c.cmd));
    for (let i = 0; i < commands.length; i++) {
      const entry = commands[i];
      const response = responses[i];
      if (response == null) {
        entry.reject?.({ message: 'no result received', code: '' });
      } else if (isErrorResponse(response)) {
        entry.reject?.(response.error);
      } else if (entry.resolve) {
        entry.resolve(response.value);
      }
    }
  }

  private async _maybeFlush() {
    if (this.inProgress > 2) {
      return;
    }
    this.inProgress += 1;
    try {
      while (this.buffer.length > 0) {
        await this._flush();
      }
    } finally {
      this.inProgress -= 1;
    }
  }

  private async _execute<const T extends SqliteCommand[]>(
    commands: T
  ): Promise<InferBatchResult<T>> {
    return await this.post('execute', commands);
  }

  onUpdate(
    listener: UpdateListener,
    options?: { tables?: string[] | undefined; batchLimit?: number | undefined }
  ): () => void {
    throw new Error('Not implemented');
  }
}

class WorkerDriverStatement implements SqliteDriverStatement {
  [Symbol.dispose]: () => void = undefined as any;

  constructor(
    private driver: WorkerDriverConnection,
    private id: number
  ) {
    if (typeof Symbol.dispose !== 'undefined') {
      this[Symbol.dispose] = () => this.finalize();
    }
  }

  async all(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteObjectRow[]> {
    return this.driver
      ._push({
        type: SqliteCommandType.query,
        id: this.id,
        parameters,
        options
      })
      .then((result) => result.rows as SqliteObjectRow[]);
  }

  async allArray(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteArrayRow[]> {
    return this.driver
      ._push({
        type: SqliteCommandType.query,
        id: this.id,
        parameters,
        options,
        array: true
      })
      .then((result) => result.rows as SqliteArrayRow[]);
  }

  stream(
    _parameters?: SqliteParameterBinding,
    _options?: StreamQueryOptions
  ): AsyncIterableIterator<SqliteObjectRow[]> {
    throw new Error('Method not implemented.');
  }

  streamArray(
    _parameters?: SqliteParameterBinding,
    _options?: StreamQueryOptions
  ): AsyncIterableIterator<SqliteArrayRow[]> {
    throw new Error('Method not implemented.');
  }

  async getColumns(): Promise<string[]> {
    return this.driver
      ._push({
        type: SqliteCommandType.parse,
        id: this.id
      })
      .then((result) => result.columns);
  }

  async run(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteChanges> {
    return this.driver._push({
      type: SqliteCommandType.run,
      id: this.id,
      parameters,
      options
    });
  }

  finalize(): void {
    this.driver._send({
      type: SqliteCommandType.finalize,
      id: this.id
    });
  }
}
