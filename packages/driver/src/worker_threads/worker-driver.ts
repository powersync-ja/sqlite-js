import * as worker_threads from 'worker_threads';
import {
  PrepareOptions,
  ResetOptions,
  SqliteDriverConnection,
  SqliteDriverStatement,
  SqliteParameterBinding,
  SqliteChanges,
  UpdateListener,
  QueryOptions,
  SqliteArrayRow,
  SqliteObjectRow,
  StreamQueryOptions
} from '../driver-api.js';

import { Deferred } from '../deferred.js';
import { SqliteError } from '../sqlite-error.js';
import {
  InferBatchResult,
  InferCommandResult,
  isErrorResponse,
  SqliteCommand,
  SqliteCommandType,
  SqliteDriverError
} from './async-commands.js';

export interface WorkerDriverConnectionOptions {
  path: string;
  connectionName?: string;
  readonly?: boolean;
  workerOptions?: worker_threads.WorkerOptions;
}

/**
 * Driver connection using worker_threads.
 */
export class WorkerDriverConnection implements SqliteDriverConnection {
  worker: worker_threads.Worker;
  private callbacks = new Map<number, (value: any) => void>();
  private nextCallbackId = 1;
  private ready: Promise<void>;
  private closing = false;
  private nextId = 1;

  buffer: CommandQueueItem[] = [];

  constructor(
    workerPath: string | URL,
    options: WorkerDriverConnectionOptions
  ) {
    const worker = new worker_threads.Worker(
      workerPath,
      options?.workerOptions
    );
    this.post('open', options);
    worker.addListener('error', (err) => {
      console.error('worker error', err);
    });
    this.ready = new Promise<void>((resolve) => {
      worker.addListener('message', (event) => {
        const { id, value } = event;
        if (id == 0) {
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
    this.worker = worker;
  }

  prepare(sql: string, options?: PrepareOptions): WorkerDriverStatement {
    const id = this.nextId++;
    this.buffer.push({
      cmd: {
        type: SqliteCommandType.prepare,
        id,
        autoFinalize: options?.autoFinalize,
        sql
      }
    });
    return new WorkerDriverStatement(this, id);
  }

  _push<T extends SqliteCommand>(cmd: T): Promise<InferCommandResult<T>> {
    const d = new Deferred<any>();
    this.buffer.push({ cmd, resolve: d.resolve, reject: d.reject });
    this._maybeFlush();
    return d.promise as Promise<InferCommandResult<T>>;
  }

  _send(cmd: SqliteCommand): void {
    this.buffer.push({ cmd });
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
    return p;
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

  private inProgress = 0;

  async _flush() {
    const commands = this.buffer;
    if (commands.length == 0) {
      return;
    }
    this.buffer = [];
    const r = await this._execute(commands.map((c) => c.cmd));
    for (let i = 0; i < commands.length; i++) {
      const c = commands[i];
      const rr = r[i];
      if (isErrorResponse(rr)) {
        c.reject?.(rr.error);
      } else if (c.resolve) {
        c.resolve!(rr.value);
      }
    }
  }

  async _maybeFlush() {
    if (this.inProgress <= 2) {
      this.inProgress += 1;
      try {
        while (this.buffer.length > 0) {
          await this._flush();
        }
      } finally {
        this.inProgress -= 1;
      }
    }
  }

  async _execute<const T extends SqliteCommand[]>(
    commands: T
  ): Promise<InferBatchResult<T>> {
    return await this.post('execute', commands);
  }

  onUpdate(
    listener: UpdateListener,
    options?:
      | { tables?: string[] | undefined; batchLimit?: number | undefined }
      | undefined
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
    if (typeof Symbol.dispose != 'undefined') {
      this[Symbol.dispose] = () => this.finalize();
    }
  }
  all(
    parameters: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteObjectRow[]> {
    return this.driver
      ._push({
        type: SqliteCommandType.query,
        id: this.id,
        parameters: parameters,
        options: options
      })
      .then((r) => r.rows as SqliteObjectRow[]);
  }

  allArray(
    parameters: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteArrayRow[]> {
    return this.driver
      ._push({
        type: SqliteCommandType.query,
        id: this.id,
        parameters: parameters,
        options: options,
        array: true
      })
      .then((r) => r.rows as SqliteArrayRow[]);
  }

  stream(
    parameters: SqliteParameterBinding,
    options?: StreamQueryOptions
  ): AsyncIterator<SqliteObjectRow[]> {
    throw new Error('Method not implemented.');
  }

  streamArray(
    parameters: SqliteParameterBinding,
    options?: StreamQueryOptions
  ): AsyncIterator<SqliteArrayRow[]> {
    throw new Error('Method not implemented.');
  }

  async getColumns(): Promise<string[]> {
    return this.driver
      ._push({
        type: SqliteCommandType.parse,
        id: this.id
      })
      .then((r) => r.columns);
  }

  async run(
    parameters?: SqliteParameterBinding,
    options?: QueryOptions
  ): Promise<SqliteChanges> {
    return this.driver._push({
      type: SqliteCommandType.run,
      id: this.id,
      parameters: parameters,
      options: options
    });
  }

  finalize(): void {
    this.driver._send({
      type: SqliteCommandType.finalize,
      id: this.id
    });
  }
}

interface CommandQueueItem {
  cmd: SqliteCommand;
  resolve?: (r: any) => void;
  reject?: (e: SqliteDriverError) => void;
}
