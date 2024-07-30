import type * as bsqlite from 'better-sqlite3';
import * as worker_threads from 'worker_threads';
import {
  InferBatchResult,
  InferCommandResult,
  PrepareOptions,
  ResetOptions,
  SqliteCommand,
  SqliteCommandResponse,
  SqliteCommandType,
  SqliteDriverConnection,
  SqliteDriverStatement,
  SqliteParameterBinding,
  SqliteStepResponse,
  StepOptions,
  UpdateListener
} from '../driver-api.js';

import { Deferred } from '../deferred.js';
import { SqliteError } from './util.js';

class AsyncDriverStatement implements SqliteDriverStatement {
  [Symbol.dispose]: () => void = undefined as any;

  constructor(
    private driver: AsyncDriverConnection,
    private id: number
  ) {
    if (typeof Symbol.dispose != 'undefined') {
      this[Symbol.dispose] = () => this.finalize();
    }
  }

  async getColumns(): Promise<string[]> {
    return this.driver
      ._push({
        type: SqliteCommandType.parse,
        id: this.id
      })
      .then((r) => r.columns);
  }

  bind(parameters: SqliteParameterBinding): void {
    this.driver._send({
      type: SqliteCommandType.bind,
      id: this.id,
      parameters: parameters
    });
  }

  async step(n?: number, options?: StepOptions): Promise<SqliteStepResponse> {
    return this.driver._push({
      type: SqliteCommandType.step,
      id: this.id,
      n: n,
      requireTransaction: options?.requireTransaction
    });
  }

  finalize(): void {
    this.driver._send({
      type: SqliteCommandType.finalize,
      id: this.id
    });
  }

  reset(options?: ResetOptions): void {
    this.driver._send({
      type: SqliteCommandType.reset,
      id: this.id,
      clear_bindings: options?.clear_bindings
    });
  }
}

interface CommandQueueItem {
  cmd: SqliteCommand;
  resolve?: (r: SqliteCommandResponse) => void;
  reject?: (e: any) => void;
}

export class AsyncDriverConnection implements SqliteDriverConnection {
  worker: worker_threads.Worker;
  private callbacks = new Map<number, (value: any) => void>();
  private nextCallbackId = 1;
  private ready: Promise<void>;
  private closing = false;
  private nextId = 1;

  buffer: CommandQueueItem[] = [];

  constructor(workerPath: string, path: string, options?: bsqlite.Options) {
    const worker = new worker_threads.Worker(workerPath);
    this.post('open', { path, options });
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

  prepare(sql: string, options?: PrepareOptions): AsyncDriverStatement {
    const id = this.nextId++;
    this.buffer.push({
      cmd: {
        type: SqliteCommandType.prepare,
        id,
        bigint: options?.bigint,
        persist: options?.persist,
        rawResults: options?.rawResults,
        sql
      }
    });
    return new AsyncDriverStatement(this, id);
  }

  async sync(): Promise<void> {
    await this._push({
      type: SqliteCommandType.sync
    });
  }

  _push<T extends SqliteCommand>(cmd: T): Promise<InferCommandResult<T>> {
    const d = new Deferred<SqliteCommandResponse>();
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
      if (rr.error) {
        c.reject?.(rr.error);
      } else if (c.resolve) {
        c.resolve!(rr);
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
