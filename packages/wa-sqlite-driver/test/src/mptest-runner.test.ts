import { describe, test } from '@sqlite-js/driver-tests';
import { SqliteError, SqliteValue } from '@sqlite-js/driver';
import type {
  ReservedConnection,
  SqliteDriverConnectionPool,
  SqliteRowRaw
} from '@sqlite-js/driver';

import { waSqliteSingleWorker } from '../../lib/index.js';

const scriptModules = (import.meta as any).glob('./mptest/**/*test', {
  as: 'raw',
  eager: true
});

const scriptMap = new Map<string, string>();
for (const [key, value] of Object.entries(scriptModules)) {
  scriptMap.set(normalizePath(key), value as string);
}

const topLevelScripts = [...scriptMap.keys()]
  .filter((name) => name.endsWith('.test'))
  .sort();

interface ScriptContext {
  clientId: number;
  connection: ReservedConnection;
  filename: string;
  displayName: string;
}

interface TokenInfo {
  length: number;
  newlines: number;
}

class ResultBuffer {
  private parts: string[] = [];

  append(value: SqliteValue | string): void {
    this.parts.push(formatTerm(value));
  }

  appendError(error: SqliteError): void {
    const code = error.code ?? 'SQLITE_ERROR';
    this.parts.push(formatTerm(`error(${code})`));
    this.parts.push(formatTerm(error.message));
  }

  reset(): void {
    this.parts = [];
  }

  tokens(): string[] {
    return [...this.parts];
  }

  toString(): string {
    return this.parts.join(' ');
  }
}

interface ClientContext {
  id: number;
  pool: SqliteDriverConnectionPool;
  reserved: ReservedConnection;
  queue: Promise<void>;
  pending: Set<Promise<void>>;
}

class MptestRunner {
  private clients = new Map<number, ClientContext>();
  private pendingTasks = new Set<Promise<void>>();

  constructor(private readonly dbPath: string) {}

  async runScript(scriptName: string): Promise<void> {
    const script = requireScript(scriptName);
    const master = await this.ensureClient(0);
    await this.runScriptInternal(
      {
        clientId: 0,
        connection: master.reserved,
        filename: scriptName,
        displayName: baseName(scriptName)
      },
      script,
      1
    );
    await this.waitForAll(0);
  }

  async close(): Promise<void> {
    const errors: Error[] = [];
    await this.waitForAll(0).catch((err) => {
      errors.push(err instanceof Error ? err : new Error(String(err)));
    });
    await Promise.all(
      [...this.clients.values()].map(async (client) => {
        try {
          await client.reserved.release();
        } catch (err) {
          errors.push(err instanceof Error ? err : new Error(String(err)));
        }
        try {
          await client.pool.close();
        } catch (err) {
          errors.push(err instanceof Error ? err : new Error(String(err)));
        }
      })
    );
    this.clients.clear();
    if (errors.length > 0) {
      throw errors[0];
    }
  }

  private async ensureClient(clientId: number): Promise<ClientContext> {
    let client = this.clients.get(clientId);
    if (client) {
      return client;
    }
    const pool = waSqliteSingleWorker(this.dbPath);
    const reserved = await pool.reserveConnection();
    client = {
      id: clientId,
      pool,
      reserved,
      queue: Promise.resolve(),
      pending: new Set()
    };
    this.clients.set(clientId, client);
    return client;
  }

  private trackTask(client: ClientContext, taskPromise: Promise<void>): void {
    client.pending.add(taskPromise);
    this.pendingTasks.add(taskPromise);
    taskPromise.finally(() => {
      client.pending.delete(taskPromise);
      this.pendingTasks.delete(taskPromise);
    });
  }

  private async scheduleTask(
    parent: ScriptContext,
    clientId: number,
    script: string,
    startLine: number,
    taskLabel: string
  ): Promise<void> {
    const client = await this.ensureClient(clientId);
    const run = async () => {
      await this.runScriptInternal(
        {
          clientId,
          connection: client.reserved,
          filename: parent.filename,
          displayName: `${parent.displayName}#client${clientId}:${taskLabel}`
        },
        script,
        startLine
      );
    };
    const task = client.queue.then(run);
    client.queue = task.catch(() => {});
    this.trackTask(client, task);
  }

  private async waitForAll(timeoutMs: number): Promise<void> {
    await this.waitWithTimeout(
      [...this.pendingTasks],
      'all clients',
      timeoutMs
    );
  }

  private async waitForClient(
    clientId: number,
    timeoutMs: number
  ): Promise<void> {
    const client = this.clients.get(clientId);
    const pending = client ? [...client.pending] : [];
    await this.waitWithTimeout(pending, `client ${clientId}`, timeoutMs);
  }

  private async waitWithTimeout(
    promises: Promise<unknown>[],
    label: string,
    timeoutMs: number
  ): Promise<void> {
    if (promises.length === 0) {
      return;
    }
    const waitPromise = Promise.all(promises).then(() => undefined);
    if (timeoutMs <= 0) {
      await waitPromise;
      return;
    }
    let handle: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        waitPromise,
        new Promise((_, reject) => {
          handle = setTimeout(
            () =>
              reject(
                new Error(`timeout waiting for ${label} (${timeoutMs}ms)`)
              ),
            timeoutMs
          );
        })
      ]);
    } finally {
      if (handle) {
        clearTimeout(handle);
      }
    }
  }

  private async runScriptInternal(
    context: ScriptContext,
    script: string,
    initialLine: number
  ): Promise<void> {
    const result = new ResultBuffer();
    let index = 0;
    let begin = 0;
    let line = initialLine;
    while (index < script.length) {
      const prevLine = line;
      const token = tokenLength(script, index);
      if (token.length <= 0) {
        break;
      }
      line += token.newlines;
      const chr = script[index];
      if (isWhitespace(chr) || (chr === '/' && script[index + 1] === '*')) {
        index += token.length;
        continue;
      }
      if (
        chr !== '-' ||
        script[index + 1] !== '-' ||
        !isAlpha(script[index + 2])
      ) {
        index += token.length;
        continue;
      }
      if (index > begin) {
        const sqlChunk = script.slice(begin, index);
        await this.executeSql(context, sqlChunk, result);
      }
      let consumed = token.length;
      const command = parseCommand(script, index, token.length);
      switch (command.name) {
        case 'sleep':
          await sleepMs(Number(command.args[0] ?? '0'));
          break;
        case 'match': {
          const expectedRaw = command.payload.trim();
          const expectedTokens = tokenizeMatch(expectedRaw);
          const actualTokens = result.tokens();
          if (!tokensEqual(expectedTokens, actualTokens)) {
            throw new Error(
              `${context.displayName}:${prevLine} expected [${expectedRaw}] but got [${result.toString()}]`
            );
          }
          result.reset();
          break;
        }
        case 'task': {
          const clientId = Number(command.args[0]);
          if (!Number.isInteger(clientId) || clientId < 0) {
            throw new Error(
              `${context.displayName}:${prevLine} invalid client ${command.args[0]}`
            );
          }
          const lineRef = { value: line };
          const blockStart = index + consumed;
          const blockLength = findEnd(script, blockStart, lineRef);
          const endToken = tokenLength(script, blockStart + blockLength);
          line = lineRef.value + endToken.newlines;
          consumed += blockLength + endToken.length;
          const taskScript = script.slice(blockStart, blockStart + blockLength);
          const taskLabel = command.args[1]
            ? command.args[1]
            : `${baseName(context.filename)}:${prevLine}`;
          await this.scheduleTask(
            context,
            clientId,
            taskScript,
            prevLine + 1,
            taskLabel
          );
          break;
        }
        case 'wait': {
          const target = command.args[0] ?? 'all';
          const timeout = command.args[1] ? Number(command.args[1]) : 10000;
          if (target === 'all') {
            await this.waitForAll(timeout);
          } else {
            await this.waitForClient(Number(target), timeout);
          }
          break;
        }
        case 'source': {
          const sourceName = command.args[0];
          if (!sourceName) {
            throw new Error(
              `${context.displayName}:${prevLine} missing source filename`
            );
          }
          const resolved = resolveSourcePath(context.filename, sourceName);
          const subScript = requireScript(resolved);
          const master = await this.ensureClient(0);
          await this.runScriptInternal(
            {
              clientId: 0,
              connection: master.reserved,
              filename: resolved,
              displayName: baseName(resolved)
            },
            subScript,
            1
          );
          break;
        }
        case 'if': {
          const condition = await this.evaluateCondition(
            context,
            command.payload
          );
          if (!condition) {
            const lineRef = { value: line };
            const skip = findEndif(script, index + consumed, true, lineRef);
            line = lineRef.value;
            consumed += skip;
          }
          break;
        }
        case 'else': {
          const lineRef = { value: line };
          const skip = findEndif(script, index + consumed, false, lineRef);
          line = lineRef.value;
          consumed += skip;
          break;
        }
        case 'endif':
          break;
        default:
          throw new Error(
            `${context.displayName}:${prevLine} unknown command --${command.name}`
          );
      }
      begin = index + consumed;
      index += consumed;
    }
    if (begin < script.length) {
      const remainder = script.slice(begin);
      await this.executeSql(context, remainder, result);
    }
  }

  private async evaluateCondition(
    context: ScriptContext,
    expr: string
  ): Promise<boolean> {
    const sql = `SELECT ${expr}`;
    const statement = context.connection.prepare(sql, { rawResults: true });
    try {
      const { rows } = await statement.step();
      if (!rows || rows.length === 0) {
        return false;
      }
      const row = rows[0] as SqliteRowRaw;
      const value = row[0];
      if (value == null) {
        return false;
      }
      if (typeof value === 'number') {
        return value !== 0;
      }
      if (typeof value === 'bigint') {
        return value !== BigInt(0);
      }
      if (typeof value === 'string') {
        return value.length > 0;
      }
      return true;
    } finally {
      statement.finalize();
    }
  }

  private async executeSql(
    context: ScriptContext,
    chunk: string,
    result: ResultBuffer
  ): Promise<void> {
    const statements = splitSql(chunk);
    for (const sql of statements) {
      if (!sql.trim()) {
        continue;
      }
      const statement = context.connection.prepare(sql, { rawResults: true });
      try {
        const { rows } = await statement.step();
        if (rows) {
          for (const row of rows) {
            for (const value of row as SqliteRowRaw) {
              result.append(value);
            }
          }
        }
      } catch (err) {
        if (isSqliteError(err)) {
          result.appendError(err);
        } else {
          throw err;
        }
      } finally {
        statement.finalize();
      }
    }
  }
}

describe('mptest scripts', () => {
  for (const script of topLevelScripts) {
    test(script, async () => {
      const dbPath = `mptest-${sanitizeForFilename(script)}-${Math.random()
        .toString(36)
        .slice(2)}.db`;
      const runner = new MptestRunner(dbPath);
      try {
        await runner.runScript(script);
      } finally {
        await runner.close();
      }
    });
  }
});

function normalizePath(input: string): string {
  return input.startsWith('./') ? input.slice(2) : input;
}

function baseName(file: string): string {
  const normalized = file.replace(/\\/g, '/');
  const idx = normalized.lastIndexOf('/');
  return idx >= 0 ? normalized.slice(idx + 1) : normalized;
}

function sanitizeForFilename(input: string): string {
  return baseName(input).replace(/[^a-zA-Z0-9_-]+/g, '_');
}

function requireScript(path: string): string {
  const normalized = normalizePath(path);
  const script = scriptMap.get(normalized);
  if (script == null) {
    throw new Error(`Unknown script: ${normalized}`);
  }
  return script;
}

function resolveSourcePath(currentFile: string, relative: string): string {
  if (relative.startsWith('/')) {
    return normalizePath(relative.slice(1));
  }
  const currentParts = normalizePath(currentFile).split('/');
  currentParts.pop();
  for (const part of relative.split('/')) {
    if (part === '' || part === '.') {
      continue;
    }
    if (part === '..') {
      if (currentParts.length > 0) {
        currentParts.pop();
      }
    } else {
      currentParts.push(part);
    }
  }
  return currentParts.join('/');
}

function isWhitespace(ch: string | undefined): boolean {
  if (ch == null) {
    return false;
  }
  return (
    ch === ' ' ||
    ch === '\n' ||
    ch === '\r' ||
    ch === '\t' ||
    ch === '\f' ||
    ch === '\v'
  );
}

function isAlpha(ch: string | undefined): boolean {
  if (!ch) {
    return false;
  }
  const code = ch.charCodeAt(0);
  return (code >= 65 && code <= 90) || (code >= 97 && code <= 122);
}

function tokenLength(script: string, start: number): TokenInfo {
  let n = 0;
  let newlines = 0;
  const first = script[start];
  if (first == null) {
    return { length: 0, newlines: 0 };
  }
  if (isWhitespace(first) || (first === '/' && script[start + 1] === '*')) {
    let inComment = first === '/' ? 1 : 0;
    if (first === '/') {
      n = 2;
    }
    while (true) {
      const c = script[start + n];
      n++;
      if (c == null) {
        break;
      }
      if (c === '\n') {
        newlines++;
      }
      if (isWhitespace(c)) {
        continue;
      }
      if (inComment && c === '*' && script[start + n] === '/') {
        n++;
        inComment = 0;
      } else if (!inComment && c === '/' && script[start + n] === '*') {
        n++;
        inComment = 1;
      } else if (!inComment) {
        break;
      }
    }
    n--;
    return { length: n, newlines };
  }
  if (first === '-' && script[start + 1] === '-') {
    n = 2;
    while (start + n < script.length && script[start + n] !== '\n') {
      n++;
    }
    if (start + n < script.length) {
      newlines++;
      n++;
    }
    return { length: n, newlines };
  }
  if (first === '"' || first === "'") {
    const delim = first;
    n = 1;
    while (start + n < script.length) {
      const c = script[start + n];
      if (c === '\n') {
        newlines++;
      }
      n++;
      if (c === delim) {
        if (script[start + n] !== delim) {
          break;
        }
        n++;
      }
    }
    return { length: n, newlines };
  }
  n = 1;
  while (start + n < script.length) {
    const c = script[start + n];
    if (!c || isWhitespace(c) || c === '"' || c === "'" || c === ';') {
      break;
    }
    n++;
  }
  return { length: n, newlines };
}

function findEnd(
  script: string,
  start: number,
  lineRef: { value: number }
): number {
  let offset = 0;
  while (start + offset < script.length) {
    if (
      script.startsWith('--end', start + offset) &&
      isWhitespace(script[start + offset + 5] ?? '\n')
    ) {
      break;
    }
    const token = tokenLength(script, start + offset);
    lineRef.value += token.newlines;
    if (token.length <= 0) {
      break;
    }
    offset += token.length;
  }
  return offset;
}

function findEndif(
  script: string,
  start: number,
  stopAtElse: boolean,
  lineRef: { value: number }
): number {
  let offset = 0;
  while (start + offset < script.length) {
    const current = start + offset;
    const token = tokenLength(script, current);
    lineRef.value += token.newlines;
    if (
      script.startsWith('--endif', current) &&
      isWhitespace(script[current + 7] ?? '\n')
    ) {
      return offset + token.length;
    }
    if (
      stopAtElse &&
      script.startsWith('--else', current) &&
      isWhitespace(script[current + 6] ?? '\n')
    ) {
      return offset + token.length;
    }
    if (
      script.startsWith('--if', current) &&
      isWhitespace(script[current + 4] ?? '\n')
    ) {
      const inner = findEndif(script, current + token.length, false, lineRef);
      offset += token.length + inner;
    } else {
      if (token.length <= 0) {
        break;
      }
      offset += token.length;
    }
  }
  return offset;
}

interface ParsedCommand {
  name: string;
  args: string[];
  payload: string;
}

function parseCommand(
  script: string,
  start: number,
  length: number
): ParsedCommand {
  const segment = script.slice(start, start + length).replace(/\r?\n?$/, '');
  const trimmed = segment.trim();
  const body = trimmed.slice(2).trim();
  const firstSpace = body.search(/\s/);
  let name: string;
  let payload: string;
  if (firstSpace === -1) {
    name = body;
    payload = '';
  } else {
    name = body.slice(0, firstSpace);
    payload = body.slice(firstSpace).trimStart();
  }
  const args = payload ? payload.split(/\s+/) : [];
  return { name, args, payload };
}

function splitSql(script: string): string[] {
  const statements: string[] = [];
  let current = '';
  let inSingle = false;
  let inDouble = false;
  let inBracket = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let i = 0; i < script.length; i++) {
    const c = script[i];
    const next = script[i + 1];
    if (inLineComment) {
      current += c;
      if (c === '\n') {
        inLineComment = false;
      }
      continue;
    }
    if (inBlockComment) {
      current += c;
      if (c === '*' && next === '/') {
        current += next;
        i++;
        inBlockComment = false;
      }
      continue;
    }
    if (!inSingle && !inDouble) {
      if (c === '-' && next === '-') {
        inLineComment = true;
        current += c;
        continue;
      }
      if (c === '/' && next === '*') {
        inBlockComment = true;
        current += c;
        continue;
      }
    }
    if (c === "'" && !inDouble) {
      inSingle = !inSingle;
      current += c;
      if (inSingle && next === "'") {
        current += next;
        i++;
      }
      continue;
    }
    if (c === '"' && !inSingle) {
      inDouble = !inDouble;
      current += c;
      if (inDouble && next === '"') {
        current += next;
        i++;
      }
      continue;
    }
    if (!inSingle && !inDouble) {
      if (c === '[') {
        inBracket = true;
      } else if (c === ']' && inBracket) {
        inBracket = false;
      }
    }
    if (!inSingle && !inDouble && !inBracket && c === ';') {
      const statement = current.trim();
      if (statement) {
        statements.push(statement);
      }
      current = '';
      continue;
    }
    current += c;
  }
  const trailing = current.trim();
  if (trailing) {
    statements.push(trailing);
  }
  return statements;
}

function tokenizeMatch(input: string): string[] {
  const tokens: string[] = [];
  let i = 0;
  while (i < input.length) {
    while (i < input.length && /\s/.test(input[i]!)) i++;
    if (i >= input.length) {
      break;
    }
    if (input[i] === "'") {
      const start = i;
      i++;
      while (i < input.length) {
        if (input[i] === "'") {
          i++;
          if (input[i] === "'") {
            i++;
            continue;
          }
          break;
        }
        i++;
      }
      tokens.push(input.slice(start, i));
    } else {
      const start = i;
      while (i < input.length && !/\s/.test(input[i]!)) i++;
      tokens.push(input.slice(start, i));
    }
  }
  return tokens;
}

function tokensEqual(expected: string[], actual: string[]): boolean {
  if (expected.length !== actual.length) {
    return false;
  }
  for (let i = 0; i < expected.length; i++) {
    const e = expected[i];
    const a = actual[i];
    if (e === a) {
      continue;
    }
    if (normalizeNumeric(e) === normalizeNumeric(a)) {
      continue;
    }
    return false;
  }
  return true;
}

function normalizeNumeric(token: string): string {
  if (!/^-?\d+(?:\.\d+)?$/.test(token)) {
    return token;
  }
  if (!token.includes('.')) {
    return token;
  }
  const negative = token.startsWith('-');
  let body = negative ? token.slice(1) : token;
  body = body.replace(/\.0+$/, '');
  body = body.replace(/(\.\d*?[1-9])0+$/, '$1');
  if (body.endsWith('.')) {
    body = body.slice(0, -1);
  }
  if (!body) {
    body = '0';
  }
  return negative ? `-${body}` : body;
}

function formatTerm(value: SqliteValue | string): string {
  if (value == null) {
    return 'nil';
  }
  if (typeof value === 'string') {
    return formatString(value);
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    return String(value);
  }
  if (value instanceof Uint8Array) {
    return `x'${toHex(value)}'`;
  }
  return formatString(String(value));
}

function formatString(value: string): string {
  if (!/\s/.test(value) && !value.includes("'")) {
    return value;
  }
  return `'${value.replace(/'/g, "''")}'`;
}

function toHex(data: Uint8Array): string {
  let hex = '';
  for (let i = 0; i < data.length; i++) {
    const byte = data[i];
    hex += byte.toString(16).padStart(2, '0');
  }
  return hex;
}

function sleepMs(ms: number): Promise<void> {
  if (ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isSqliteError(err: unknown): err is SqliteError {
  return (
    err instanceof SqliteError ||
    (typeof err === 'object' &&
      err != null &&
      'code' in err &&
      'message' in err)
  );
}
