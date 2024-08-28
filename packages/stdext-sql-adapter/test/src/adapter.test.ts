import * as fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { assert, test } from 'vitest';
import {
  BetterSqliteClient,
  BetterSqliteClientPool,
  SqliteClient,
  SqliteClientPool
} from '../../lib/index.js';

const assertEquals = assert.deepEqual;

test('sqlite', async () => {
  const DB_URL = new URL('../../test.db', import.meta.url);
  const path = fileURLToPath(DB_URL);

  // Remove any existing test.db.
  await fs.rm(path).catch(() => {});
  await fs.rm(path + '-shm').catch(() => {});
  await fs.rm(path + '-wal').catch(() => {});

  // To test the pool:
  // let pool: SqliteClientPool = new BetterSqliteClientPool(path);
  // let db = await pool.acquire();

  let db: SqliteClient = new BetterSqliteClient(path);
  await db.connect();

  await db.execute('pragma journal_mode = WAL');
  await db.execute('pragma synchronous = normal');
  assertEquals(await db.execute('pragma temp_store = memory'), 0);

  const [version] = (await db
    .prepare('select sqlite_version()')
    .queryOneArray<[string]>())!;

  await db.execute(`create table test (
      integer integer,
      text text not null,
      double double,
      blob blob not null,
      nullable integer
    )`);

  await db.execute(
    `insert into test (integer, text, double, blob, nullable)
      values (?, ?, ?, ?, ?)`,
    [0, 'hello world', 3.14, new Uint8Array([1, 2, 3]), null]
  );

  await db.execute('delete from test where integer = 0');

  const SQL = `insert into test (integer, text, double, blob, nullable)
    values (?, ?, ?, ?, ?)`;
  const stmt = db.prepare(SQL);

  await db.transaction(async () => {
    const data: any[][] = [];
    for (let i = 0; i < 10; i++) {
      data.push([i, `hello ${i}`, 3.14, new Uint8Array([3, 2, 1]), null]);
    }

    for (const row of data) {
      stmt.execute(row);
    }
  });

  await stmt.deallocate();

  const row = (
    await db
      .prepare('select * from test where integer = 0')
      .queryArray<[number, string, number, Uint8Array, null]>()
  )[0];

  assertEquals(row[0], 0);
  assertEquals(row[1], 'hello 0');
  assertEquals(row[2], 3.14);
  assertEquals(row[3], new Uint8Array([3, 2, 1]));
  assertEquals(row[4], null);

  const rows = await db
    .prepare('select * from test where integer != ? and text != ?')
    .query<{
      integer: number;
      text: string;
      double: number;
      blob: Uint8Array;
      nullable: null;
    }>([1, 'hello world']);

  assertEquals(rows.length, 9);
  for (const row of rows) {
    assertEquals(typeof row.integer, 'number');
    assertEquals(row.text, `hello ${row.integer}`);
    assertEquals(row.double, 3.14);
    assertEquals(row.blob, new Uint8Array([3, 2, 1]));
    assertEquals(row.nullable, null);
  }
});
