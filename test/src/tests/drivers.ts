import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { SqliteDriverConnectionPool } from '../../../lib/driver-api.js';

export function describeDriverTests(
  name: string,
  factory: (path: string) => SqliteDriverConnectionPool
) {
  describe(`${name} - driver tests`, () => {
    let dbPath: string;
    let _dbs: SqliteDriverConnectionPool[] = [];

    const open = async () => {
      const dir = path.dirname(dbPath);
      try {
        await fs.mkdir(dir);
      } catch (e) {}
      try {
        await fs.rm(dbPath);
      } catch (e) {}
      const db = factory(dbPath);
      _dbs.push(db);
      return db;
    };

    beforeEach(({ expect }) => {
      const testNameSanitized = expect
        .getState()
        .currentTestName!.replaceAll(/[\s\/\\>\.\-]+/g, '_');
      dbPath = `test-db/${testNameSanitized}.db`;
    });

    afterEach(async () => {
      const closeDbs = _dbs;
      _dbs = [];
      for (let db of closeDbs) {
        await db.close();
      }
    });

    test('basic select', async () => {
      const driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select 1 as one'
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        { sync: {} }
      ]);

      const [{ columns }, { rows }] = results as any[];

      expect(columns).toEqual(['one']);
      expect(rows).toEqual([[1]]);
    });

    test('big number', async () => {
      const driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select 9223372036854775807 as bignumber'
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        { sync: {} }
      ]);

      const [, { rows }] = results as any[];

      expect(rows).toEqual([[9223372036854776000]]);

      const results2 = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select ? as bignumber'
          }
        },
        {
          bind: {
            id: 0,
            parameters: [9223372036854775807n]
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        { sync: {} }
      ]);

      const [, , { rows: rows2 }] = results2 as any[];

      expect(rows2).toEqual([[9223372036854776000]]);
    });

    test('bigint', async () => {
      const driver = await open();
      using connection = await driver.reserveConnection();
      const results1 = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select ? as bignumber'
          }
        },
        {
          bind: {
            id: 0,
            parameters: [9223372036854775807n]
          }
        },
        {
          step: {
            id: 0,
            all: true,
            bigint: true
          }
        },
        { sync: {} }
      ]);

      const [, , { rows: rows1 }] = results1 as any[];

      expect(rows1).toEqual([[9223372036854775807n]]);

      const results2 = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select 9223372036854775807 as bignumber'
          }
        },
        {
          step: {
            id: 0,
            all: true,
            bigint: true
          }
        },
        { sync: {} }
      ]);

      const [, { rows: rows2 }] = results2 as any[];

      expect(rows2).toEqual([[9223372036854775807n]]);
    });

    test('insert returning', async () => {
      const driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'create table test_data(id integer primary key, data text)'
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          prepare: {
            id: 0,
            sql: 'insert into test_data(data) values(123) returning id'
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        { sync: {} }
      ]);

      const [, , { columns }, { rows }, { error }] = results as any[];

      expect(error).toBe(undefined);
      expect(columns).toEqual(['id']);
      expect(rows).toEqual([[1]]);
    });

    test('runWithResults', async () => {
      const driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'create table test_data(id integer primary key, data text)'
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          prepare: {
            id: 0,
            sql: 'insert into test_data(data) values(123)'
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          last_insert_row_id: {}
        },
        {
          changes: {}
        },
        { sync: {} }
      ]);

      const [, , , , { last_insert_row_id }, { changes }, { error }] =
        results as any[];

      expect(error).toBe(undefined);
      expect(changes).toEqual(1);
      expect(last_insert_row_id).toEqual(1n);
    });

    test('runWithResults - returning statement', async () => {
      const driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'create table test_data(id integer primary key, data text)'
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          prepare: {
            id: 0,
            sql: 'insert into test_data(data) values(123) returning id'
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          last_insert_row_id: {}
        },
        {
          changes: {}
        },
        { sync: {} }
      ]);

      const [, , , { rows }, { last_insert_row_id }, { changes }, { error }] =
        results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1]]);
      expect(changes).toEqual(1);
      expect(last_insert_row_id).toEqual(1n);
    });

    test('runWithResults - select', async () => {
      const driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select 1 as one'
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          last_insert_row_id: {}
        },
        {
          changes: {}
        },
        { sync: {} }
      ]);

      const [, { rows }, { last_insert_row_id }, { changes }, { error }] =
        results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1]]);
      expect(changes).toEqual(0);
      expect(last_insert_row_id).toEqual(0n);
    });

    test.skip('onUpdate', async () => {
      // Skipped: Not properly implemented yet.

      const driver = await open();
      using connection = await driver.reserveConnection();
      // await connection.run(
      //   "create table test_data(id integer primary key, data text)"
      // );
      // // TODO: test the results
      // connection.onUpdate(({ events }) => {
      //   console.log("update", events);
      // });
      // await connection.run(
      //   "insert into test_data(data) values(123) returning id"
      // );
      // await connection.run("update test_data set data = data || 'test'");
    });
  });
}
