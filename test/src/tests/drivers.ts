import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { SqliteDriverConnectionPool } from '../../../lib/driver-api.js';
import { SqliteValue } from '../../../lib/common.js';

export function describeDriverTests(
  name: string,
  factory: (path: string) => SqliteDriverConnectionPool
) {
  describe(`${name} - driver tests`, () => {
    let dbPath: string;

    const open = async () => {
      const dir = path.dirname(dbPath);
      try {
        await fs.mkdir(dir);
      } catch (e) {}
      try {
        await fs.rm(dbPath);
      } catch (e) {}
      const db = factory(dbPath);
      return db;
    };

    beforeEach(({ expect }) => {
      const testNameSanitized = expect
        .getState()
        .currentTestName!.replaceAll(/[\s\/\\>\.\-]+/g, '_');
      dbPath = `test-db/${testNameSanitized}.db`;
    });

    test('basic select', async () => {
      await using driver = await open();
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
      await using driver = await open();
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
      await using driver = await open();
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

      const [, , { rows: rows1 }, { error: error1 }] = results1 as any[];

      expect(error1).toBe(undefined);
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

      const [, { rows: rows2 }, { error }] = results2 as any[];

      expect(error).toBe(undefined);

      expect(rows2).toEqual([[9223372036854775807n]]);
    });

    test('insert returning', async () => {
      await using driver = await open();
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

    test('bind named args', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select :one as one, :two as two'
          }
        },
        {
          bind: {
            id: 0,
            parameters: { one: 1, two: 2 }
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

      const [, , { rows }, { error }] = results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1, 2]]);
    });

    test.skip('skip named arg', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select :one as one, :two as two'
          }
        },
        {
          bind: {
            id: 0,
            parameters: { two: 2 }
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

      const [, , { rows }, { error }] = results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[null, 2]]);
    });

    test('rebind arg', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select :one as one, :two as two'
          }
        },
        {
          bind: {
            id: 0,
            parameters: { one: 1, two: 2 }
          }
        },
        {
          bind: {
            id: 0,
            parameters: { one: 11, two: 22 }
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

      const [, , , { rows }, { error }] = results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[11, 22]]);
    });

    test('partial rebind', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select :one as one, :two as two'
          }
        },
        {
          bind: {
            id: 0,
            parameters: { one: 1, two: 2 }
          }
        },
        {
          bind: {
            id: 0,
            parameters: { two: 22 }
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

      const [, , , { rows }, { error }] = results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1, 22]]);
    });

    test('positional parameters', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select ? as one, ? as two'
          }
        },
        {
          bind: {
            id: 0,
            parameters: [1, 2]
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

      const [, , { rows }, { error }] = results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1, 2]]);
    });

    test('positional specific parameters', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select ?2 as two, ?1 as one'
          }
        },
        {
          bind: {
            id: 0,
            parameters: { '1': 1, '2': 2 }
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

      const [, , { rows }, { error }] = results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[2, 1]]);
    });

    test('positional parameters partial rebind', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select ? as one, ? as two'
          }
        },
        {
          bind: {
            id: 0,
            parameters: [1, 2]
          }
        },
        {
          bind: {
            id: 0,
            parameters: [undefined, 22]
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

      const [, , , { rows }, { error }] = results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1, 22]]);
    });

    test('named and positional parameters', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select ? as one, @three as three, ? as two'
          }
        },
        {
          bind: {
            id: 0,
            parameters: [1, 2]
          }
        },
        {
          bind: {
            id: 0,
            parameters: { three: 3 }
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

      const [, , , { rows }, { error }] = results as any[];

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1, 3, 2]]);
    });

    test('reset parameters', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select ? as one, ? as two'
          }
        },
        {
          bind: {
            id: 0,
            parameters: [1, 2]
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          reset: {
            id: 0
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          reset: {
            id: 0,
            clear_bindings: true
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

      const [
        ,
        ,
        { rows: rows1 },
        ,
        { rows: rows2 },
        ,
        { error },

        { error: totalError }
      ] = results as any[];

      // expect(error).toBe(undefined);
      expect(rows1).toEqual([[1, 2]]);
      expect(rows2).toEqual([[1, 2]]);
      expect(error).toMatchObject({
        message: 'Too few parameter values were provided'
      });
    });

    test('partial reset', async () => {
      await using driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: "select json_each.value as v from json_each('[1,2,3,4,5]')"
          }
        },
        {
          step: {
            id: 0,
            n: 3
          }
        },
        {
          reset: {
            id: 0
          }
        },
        {
          step: {
            id: 0,
            n: 3
          }
        },
        {
          step: {
            id: 0,
            n: 3
          }
        },
        {
          step: {
            id: 0,
            n: 3
          }
        },
        {
          reset: {
            id: 0
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

      const [
        ,
        { rows: rows1 },
        ,
        { rows: rows2 },
        { rows: rows3 },
        { rows: rows4 },
        ,
        { rows: rows5 },
        { error }
      ] = results as any[];

      expect(error).toBe(undefined);
      expect(rows1).toEqual([[1], [2], [3]]);
      expect(rows2).toEqual([[1], [2], [3]]);
      expect(rows3).toEqual([[4], [5]]);
      expect(rows4).toEqual([]);
      expect(rows5).toEqual([[1], [2], [3], [4], [5]]);
    });

    test('multiple insert step', async () => {
      await using driver = await open();
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
            sql: "insert into test_data(data) values('test')"
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          step: {
            id: 0,
            all: true
          }
        },
        {
          reset: {
            id: 0
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
            sql: 'select count(*) from test_data'
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

      const [
        ,
        ,
        ,
        { rows: rows1 },
        { rows: rows2 },
        ,
        { rows: rows3 },
        ,
        { rows: rows4 },
        { error }
      ] = results as any[];

      expect(error).toBe(undefined);
      expect(rows1).toEqual([]);
      expect(rows2).toEqual([]);
      expect(rows3).toEqual([]);
      expect(rows4).toEqual([[2]]);
    });

    test.skip('onUpdate', async () => {
      // Skipped: Not properly implemented yet.

      await using driver = await open();
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
