import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { beforeEach, describe, test } from './test.js';
import { expect } from 'expect';
import { SqliteDriverConnectionPool } from '../../../lib/driver-api.js';

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

    beforeEach((context) => {
      const testNameSanitized = context.fullName.replaceAll(
        /[\s\/\\>\.\-\:]+/g,
        '_'
      );
      dbPath = `test-db/${testNameSanitized}.db`;
    });

    test('basic select', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select 1 as one', {
        rawResults: true
      });
      const { rows } = await s.step();
      const columns = await s.getColumns();

      expect(columns).toEqual(['one']);
      expect(rows).toEqual([[1]]);
    });

    test('big number', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select 9223372036854775807 as bignumber');
      const { rows } = await s.step();

      expect(rows).toEqual([{ bignumber: 9223372036854776000 }]);

      using s2 = connection.prepare('select ? as bignumber');
      s2.bind([9223372036854775807n]);
      const { rows: rows2 } = await s2.step();

      expect(rows2).toEqual([{ bignumber: 9223372036854776000 }]);
    });

    test('bigint', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select ? as bignumber', { bigint: true });
      s.bind([9223372036854775807n]);
      const { rows: rows1 } = await s.step();
      expect(rows1).toEqual([{ bignumber: 9223372036854775807n }]);

      using s2 = connection.prepare('select 9223372036854775807 as bignumber', {
        bigint: true
      });
      const { rows: rows2 } = await s2.step();
      expect(rows2).toEqual([{ bignumber: 9223372036854775807n }]);
    });

    test('insert returning', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s1 = connection.prepare(
        'create table test_data(id integer primary key, data text)'
      );
      await s1.step();
      using s2 = connection.prepare(
        'insert into test_data(data) values(123) returning id'
      );
      const columns = await s2.getColumns();
      const { rows } = await s2.step();

      expect(columns).toEqual(['id']);
      expect(rows).toEqual([{ id: 1 }]);
    });

    test('bind named args', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = await connection.prepare('select :one as one, :two as two');
      s.bind({ one: 1, two: 2 });
      const { rows } = await s.step();
      expect(rows).toEqual([{ one: 1, two: 2 }]);
    });

    test.skip('skip named arg', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
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

      const [, , { rows }, { error }] = results;

      expect(error).toBe(undefined);
      expect(rows).toEqual([[null, 2]]);
    });

    test('rebind arg', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
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

      const [, , , { rows }, { error }] = results;

      expect(error).toBe(undefined);
      expect(rows).toEqual([[11, 22]]);
    });

    test('partial rebind', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
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

      const [, , , { rows }, { error }] = results;

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1, 22]]);
    });

    test('positional parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
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

      const [, , { rows }, { error }] = results;

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1, 2]]);
    });

    test('positional specific parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
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

      const [, , { rows }, { error }] = results;

      expect(error).toBe(undefined);
      expect(rows).toEqual([[2, 1]]);
    });

    test('positional parameters partial rebind', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
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

      const [, , , { rows }, { error }] = results;

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1, 22]]);
    });

    test('named and positional parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
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

      const [, , , { rows }, { error }] = results;

      expect(error).toBe(undefined);
      expect(rows).toEqual([[1, 3, 2]]);
    });

    test('reset parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
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
      await using connection = await driver.reserveConnection();
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
        { rows: rows4, skipped: skipped4 },
        ,
        { rows: rows5 },
        { error }
      ] = results as any[];

      expect(error).toBe(undefined);
      expect(rows1).toEqual([[1], [2], [3]]);
      expect(rows2).toEqual([[1], [2], [3]]);
      expect(rows3).toEqual([[4], [5]]);
      expect(rows4).toBe(undefined);
      expect(skipped4).toBe(true);
      expect(rows5).toEqual([[1], [2], [3], [4], [5]]);
    });

    test('multiple insert step', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
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
        { rows: rows2, skipped: skipped2 },
        ,
        { rows: rows3 },
        ,
        { rows: rows4 },
        { error }
      ] = results as any[];

      expect(error).toBe(undefined);
      expect(rows1).toEqual([]);
      expect(rows2).toBe(undefined);
      expect(skipped2).toBe(true);
      expect(rows3).toEqual([]);
      expect(rows4).toEqual([[2]]);
    });

    test('error handling - prepare', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: 'select foobar'
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

      const [{ columns, error: error1 }, { rows }, { error: error2 }] = results;

      expect(rows).toBe(undefined);
      expect(error1).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'no such column: foobar'
      });
      expect(error2).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'no such column: foobar'
      });
    });

    test('error handling - step', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 0,
            sql: "select json_each.value from json_each('test')"
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
        { columns, error: error1 },
        { rows, error: error2 },
        { error: error3 }
      ] = results;

      expect(rows).toBe(undefined);
      expect(columns).toEqual(['value']);
      expect(error1).toBe(undefined);
      expect(error2).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'malformed JSON'
      });
      expect(error3).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'malformed JSON'
      });
    });

    test('error recovery', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      const results = await connection.execute([
        {
          prepare: {
            id: 1,
            sql: 'select json_each.value from json_each(?)'
          }
        },
        {
          bind: {
            id: 1,
            parameters: ['test']
          }
        },
        {
          step: {
            id: 1,
            all: true
          }
        },
        {
          step: {
            id: 1,
            all: true
          }
        },
        { sync: {} },
        {
          step: {
            id: 1,
            all: true
          }
        },
        {
          reset: {
            id: 1
          }
        },
        {
          bind: {
            id: 1,
            parameters: ['["test"]']
          }
        },
        {
          step: {
            id: 1,
            all: true
          }
        },
        {
          sync: {}
        },
        {
          finalize: {
            id: 1
          }
        }
      ]);

      const [
        { columns }, // prepare
        ,
        // bind
        { error: error1 }, // step
        { skipped: skip1 }, // step
        { error: error2 }, // sync
        { rows: rows1 }, // step
        ,
        ,
        // reset
        // bind
        { rows: rows2 }, // step
        // sync
        // finalize
        ,
      ] = results;

      expect(error1).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'malformed JSON'
      });
      expect(skip1).toBe(true);
      expect(error2).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'malformed JSON'
      });
      expect(rows1).toEqual([]);
      expect(rows2).toEqual([['test']]);
    });

    test.skip('onUpdate', async () => {
      // Skipped: Not properly implemented yet.

      await using driver = await open();
      await using connection = await driver.reserveConnection();
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
