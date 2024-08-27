import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { beforeEach, describe, test } from './test.js';
import { expect } from 'expect';
import { SqliteDriverConnectionPool } from '@sqlite-js/driver';

export interface DriverFeatures {
  getColumns: boolean;
  rawResults: boolean;
  allowsMissingParameters: boolean;
}

export function describeDriverTests(
  name: string,
  features: DriverFeatures,
  factory: (path: string) => SqliteDriverConnectionPool
) {
  describe(`${name} - driver tests`, () => {
    let dbPath: string;

    const open = async () => {
      // const dir = path.dirname(dbPath);
      // try {
      //   await fs.mkdir(dir);
      // } catch (e) {}
      // try {
      //   await fs.rm(dbPath);
      // } catch (e) {}
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

    test.skipIf(!features.rawResults)('basic select - raw', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select 1 as one', {
        rawResults: true
      });
      const { rows } = await s.step();

      expect(rows).toEqual([[1]]);

      if (features.getColumns) {
        const columns = await s.getColumns();
        expect(columns).toEqual(['one']);
      }
    });

    test('basic select - object', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select 1 as one');
      const { rows } = await s.step();
      expect(rows).toEqual([{ one: 1 }]);
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
      const { rows } = await s2.step();

      expect(rows).toEqual([{ id: 1 }]);

      expect(await connection.connection.getLastChanges()).toEqual({
        changes: 1,
        lastInsertRowId: 1n
      });

      if (features.getColumns) {
        const columns = await s2.getColumns();
        expect(columns).toEqual(['id']);
      }
    });

    test('bind named args', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select :one as one, :two as two');
      s.bind({ one: 1, two: 2 });
      const { rows } = await s.step();
      expect(rows).toEqual([{ one: 1, two: 2 }]);
    });

    test('bind named args - explicit names', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select $one as one, $two as two');
      s.bind({ $one: 1, $two: 2 });
      const { rows } = await s.step();
      expect(rows).toEqual([{ one: 1, two: 2 }]);
    });

    test.skipIf(!features.allowsMissingParameters)(
      'skip named arg',
      async () => {
        await using driver = await open();
        await using connection = await driver.reserveConnection();
        using s = connection.prepare('select :one as one, :two as two');
        s.bind({ two: 2 });

        const { rows } = await s.step();
        expect(rows).toEqual([{ one: null, two: 2 }]);
      }
    );

    test('rebind arg', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select :one as one, :two as two');
      s.bind({ one: 1, two: 2 });
      s.bind({ one: 11, two: 22 });
      const { rows } = await s.step();
      expect(rows).toEqual([{ one: 11, two: 22 }]);
    });

    test('partial rebind', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select :one as one, :two as two');
      s.bind({ one: 1, two: 2 });
      s.bind({ two: 22 });
      const { rows } = await s.step();
      expect(rows).toEqual([{ one: 1, two: 22 }]);
    });

    test('positional parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select ? as one, ? as two');
      s.bind([1, 2]);
      const { rows } = await s.step();
      expect(rows).toEqual([{ one: 1, two: 2 }]);
    });

    test('positional specific parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select ?2 as two, ?1 as one');
      s.bind({ '1': 1, '2': 2 });
      const { rows } = await s.step();
      expect(rows).toEqual([{ two: 2, one: 1 }]);
    });

    test('positional parameters partial rebind', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select ? as one, ? as two');
      s.bind([1, 2]);
      s.bind([undefined, 22]);
      const { rows } = await s.step();
      expect(rows).toEqual([{ one: 1, two: 22 }]);
    });

    test('named and positional parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare(
        'select ? as one, @three as three, ? as two'
      );
      s.bind([1, 2]);
      s.bind({ three: 3 });
      const { rows } = await s.step();
      expect(rows).toEqual([{ one: 1, three: 3, two: 2 }]);
    });

    test('reset parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select ? as one, ? as two');
      s.bind([1, 2]);
      const { rows: rows1 } = await s.step();
      s.reset();
      const { rows: rows2 } = await s.step();
      s.reset({ clearBindings: true });

      expect(rows1).toEqual([{ one: 1, two: 2 }]);
      expect(rows2).toEqual([{ one: 1, two: 2 }]);

      if (features.allowsMissingParameters) {
        const { rows: rows3 } = await s.step();
        expect(rows3).toEqual([{ one: null, two: null }]);
      }
    });

    test('partial reset', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare(
        "select json_each.value as v from json_each('[1,2,3,4,5]')"
      );
      const { rows: rows1 } = await s.step(3);
      s.reset();
      const { rows: rows2 } = await s.step(3);
      const { rows: rows3 } = await s.step(3);
      const { rows: rows4 } = await s.step(3);
      s.reset();
      const { rows: rows5 } = await s.step();

      expect(rows1).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }]);
      expect(rows2).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }]);
      expect(rows3).toEqual([{ v: 4 }, { v: 5 }]);
      expect(rows4).toBe(undefined);
      expect(rows5).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }, { v: 5 }]);
    });

    test('multiple insert step', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();

      using s1 = connection.prepare(
        'create table test_data(id integer primary key, data text)'
      );
      await s1.step();
      using s2 = connection.prepare(
        "insert into test_data(data) values('test')"
      );
      const { rows: rows1 } = await s2.step();
      const { rows: rows2 } = await s2.step();
      s2.reset();
      const { rows: rows3 } = await s2.step();
      using s3 = connection.prepare('select count(*) as count from test_data');
      const { rows: rows4 } = await s3.step();

      expect(rows1).toEqual([]);
      expect(rows2).toBe(undefined);
      expect(rows3).toEqual([]);
      expect(rows4).toEqual([{ count: 2 }]);
    });

    test('error handling - prepare', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select foobar');
      expect(await s.getColumns().catch((e) => e)).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'no such column: foobar'
      });
      expect(await s.step().catch((e) => e)).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'no such column: foobar'
      });
    });

    test('error handling - step', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare(
        "select json_each.value from json_each('test')"
      );
      if (features.getColumns) {
        expect(await s.getColumns()).toEqual(['value']);
      }
      expect(await s.step().catch((e) => e)).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'malformed JSON'
      });
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
