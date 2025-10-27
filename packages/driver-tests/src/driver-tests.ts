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

    test.skipIf(!features.rawResults)('basic select - array', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select 1 as one');
      const rows = await s.allArray();

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
      const rows = await s.all();
      expect(rows).toEqual([{ one: 1 }]);
    });

    test('big number', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select 9223372036854775807 as bignumber');
      const rows = await s.all();

      expect(rows).toEqual([{ bignumber: 9223372036854776000 }]);

      using s2 = connection.prepare('select ? as bignumber');
      const rows2 = await s2.all([9223372036854775807n]);

      expect(rows2).toEqual([{ bignumber: 9223372036854776000 }]);
    });

    test('bigint', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select ? as bignumber');
      const rows1 = await s.all([9223372036854775807n], { bigint: true });
      expect(rows1).toEqual([{ bignumber: 9223372036854775807n }]);

      using s2 = connection.prepare(
        'select 9223372036854775807 as bignumber',
        {}
      );
      const rows2 = await s2.all(undefined, { bigint: true });
      expect(rows2).toEqual([{ bignumber: 9223372036854775807n }]);
    });

    test('insert returning', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s1 = connection.prepare(
        'create table test_data(id integer primary key, data text)'
      );
      await s1.run();
      using s2 = connection.prepare(
        'insert into test_data(data) values(123) returning id'
      );
      const rows = await s2.all();

      expect(rows).toEqual([{ id: 1 }]);

      if (features.getColumns) {
        const columns = await s2.getColumns();
        expect(columns).toEqual(['id']);
      }
    });

    test('bind named args', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select :one as one, :two as two');
      const rows = await s.all({ one: 1, two: 2 });
      expect(rows).toEqual([{ one: 1, two: 2 }]);
    });

    test('bind named args - explicit names', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select $one as one, $two as two');
      const rows = await s.all({ $one: 1, $two: 2 });
      expect(rows).toEqual([{ one: 1, two: 2 }]);
    });

    test.skipIf(!features.allowsMissingParameters)(
      'skip named arg',
      async () => {
        await using driver = await open();
        await using connection = await driver.reserveConnection();
        using s = connection.prepare('select :one as one, :two as two');

        const rows = await s.all({ two: 2 });
        expect(rows).toEqual([{ one: null, two: 2 }]);
      }
    );

    test('positional parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select ? as one, ? as two');
      const rows = await s.all([1, 2]);
      expect(rows).toEqual([{ one: 1, two: 2 }]);
    });

    test('positional specific parameters', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select ?2 as two, ?1 as one');
      const rows = await s.all({ '1': 1, '2': 2 });
      expect(rows).toEqual([{ two: 2, one: 1 }]);
    });

    test.skip('named and positional parameters', async () => {
      // TODO: Specify the behavior for this
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare(
        'select ? as one, @three as three, ? as two'
      );
      const rows = await s.all({ 1: 1, 2: 2, three: 3 });
      expect(rows).toEqual([{ one: 1, three: 3, two: 2 }]);
    });

    test.skipIf(!features.allowsMissingParameters)(
      'reset parameters',
      async () => {
        await using driver = await open();
        await using connection = await driver.reserveConnection();
        using s = connection.prepare('select ? as one, ? as two');
        const rows1 = await s.all([1, 2]);

        expect(rows1).toEqual([{ one: 1, two: 2 }]);

        const rows2 = await s.all();
        expect(rows2).toEqual([{ one: null, two: null }]);
      }
    );

    test('error handling - prepare', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare('select foobar');
      expect(await s.getColumns().catch((e) => e)).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'no such column: foobar'
      });
      expect(await s.all().catch((e) => e)).toMatchObject({
        code: 'SQLITE_ERROR',
        message: 'no such column: foobar'
      });
    });

    test('error handling - query', async () => {
      await using driver = await open();
      await using connection = await driver.reserveConnection();
      using s = connection.prepare(
        "select json_each.value from json_each('test')"
      );
      if (features.getColumns) {
        expect(await s.getColumns()).toEqual(['value']);
      }
      expect(await s.all().catch((e) => e)).toMatchObject({
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
