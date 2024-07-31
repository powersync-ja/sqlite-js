import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { beforeEach, describe, test } from './test.js';
import { expect } from 'expect';
import { ConnectionPoolImpl } from '../../../lib/impl.js';

export function describeImplTests(
  name: string,
  factory: (path: string) => ConnectionPoolImpl
) {
  describe(`${name} - api tests`, () => {
    let dbPath: string;

    const open = async () => {
      const dir = path.dirname(dbPath);
      try {
        await fs.mkdir(dir);
      } catch (e) {}
      try {
        await fs.rm(dbPath);
      } catch (e) {}
      return factory(dbPath);
    };

    beforeEach((context) => {
      const testNameSanitized = context.fullName.replaceAll(
        /[\s\/\\>\.\-]+/g,
        '_'
      );
      dbPath = `test-db/${testNameSanitized}.db`;
    });

    test('basic select', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();
      const results = await connection.select('select 1 as one');
      expect(results).toEqual([{ one: 1 }]);
    });

    test('big number', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();
      const results = await connection.select(
        'select 9223372036854775807 as bignumber'
      );

      expect(results).toEqual([{ bignumber: 9223372036854776000 }]);
      const results2 = await connection.select('select ? as bignumber', [
        9223372036854775807n
      ]);

      expect(results2).toEqual([{ bignumber: 9223372036854776000 }]);
    });

    test('bigint', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();

      const rows1 = await connection.select(
        'select ? as bignumber',
        [9223372036854775807n],
        { bigint: true }
      );
      expect(rows1).toEqual([{ bignumber: 9223372036854775807n }]);

      const rows2 = await connection.select(
        'select 9223372036854775807 as bignumber',
        undefined,
        { bigint: true }
      );

      expect(rows2).toEqual([{ bignumber: 9223372036854775807n }]);
    });

    test('insert returning', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();
      await connection.execute(
        'create table test_data(id integer primary key, data text)'
      );
      const results = await connection.execute(
        'insert into test_data(data) values(123) returning id'
      );

      expect(results.rows).toEqual([{ id: 1 }]);
    });

    test('runWithResults', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();
      await connection.execute(
        'create table test_data(id integer primary key, data text)'
      );
      const results = await connection.execute(
        'insert into test_data(data) values(123)',
        undefined,
        { includeChanges: true, includeRowId: true }
      );

      expect(results.rows).toEqual([]);
      expect(results.changes).toEqual(1);
      expect(results.rowId).toEqual(1);
    });

    test('runWithResults - returning statement', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();
      await connection.execute(
        'create table test_data(id integer primary key, data text)'
      );
      const results = await connection.execute(
        'insert into test_data(data) values(123) returning id',
        undefined,
        { includeChanges: true, includeRowId: true }
      );

      expect(results.rows).toEqual([{ id: 1 }]);
      expect(results.changes).toEqual(1);
      expect(results.rowId).toEqual(1);
    });

    test('runWithResults - select', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();
      const results = await connection.execute('select 1 as one', undefined, {
        includeChanges: true,
        includeRowId: true
      });

      expect(results.rows).toEqual([{ one: 1 }]);
      expect(results.changes).toEqual(0);
      expect(results.rowId).toEqual(0);
    });

    test('transaction', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();
      const results1 = await connection.transaction(async () => {
        return await connection.select('select 1 as one');
      });
      const results2 = await connection.transaction(async () => {
        return await connection.select('select 1 as one');
      });

      expect(results1).toEqual([{ one: 1 }]);
      expect(results2).toEqual([{ one: 1 }]);
    });

    test('usingTransaction', async () => {
      await using db = await open();
      {
        await using tx = await db.usingTransaction();
        const results1 = await tx.select('select 1 as one');
        expect(results1).toEqual([{ one: 1 }]);
        await tx.commit();
      }
      {
        await using tx = await db.usingTransaction();
        const results1 = await tx.select('select 1 as one');
        expect(results1).toEqual([{ one: 1 }]);
        await tx.commit();
      }
      {
        await using tx = await db.usingTransaction();
        const results = await tx.select('select 1 as one');
        expect(results).toEqual([{ one: 1 }]);
        await tx.commit();
      }
    });

    test.skip('onUpdate', async () => {
      // Skipped: Not properly implemented yet.

      await using db = await open();
      await using connection = await db.reserveConnection();
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
