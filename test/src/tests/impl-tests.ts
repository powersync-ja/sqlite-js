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
      await connection.select(
        'create table test_data(id integer primary key, data text)'
      );
      const results = await connection.select(
        'insert into test_data(data) values(123) returning id'
      );

      expect(results).toEqual([{ id: 1 }]);
    });

    test('run', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();
      await connection.run(
        'create table test_data(id integer primary key, data text)'
      );
      const results = await connection.run(
        'insert into test_data(data) values(123)'
      );

      expect(results.changes).toEqual(1);
      expect(results.lastInsertRowId).toEqual(1n);
    });

    test('run - select', async () => {
      await using db = await open();
      await using connection = await db.reserveConnection();
      const results = await connection.run('select 1 as one');

      expect(results.changes).toEqual(0);
      expect(results.lastInsertRowId).toEqual(0n);
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

    test('begin', async () => {
      await using db = await open();
      {
        await using tx = await db.begin();
        const results1 = await tx.select('select 1 as one');
        expect(results1).toEqual([{ one: 1 }]);
        await tx.commit();
      }
      {
        await using tx = await db.begin();
        const results1 = await tx.select('select 1 as one');
        expect(results1).toEqual([{ one: 1 }]);
        await tx.commit();
      }
      {
        await using tx = await db.begin();
        const results = await tx.select('select 1 as one');
        expect(results).toEqual([{ one: 1 }]);
        await tx.commit();
      }
    });

    test('begin - explicit commit in sequence', async () => {
      await using db = await open();
      {
        await using tx1 = await db.begin();
        const results1 = await tx1.select('select 1 as one');
        expect(results1).toEqual([{ one: 1 }]);
        await tx1.commit();
        await using tx2 = await db.begin();
        const results2 = await tx2.select('select 1 as one');
        expect(results2).toEqual([{ one: 1 }]);
        await tx2.commit();
        await using tx3 = await db.begin();
        const results3 = await tx3.select('select 1 as one');
        expect(results3).toEqual([{ one: 1 }]);
        await tx3.commit();
      }
    });

    test('begin - error when not using asyncDispose', async () => {
      await using db = await open();
      const tx = await db.begin();
      try {
        await expect(() => tx.select('select 1 as one')).rejects.toMatchObject({
          message: expect.stringContaining('dispose handler is not registered')
        });
      } finally {
        // Just for the test itself
        await tx.dispose();
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
