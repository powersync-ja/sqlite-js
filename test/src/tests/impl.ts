import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';
import { SqliteDriverConnectionPool } from '../../../lib/driver-api.js';
import { ConnectionPoolImpl } from '../../../lib/impl.js';

export function describeImplTests(
  name: string,
  factory: (path: string) => ConnectionPoolImpl
) {
  describe(`${name} - api tests`, () => {
    let dbPath: string;
    let _dbs: ConnectionPoolImpl[] = [];

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
      const results = await connection.select('select 1 as one');
      expect(results).toEqual([{ one: 1 }]);
    });

    test('big number', async () => {
      const driver = await open();
      using connection = await driver.reserveConnection();
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
      const driver = await open();
      using connection = await driver.reserveConnection();

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
      const driver = await open();
      using connection = await driver.reserveConnection();
      await connection.execute(
        'create table test_data(id integer primary key, data text)'
      );
      const results = await connection.execute(
        'insert into test_data(data) values(123) returning id'
      );

      expect(results.rows).toEqual([{ id: 1 }]);
    });

    test('runWithResults', async () => {
      const driver = await open();
      using connection = await driver.reserveConnection();
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
      const driver = await open();
      using connection = await driver.reserveConnection();
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
      const driver = await open();
      using connection = await driver.reserveConnection();
      const results = await connection.execute('select 1 as one', undefined, {
        includeChanges: true,
        includeRowId: true
      });

      expect(results.rows).toEqual([{ one: 1 }]);
      expect(results.changes).toEqual(0);
      expect(results.rowId).toEqual(0);
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
