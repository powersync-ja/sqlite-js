import * as fs from "node:fs/promises";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { SqliteDriverConnectionPool } from "../../src/driver-api.js";

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
        .currentTestName!.replaceAll(/[\s\/\\>\.\-]+/g, "_");
      dbPath = `test-db/${testNameSanitized}.db`;
    });

    afterEach(async () => {
      for (let db of _dbs) {
        await db.close();
      }
      _dbs = [];
    });

    test("basic select", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        const rs = await connection.prepare("select 1 as one").selectAll();
        expect(rs.columns).toEqual(["one"]);
        expect(rs.rows).toEqual([[1]]);
      } finally {
        release();
      }
    });

    test("big number", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        const rs = await connection
          .prepare("select 9223372036854775807 as bignumber")
          .selectAll();
        expect(rs.rows).toEqual([[9223372036854776000]]);

        const rs2 = await connection
          .prepare("select ? as bignumber")
          .selectAll([9223372036854775807n]);
        expect(rs2.rows).toEqual([[9223372036854776000]]);
      } finally {
        release();
      }
    });

    test("bigint", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        const rs = await connection
          .prepare("select 9223372036854775807 as bignumber")
          .selectAll(undefined, { bigint: true });
        expect(rs.columns).toEqual(["bignumber"]);
        expect(rs.rows).toEqual([[9223372036854775807n]]);

        const rs2 = await connection
          .prepare("select ? as bignumber")
          .selectAll([9223372036854775807n], { bigint: true });
        expect(rs2.rows).toEqual([[9223372036854775807n]]);
      } finally {
        release();
      }
    });

    test("insert returning", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        await connection
          .prepare("create table test_data(id integer primary key, data text)")
          .run();

        const rs = await connection
          .prepare("insert into test_data(data) values(123) returning id")
          .selectAll();
        expect(rs.columns).toEqual(["id"]);
        expect(rs.rows).toEqual([[1]]);
      } finally {
        release();
      }
    });

    test("runWithResults", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        const r1 = await connection
          .prepare("create table test_data(id integer primary key, data text)")
          .runWithResults();

        expect(r1).toEqual({ changes: 0, lastInsertRowId: 0n });
        const r2 = await connection
          .prepare("insert into test_data(data) values(123) returning id")
          .runWithResults();
        expect(r2).toEqual({
          changes: 1,
          lastInsertRowId: 1n,
        });
      } finally {
        release();
      }
    });

    test("runWithResults - select", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        const r1 = await connection.prepare("select 1 as one").runWithResults();

        expect(r1).toEqual({ changes: 0, lastInsertRowId: 0n });
      } finally {
        release();
      }
    });
  });
}
