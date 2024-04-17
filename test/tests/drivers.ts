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
      const closeDbs = _dbs;
      _dbs = [];
      for (let db of closeDbs) {
        await db.close();
      }
    });

    test("basic select", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        const rs = await connection.selectAll("select 1 as one");
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
        const rs = await connection.selectAll(
          "select 9223372036854775807 as bignumber"
        );
        expect(rs.rows).toEqual([[9223372036854776000]]);
        const rs2 = await connection.selectAll("select ? as bignumber", [
          9223372036854775807n,
        ]);
        expect(rs2.rows).toEqual([[9223372036854776000]]);
      } finally {
        release();
      }
    });

    test("bigint", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        const rs1 = await connection.selectAll(
          "select 9223372036854775807 as bignumber",
          undefined,
          { bigint: true }
        );
        expect(rs1.columns).toEqual(["bignumber"]);
        expect(rs1.rows).toEqual([[9223372036854775807n]]);

        const rs2 = await connection.selectAll(
          "select ? as bignumber",
          [9223372036854775807n],
          {
            bigint: true,
          }
        );

        expect(rs2.rows).toEqual([[9223372036854775807n]]);
      } finally {
        release();
      }
    });

    test("insert returning", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        await connection.run(
          "create table test_data(id integer primary key, data text)"
        );

        const rs = await connection.selectAll(
          "insert into test_data(data) values(123) returning id"
        );
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
        const r1 = await connection.runWithResults(
          "create table test_data(id integer primary key, data text)"
        );

        expect(r1).toEqual({ changes: 0, lastInsertRowId: 0n });
        const r2 = await connection.runWithResults(
          "insert into test_data(data) values(123)"
        );
        expect(r2).toEqual({
          changes: 1,
          lastInsertRowId: 1n,
        });
      } finally {
        release();
      }
    });

    test("runWithResults - returning statement", async () => {
      const driver = await open();
      const { connection, release } = await driver.reserveConnection();
      try {
        const r1 = await connection.runWithResults(
          "create table test_data(id integer primary key, data text)"
        );

        expect(r1).toEqual({ changes: 0, lastInsertRowId: 0n });
        const r2 = await connection.runWithResults(
          "insert into test_data(data) values(123) returning id"
        );
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
        const r1 = await connection.runWithResults("select 1 as one");

        expect(r1).toEqual({ changes: 0, lastInsertRowId: 0n });
      } finally {
        release();
      }
    });
  });
}
