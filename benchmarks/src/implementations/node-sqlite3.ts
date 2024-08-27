import { Benchmark } from '../Benchmark.js';
import { join } from 'path';
import { promises as fs } from 'fs';
import Prando from 'prando';
import assert from 'node:assert';
import { numberName } from '../util.js';
import sqlite3 from 'sqlite3';
import { open, Database } from 'sqlite';

export class NodeSqlite3Impl extends Benchmark {
  private db!: Database;
  private dir: string;
  private random = new Prando.default(0);

  constructor(
    public name: string,
    dir: string
  ) {
    super();
    this.dir = dir;
  }

  async setUp(): Promise<void> {
    const dbPath = join(this.dir, this.name + '.db');

    try {
      await fs.unlink(dbPath);
      await fs.unlink(dbPath + '-wal');
    } catch (e) {
      // Ignore
    }

    const db = await open({ driver: sqlite3.Database, filename: dbPath });
    this.db = db;
    await db.exec('PRAGMA journal_mode = WAL');
    await db.exec('PRAGMA synchronous = normal');

    await db.exec(
      'CREATE TABLE t1(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)'
    );
    await db.exec(
      'CREATE TABLE t2(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)'
    );
    await db.exec(
      'CREATE TABLE t3(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)'
    );
    await db.exec('CREATE INDEX i3a ON t3(a)');
    await db.exec('CREATE INDEX i3b ON t3(b)');
  }

  async tearDown(): Promise<void> {
    await this.db.close();
  }

  // Test 1: 1000 INSERTs
  async test1(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare('INSERT INTO t1(a, b, c) VALUES(?, ?, ?)');
    try {
      for (let i = 0; i < 1000; i++) {
        const n = this.random.nextInt(0, 100000);
        await stmt.run(i + 1, n, numberName(n));
      }
    } finally {
      await stmt.finalize();
    }
    // await db.run('PRAGMA wal_checkpoint(RESTART)');
    const total = await db.get<{ count: number }>(
      'select count() as count from t1'
    );
    assert(total!.count == 1000);
  }

  // Test 2: 25000 INSERTs in a transaction
  async test2(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare('INSERT INTO t2(a, b, c) VALUES(?, ?, ?)');
    try {
      await this.transaction(async () => {
        for (let i = 0; i < 25000; i++) {
          const n = this.random.nextInt(0, 100000);
          await stmt.run(i + 1, n, numberName(n));
        }
      });
      await db.exec('PRAGMA wal_checkpoint(RESTART)');
      const total = await db.get<{ count: number }>(
        'select count() as count from t2'
      );
      assert(total!.count == 25000);
    } finally {
      await stmt.finalize();
    }
  }

  // Test 3: 25000 INSERTs into an indexed table
  async test3(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare('INSERT INTO t3(a, b, c) VALUES(?, ?, ?)');
    try {
      await this.transaction(async () => {
        for (let i = 0; i < 25000; i++) {
          const n = this.random.nextInt(0, 100000);
          await stmt.run([i + 1, n, numberName(n)]);
        }
      });
    } finally {
      await stmt.finalize();
    }
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 4: 100 SELECTs without an index
  async test4(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare(
      'SELECT count(*) count, avg(b) avg FROM t2 WHERE b>=? AND b<?'
    );
    await this.transaction(async () => {
      for (let i = 0; i < 100; i++) {
        const row = await stmt.get<{ count: number; avg: number }>([
          i * 100,
          i * 100 + 1000
        ]);

        assert(row!.count > 200);
        assert(row!.count < 300);
        assert(row!.avg > i * 100);
        assert(row!.avg < i * 100 + 1000);
      }
    });
    await stmt.finalize();
  }

  // Test 5: 100 SELECTs on a string comparison
  async test5(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare(
      'SELECT count(*) count, avg(b) avg FROM t2 WHERE c LIKE ?'
    );
    await this.transaction(async () => {
      for (let i = 0; i < 100; i++) {
        const row = await stmt.get<{ count: number; avg: number }>([
          `%${numberName(i + 1)}%`
        ]);
        assert(row!.count > 400);
        assert(row!.count < 12000);
        assert(row!.avg > 25000);
      }
    });
    await stmt.finalize();
  }

  // Test 7: 5000 SELECTs with an index
  async test7(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare(
      'SELECT count(*) count, avg(b) avg FROM t3 WHERE b>=? AND b<?'
    );
    await this.transaction(async () => {
      for (let i = 0; i < 5000; i++) {
        const row = await stmt.get<{ count: number; avg: number }>([
          i * 100,
          i * 100 + 100
        ]);
        if (i < 1000) {
          assert(row!.count > 8);
          assert(row!.count < 100);
        } else {
          assert(row!.count === 0);
        }
      }
    });
    await stmt.finalize();
  }

  // Test 8: 1000 UPDATEs without an index
  async test8(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare('UPDATE t1 SET b=b*2 WHERE a>=? AND a<?');
    await this.transaction(async () => {
      for (let i = 0; i < 1000; i++) {
        await stmt.run([i * 10, i * 10 + 10]);
      }
    });
    await stmt.finalize();
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 9: 25000 UPDATEs with an index
  async test9(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare('UPDATE t3 SET b=? WHERE a=?');
    await this.transaction(async () => {
      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        await stmt.run([n, i + 1]);
      }
    });
    await stmt.finalize();
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 10: 25000 text UPDATEs with an index
  async test10(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare('UPDATE t3 SET c=? WHERE a=?');
    await this.transaction(async () => {
      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        await stmt.run([numberName(n), i + 1]);
      }
    });
    await stmt.finalize();
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 11: INSERTs from a SELECT
  async test11(): Promise<void> {
    const db = this.db;
    await this.transaction(async () => {
      await db.exec('INSERT INTO t1(a, b, c) SELECT b,a,c FROM t3');
      await db.exec('INSERT INTO t3(a, b, c) SELECT b,a,c FROM t1');
    });
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 12: DELETE without an index
  async test12(): Promise<void> {
    const db = this.db;
    await db.exec("DELETE FROM t3 WHERE c LIKE '%fifty%'");
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 13: DELETE with an index
  async test13(): Promise<void> {
    const db = this.db;
    await db.exec('DELETE FROM t3 WHERE a>10 AND a<20000');
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 14: A big INSERT after a big DELETE
  async test14(): Promise<void> {
    const db = this.db;
    await db.exec('INSERT INTO t3(a, b, c) SELECT a, b, c FROM t1');
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 15: A big DELETE followed by many small INSERTs
  async test15(): Promise<void> {
    const db = this.db;
    const stmt = await db.prepare('INSERT INTO t1(a, b, c) VALUES(?, ?, ?)');
    await this.transaction(async () => {
      await db.run('DELETE FROM t1');
      for (let i = 0; i < 12000; i++) {
        const n = this.random.nextInt(0, 100000);
        await stmt.run([i + 1, n, numberName(n)]);
      }
    });
    await stmt.finalize();
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 16: Clear table
  async test16(): Promise<void> {
    const db = this.db;
    const row1 = await db.get<{ count: number }>(
      'SELECT count() count FROM t1'
    );
    const row2 = await db.get<{ count: number }>(
      'SELECT count() count FROM t2'
    );
    const row3 = await db.get<{ count: number }>(
      'SELECT count() count FROM t3'
    );
    assert(row1!.count === 12000);
    assert(row2!.count === 25000);
    assert(row3!.count > 34000);
    assert(row3!.count < 36000);

    await db.exec('DELETE FROM t1');
    await db.exec('DELETE FROM t2');
    await db.exec('DELETE FROM t3');
    await db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  async transaction(callback: () => Promise<void>) {
    await this.db.exec('begin');
    try {
      await callback();
      await this.db.exec('commit');
    } catch (e) {
      try {
        await this.db.exec('rollback');
      } catch (e2) {}
      throw e;
    }
  }
}
