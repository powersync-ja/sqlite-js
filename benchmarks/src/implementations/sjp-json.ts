import { promises as fs } from 'fs';
import assert from 'node:assert';
import { join } from 'path';
import Prando from 'prando';
import { SqliteConnectionPool } from '../../../lib/index.js';
import { Benchmark } from '../Benchmark.js';
import { numberName } from '../util.js';

export class JSPJsonImpl extends Benchmark {
  private db!: SqliteConnectionPool;
  private dir: string;
  private random = new Prando.default(0);

  constructor(
    public name: string,
    dir: string,
    private driver: (path: string) => SqliteConnectionPool
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

    const db = this.driver(dbPath);
    this.db = db;

    await using c = await db.reserveConnection();

    await c.run(
      'CREATE TABLE t1(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)'
    );
    await c.run(
      'CREATE TABLE t2(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)'
    );
    await c.run(
      'CREATE TABLE t3(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)'
    );
    await c.run('CREATE INDEX i3a ON t3(a)');
    await c.run('CREATE INDEX i3b ON t3(b)');

    let promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(
        (async () => {
          await using c = await db.reserveConnection({ readonly: true });
          await new Promise((resolve) => setTimeout(resolve, 1));
        })()
      );
    }
    await Promise.all(promises);
  }

  async tearDown(): Promise<void> {
    await this.db.close();
  }

  // Test 1: 1000 INSERTs
  async test1(): Promise<void> {
    await using db = await this.db.reserveConnection();
    using s = db.prepare('INSERT INTO t1(a, b, c) VALUES(?, ?, ?)');

    for (let i = 0; i < 1000; i++) {
      const n = this.random.nextInt(0, 100000);
      await s.run([i + 1, n, numberName(n)]);
    }
    await db.run('PRAGMA wal_checkpoint(RESTART)');
    const total = (
      await db.select<{ count: number }>('select count() as count from t1')
    )[0];
    assert(total.count == 1000);
  }

  // Test 2: 25000 INSERTs in a transaction
  async test2(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.transaction(async (tx) => {
      using s = tx.prepare(
        'INSERT INTO t2(a, b, c) SELECT e.value ->> 0, e.value ->> 1, e.value ->> 2 from json_each(?) e'
      );
      let buffer: any[][] = [];

      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        buffer.push([i + 1, n, numberName(n)]);

        if (buffer.length >= 100) {
          await s.run([JSON.stringify(buffer)]);
          buffer = [];
        }
      }
      await s.run([JSON.stringify(buffer)]);
    });
    await db.run('PRAGMA wal_checkpoint(RESTART)');
    const total = (
      await db.select<{ count: number }>('select count() as count from t2')
    )[0];
    assert(total.count == 25000);
  }

  // Test 3: 25000 INSERTs into an indexed table
  async test3(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.transaction(async (tx) => {
      using s = tx.prepare(
        'INSERT INTO t3(a, b, c) SELECT e.value ->> 0, e.value ->> 1, e.value ->> 2 from json_each(?) e'
      );
      let buffer: any[][] = [];

      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        buffer.push([i + 1, n, numberName(n)]);
        if (buffer.length >= 100) {
          await s.run([JSON.stringify(buffer)]);
          buffer = [];
        }
      }
      await s.run([JSON.stringify(buffer)]);
    });
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 4: 100 SELECTs without an index
  async test4(): Promise<void> {
    await this.db.transaction(
      async (tx) => {
        using s = tx.prepare<{ count: number; avg: number }>(
          'SELECT count(*) count, avg(b) avg FROM t2 WHERE b>=? AND b<?'
        );
        for (let i = 0; i < 100; i++) {
          const row = (await s.select([i * 100, i * 100 + 1000]))[0];
          assert(row.count > 200);
          assert(row.count < 300);
          assert(row.avg > i * 100);
          assert(row.avg < i * 100 + 1000);
        }
      },
      { readonly: true }
    );
  }

  // Test 5: 100 SELECTs on a string comparison
  async test5(): Promise<void> {
    await this.db.transaction(
      async (tx) => {
        using s = tx.prepare<{ count: number; avg: number }>(
          'SELECT count(*) count, avg(b) avg FROM t2 WHERE c LIKE ?'
        );
        for (let i = 0; i < 100; i++) {
          const row = (await s.select([`%${numberName(i + 1)}%`]))[0];
          assert(row.count > 400);
          assert(row.count < 12000);
          assert(row.avg > 25000);
        }
      },
      { readonly: true }
    );
  }

  // Test 7: 5000 SELECTs with an index
  async test7(): Promise<void> {
    let promises: Promise<void>[] = [];
    const batchSize = 500;
    const batches = Math.ceil(5000 / batchSize);
    for (let batch = 0; batch < batches; batch++) {
      const promise = this.db.transaction(
        async (tx) => {
          using s = tx.prepare<{ count: number; avg: number }>(
            'SELECT count(*) count, avg(b) avg FROM t3 WHERE b>=? AND b<?'
          );
          for (
            let i = batch * batchSize;
            i < batch * batchSize + batchSize && i < 5000;
            i++
          ) {
            const row = (await s.select([i * 100, i * 100 + 100]))[0];
            if (i < 1000) {
              assert(row.count > 8);
              assert(row.count < 100);
            } else {
              assert(row.count === 0);
            }
          }
        },
        { readonly: true }
      );
      promises.push(promise);
    }
    await Promise.all(promises);
  }

  // Test 8: 1000 UPDATEs without an index
  async test8(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.transaction(async (tx) => {
      using s = tx.prepare('UPDATE t1 SET b=b*2 WHERE a>=? AND a<?');
      for (let i = 0; i < 1000; i++) {
        await s.run([i * 10, i * 10 + 10]);
      }
    });
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 9: 25000 UPDATEs with an index
  async test9(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.transaction(async (tx) => {
      using s = tx.prepare(
        'UPDATE t3 SET b = e.value ->> 1 FROM json_each(?) e WHERE a = e.value ->> 0'
      );
      let batch: any[][] = [];
      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        batch.push([i + 1, n]);
        if (batch.length >= 100) {
          await s.run([JSON.stringify(batch)]);
          batch = [];
        }
      }
      await s.run([JSON.stringify(batch)]);
    });
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 10: 25000 text UPDATEs with an index
  async test10(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.transaction(async (tx) => {
      using s = tx.prepare(
        'UPDATE t3 SET c = e.value ->> 1 FROM json_each(?) e WHERE a = e.value ->> 0'
      );

      let batch: any[][] = [];
      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        batch.push([i + 1, numberName(n)]);
        if (batch.length >= 100) {
          await s.run([JSON.stringify(batch)]);
          batch = [];
        }
      }
      await s.run([JSON.stringify(batch)]);
    });
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 11: INSERTs from a SELECT
  async test11(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.transaction(async (tx) => {
      await tx.run('INSERT INTO t1(a, b, c) SELECT b,a,c FROM t3');
      await tx.run('INSERT INTO t3(a, b, c) SELECT b,a,c FROM t1');
    });
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 12: DELETE without an index
  async test12(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.run("DELETE FROM t3 WHERE c LIKE '%fifty%'");
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 13: DELETE with an index
  async test13(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.run('DELETE FROM t3 WHERE a>10 AND a<20000');
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 14: A big INSERT after a big DELETE
  async test14(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.run('INSERT INTO t3(a, b, c) SELECT a, b, c FROM t1');
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 15: A big DELETE followed by many small INSERTs
  async test15(): Promise<void> {
    await using db = await this.db.reserveConnection();
    await db.transaction(async (tx) => {
      using s = tx.prepare(
        'INSERT INTO t1(a, b, c) SELECT value ->> 0, value ->> 1, value ->> 2 FROM json_each(?)'
      );
      await tx.run('DELETE FROM t1');
      let batch: any[][] = [];
      for (let i = 0; i < 12000; i++) {
        const n = this.random.nextInt(0, 100000);
        batch.push([i + 1, n, numberName(n)]);
        if (batch.length >= 100) {
          await s.run([JSON.stringify(batch)]);
          batch = [];
        }
      }
      await s.run([JSON.stringify(batch)]);
    });
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 16: Clear table
  async test16(): Promise<void> {
    await using db = await this.db.reserveConnection();
    const row1 = (
      await db.select<{ count: number }>('SELECT count() count FROM t1')
    )[0];
    const row2 = (
      await db.select<{ count: number }>('SELECT count() count FROM t2')
    )[0];
    const row3 = (
      await db.select<{ count: number }>('SELECT count() count FROM t3')
    )[0];
    assert(row1.count === 12000);
    assert(row2.count === 25000);
    assert(row3.count > 34000);
    assert(row3.count < 36000);

    await db.run('DELETE FROM t1');
    await db.run('DELETE FROM t2');
    await db.run('DELETE FROM t3');
    await db.run('PRAGMA wal_checkpoint(RESTART)');
  }
}
