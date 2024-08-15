import { promises as fs } from 'fs';
import assert from 'node:assert';
import { join } from 'path';
import Prando from 'prando';
import { Benchmark } from '../Benchmark.js';
import { numberName } from '../util.js';

//@ts-ignore
import * as sqlite from 'node:sqlite';

export class NodeSqliteImpl extends Benchmark {
  private db!: sqlite.DatabaseSync;
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

    const db = new sqlite.DatabaseSync(dbPath);
    this.db = db;
    db.exec('PRAGMA journal_mode = wal');
    db.exec('PRAGMA synchronous = normal');

    db.exec(
      'CREATE TABLE t1(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)'
    );
    db.exec(
      'CREATE TABLE t2(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)'
    );
    db.exec(
      'CREATE TABLE t3(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)'
    );
    db.exec('CREATE INDEX i3a ON t3(a)');
    db.exec('CREATE INDEX i3b ON t3(b)');
  }

  async tearDown(): Promise<void> {
    this.db.close();
  }

  // Test 1: 1000 INSERTs
  async test1(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare('INSERT INTO t1(a, b, c) VALUES(?, ?, ?)');
    for (let i = 0; i < 1000; i++) {
      const n = this.random.nextInt(0, 100000);
      stmt.run(i + 1, n, numberName(n));
    }
    db.exec('PRAGMA wal_checkpoint(RESTART)');
    const total = db.prepare('select count() as count from t1').get() as {
      count: number;
    };

    assert(total.count == 1000);
  }

  // Test 2: 25000 INSERTs in a transaction
  async test2(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare('INSERT INTO t2(a, b, c) VALUES(?, ?, ?)');
    this.transaction(() => {
      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        stmt.run(i + 1, n, numberName(n));
      }
    });
    db.exec('PRAGMA wal_checkpoint(RESTART)');
    const total = db.prepare('select count() as count from t2').get() as {
      count: number;
    };

    assert(total.count == 25000);
  }

  // Test 3: 25000 INSERTs into an indexed table
  async test3(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare('INSERT INTO t3(a, b, c) VALUES(?, ?, ?)');
    this.transaction(() => {
      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        stmt.run(i + 1, n, numberName(n));
      }
    });
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 4: 100 SELECTs without an index
  async test4(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare(
      'SELECT count(*) count, avg(b) avg FROM t2 WHERE b>=? AND b<?'
    );
    this.transaction(() => {
      for (let i = 0; i < 100; i++) {
        const row = stmt.get(i * 100, i * 100 + 1000) as {
          count: number;
          avg: number;
        };

        assert(row.count > 200);
        assert(row.count < 300);
        assert(row.avg > i * 100);
        assert(row.avg < i * 100 + 1000);
      }
    });
  }

  // Test 5: 100 SELECTs on a string comparison
  async test5(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare(
      'SELECT count(*) count, avg(b) avg FROM t2 WHERE c LIKE ?'
    );
    this.transaction(() => {
      for (let i = 0; i < 100; i++) {
        const row = stmt.get(`%${numberName(i + 1)}%`) as {
          count: number;
          avg: number;
        };
        assert(row.count > 400);
        assert(row.count < 12000);
        assert(row.avg > 25000);
      }
    });
  }

  // Test 7: 5000 SELECTs with an index
  async test7(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare(
      'SELECT count(*) count, avg(b) avg FROM t3 WHERE b>=? AND b<?'
    );
    this.transaction(() => {
      for (let i = 0; i < 5000; i++) {
        const row = stmt.get(i * 100, i * 100 + 100) as {
          count: number;
          avg: number;
        };
        if (i < 1000) {
          assert(row.count > 8);
          assert(row.count < 100);
        } else {
          assert(row.count === 0);
        }
      }
    });
  }

  // Test 8: 1000 UPDATEs without an index
  async test8(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare('UPDATE t1 SET b=b*2 WHERE a>=? AND a<?');
    this.transaction(() => {
      for (let i = 0; i < 1000; i++) {
        stmt.run(i * 10, i * 10 + 10);
      }
    });
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 9: 25000 UPDATEs with an index
  async test9(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare('UPDATE t3 SET b=? WHERE a=?');
    this.transaction(() => {
      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        stmt.run(n, i + 1);
      }
    });
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 10: 25000 text UPDATEs with an index
  async test10(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare('UPDATE t3 SET c=? WHERE a=?');
    this.transaction(() => {
      for (let i = 0; i < 25000; i++) {
        const n = this.random.nextInt(0, 100000);
        stmt.run(numberName(n), i + 1);
      }
    });
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 11: INSERTs from a SELECT
  async test11(): Promise<void> {
    const db = this.db;
    this.transaction(() => {
      db.exec('INSERT INTO t1(a, b, c) SELECT b,a,c FROM t3');
      db.exec('INSERT INTO t3(a, b, c) SELECT b,a,c FROM t1');
    });
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 12: DELETE without an index
  async test12(): Promise<void> {
    const db = this.db;
    db.exec("DELETE FROM t3 WHERE c LIKE '%fifty%'");
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 13: DELETE with an index
  async test13(): Promise<void> {
    const db = this.db;
    db.exec('DELETE FROM t3 WHERE a>10 AND a<20000');
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 14: A big INSERT after a big DELETE
  async test14(): Promise<void> {
    const db = this.db;
    db.exec('INSERT INTO t3(a, b, c) SELECT a, b, c FROM t1');
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 15: A big DELETE followed by many small INSERTs
  async test15(): Promise<void> {
    const db = this.db;
    const stmt = db.prepare('INSERT INTO t1(a, b, c) VALUES(?, ?, ?)');
    this.transaction(() => {
      db.exec('DELETE FROM t1');
      for (let i = 0; i < 12000; i++) {
        const n = this.random.nextInt(0, 100000);
        stmt.run(i + 1, n, numberName(n));
      }
    });
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  // Test 16: Clear table
  async test16(): Promise<void> {
    const db = this.db;
    const row1 = db.prepare('SELECT count() count FROM t1').get() as {
      count: number;
    };
    const row2 = db.prepare('SELECT count() count FROM t2').get() as {
      count: number;
    };
    const row3 = db.prepare('SELECT count() count FROM t3').get() as {
      count: number;
    };
    assert(row1.count === 12000);
    assert(row2.count === 25000);
    assert(row3.count > 34000);
    assert(row3.count < 36000);

    db.exec('DELETE FROM t1');
    db.exec('DELETE FROM t2');
    db.exec('DELETE FROM t3');
    db.exec('PRAGMA wal_checkpoint(RESTART)');
  }

  transaction(callback: () => void) {
    this.db.exec('begin');
    try {
      callback();
      this.db.exec('commit');
    } catch (e) {
      try {
        this.db.exec('rollback');
      } catch (e2) {}
      throw e;
    }
  }
}
