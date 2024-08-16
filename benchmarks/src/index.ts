import { NodeSqliteDriver } from '@powersync/sqlite-js-driver/node';
import { ConnectionPoolImpl } from '@powersync/sqlite-js-api';
import { BetterSqliteDriver } from '@powersync/sqlite-js-better-sqlite3';
import { Benchmark } from './Benchmark.js';
import { BenchmarkResults } from './BenchmarkResults.js';
import { BetterSqlite3Impl } from './implementations/better-sqlite3.js';
import { NodeSqliteImpl } from './implementations/node-sqlite.js';
import { NodeSqlite3Impl } from './implementations/node-sqlite3.js';
import { JSPJsonImpl } from './implementations/sjp-json.js';
import { JSPOptimizedImpl } from './implementations/sjp-optimized.js';
import { JSPImpl } from './implementations/sjp.js';

async function main() {
  const dir = 'db';

  const results: BenchmarkResults[] = [
    await test(
      new JSPImpl('sjp-sync', dir, (path) => {
        const db = new ConnectionPoolImpl(
          BetterSqliteDriver.openInProcess(path)
        );
        return db;
      })
    ),
    await test(
      new JSPImpl('sjp-async', dir, (path) => {
        const db = new ConnectionPoolImpl(BetterSqliteDriver.open(path));
        return db;
      })
    ),
    await test(
      new JSPOptimizedImpl('sjp-sync-optimized', dir, (path) => {
        const db = new ConnectionPoolImpl(
          BetterSqliteDriver.openInProcess(path)
        );
        return db;
      })
    ),
    await test(
      new JSPOptimizedImpl('sjp-async-optimized', dir, (path) => {
        const db = new ConnectionPoolImpl(BetterSqliteDriver.open(path));
        return db;
      })
    ),
    await test(
      new JSPJsonImpl('sjp-sync-json', dir, (path) => {
        const db = new ConnectionPoolImpl(
          BetterSqliteDriver.openInProcess(path)
        );
        return db;
      })
    ),
    await test(
      new JSPJsonImpl('sjp-async-json', dir, (path) => {
        const db = new ConnectionPoolImpl(BetterSqliteDriver.open(path));
        return db;
      })
    ),
    await test(
      new JSPImpl('node-sjp-sync', dir, (path) => {
        const db = new ConnectionPoolImpl(NodeSqliteDriver.openInProcess(path));
        return db;
      })
    ),
    await test(
      new JSPImpl('node-sjp-async', dir, (path) => {
        const db = new ConnectionPoolImpl(NodeSqliteDriver.open(path));
        return db;
      })
    ),
    await test(
      new JSPOptimizedImpl('node-sjp-sync-optimized', dir, (path) => {
        const db = new ConnectionPoolImpl(NodeSqliteDriver.openInProcess(path));
        return db;
      })
    ),
    await test(
      new JSPOptimizedImpl('node-sjp-async-optimized', dir, (path) => {
        const db = new ConnectionPoolImpl(NodeSqliteDriver.open(path));
        return db;
      })
    ),
    await test(new BetterSqlite3Impl('better-sqlite3', dir)),
    await test(new NodeSqlite3Impl('node-sqlite3', dir)),
    await test(new NodeSqliteImpl('node:sqlite', dir))
  ];

  const first = results[0];
  let s = 'Test';
  for (const rr of results) {
    s += `,${rr.suite}`;
  }
  console.log('');
  console.log(s);
  for (let i = 0; i < first.results.length; i++) {
    const test = first.results[i].test;
    let s = `${test}`;
    for (const rr of results) {
      const r3 = rr.results[i].duration;
      s += `,${r3}`;
    }
    console.log(s);
  }
  console.log('');
}

async function test(bm: Benchmark): Promise<BenchmarkResults> {
  console.log(bm.name);
  const results = await bm.runAll();
  return results;
}

await main();
