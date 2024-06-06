import {
  ConnectionPoolImpl,
  betterSqliteAsyncPool,
  betterSqlitePool
} from '../../lib/index.js';
import { Benchmark } from './Benchmark.js';
import { BenchmarkResults } from './BenchmarkResults.js';
import { BetterSqlite3Impl } from './implementations/better-sqlite3.js';
import { JSPOptimizedImpl } from './implementations/sjp-optimized.js';
import { JSPImpl } from './implementations/sjp.js';

async function main() {
  const dir = 'db';

  const results: BenchmarkResults[] = [
    await test(
      new JSPImpl('sjp-sync', dir, (path) => {
        const db = new ConnectionPoolImpl(betterSqlitePool(path));
        return db;
      })
    ),
    await test(
      new JSPImpl('sjp-async', dir, (path) => {
        const db = new ConnectionPoolImpl(betterSqliteAsyncPool(path));
        return db;
      })
    ),
    await test(
      new JSPOptimizedImpl('sjp-sync-optimized', dir, (path) => {
        const db = new ConnectionPoolImpl(betterSqlitePool(path));
        return db;
      })
    ),
    await test(
      new JSPOptimizedImpl('sjp-async-optimized', dir, (path) => {
        const db = new ConnectionPoolImpl(betterSqliteAsyncPool(path));
        return db;
      })
    ),
    await test(new BetterSqlite3Impl('better-sqlite3', dir))
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
