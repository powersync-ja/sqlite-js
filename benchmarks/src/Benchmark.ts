import { BenchmarkResults } from './BenchmarkResults.js';

export abstract class Benchmark {
  abstract name: string;

  async runAll(): Promise<BenchmarkResults> {
    let results = new BenchmarkResults(this.name);
    let droppedFrames = 0;
    let last = performance.now();
    var timer = setInterval(() => {
      const now = performance.now();
      const diff = now - last;
      last = now;
      if (diff >= 16) {
        droppedFrames += Math.floor(diff / 16);
      }
    }, 1);

    await this.setUp();

    await results.record('Test 1: 1000 INSERTs', this.test1.bind(this));
    await results.record(
      'Test 2: 25000 INSERTs in a transaction',
      this.test2.bind(this)
    );
    await results.record(
      'Test 3: 25000 INSERTs into an indexed table',
      this.test3.bind(this)
    );
    await results.record(
      'Test 4: 100 SELECTs without an index',
      this.test4.bind(this)
    );
    await results.record(
      'Test 5: 100 SELECTs on a string comparison',
      this.test5.bind(this)
    );
    await results.record(
      'Test 7: 5000 SELECTs with an index',
      this.test7.bind(this)
    );
    await results.record(
      'Test 8: 1000 UPDATEs without an index',
      this.test8.bind(this)
    );
    await results.record(
      'Test 9: 25000 UPDATEs with an index',
      this.test9.bind(this)
    );
    await results.record(
      'Test 10: 25000 text UPDATEs with an index',
      this.test10.bind(this)
    );
    await results.record(
      'Test 11: INSERTs from a SELECT',
      this.test11.bind(this)
    );
    await results.record(
      'Test 12: DELETE without an index',
      this.test12.bind(this)
    );
    await results.record(
      'Test 13: DELETE with an index',
      this.test13.bind(this)
    );
    await results.record(
      'Test 14: A big INSERT after a big DELETE',
      this.test14.bind(this)
    );
    await results.record(
      'Test 15: A big DELETE followed by many small INSERTs',
      this.test15.bind(this)
    );
    await results.record('Test 16: Clear table', this.test16.bind(this));

    await this.tearDown();

    clearInterval(timer);

    const diff = performance.now() - last;
    if (diff >= 16) {
      droppedFrames += Math.floor(diff / 16);
    }

    console.log(`Dropped frames: ${droppedFrames} (diff ${diff})`);
    return results;
  }

  abstract setUp(): Promise<void>;

  abstract test1(): Promise<void>;
  abstract test2(): Promise<void>;
  abstract test3(): Promise<void>;
  abstract test4(): Promise<void>;
  abstract test5(): Promise<void>;
  abstract test7(): Promise<void>;
  abstract test8(): Promise<void>;
  abstract test9(): Promise<void>;
  abstract test10(): Promise<void>;
  abstract test11(): Promise<void>;
  abstract test12(): Promise<void>;
  abstract test13(): Promise<void>;
  abstract test14(): Promise<void>;
  abstract test15(): Promise<void>;
  abstract test16(): Promise<void>;

  abstract tearDown(): Promise<void>;
}
