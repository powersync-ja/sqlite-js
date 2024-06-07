export class BenchmarkResults {
  suite: string;

  results: BenchmarkResult[] = [];

  constructor(suite: string) {
    this.suite = suite;
  }

  async record(name: string, callback: () => Promise<void>) {
    const start = performance.now();
    await callback();
    const end = performance.now();
    const elapsed = end - start;
    this.results.push(new BenchmarkResult(name, elapsed));
    console.log(`${name} :: ${Math.round(elapsed)}ms`);
    // yield the event loop
    await new Promise((resolve) => setTimeout(resolve, 0));
  }

  toString() {
    return this.results.map((r) => r.toString()).join('\n');
  }

  toCsv() {
    return this.results.map((r) => r.toCsv()).join('\n');
  }
}

export class BenchmarkResult {
  constructor(
    public test: string,
    public duration: number
  ) {}

  toString() {
    return `${this.test}: ${this.duration / 1000.0}s`;
  }

  toCsv() {
    return `${this.test},${this.duration}`;
  }
}
