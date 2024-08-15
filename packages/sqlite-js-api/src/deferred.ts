export class Deferred<T> {
  promise: Promise<T>;
  resolve: (result: T) => void = undefined as any;
  reject: (error: any) => void = undefined as any;

  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}
