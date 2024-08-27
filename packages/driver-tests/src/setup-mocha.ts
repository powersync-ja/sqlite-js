import type { TestContext } from './test.js';

import { beforeEach as originalBeforeEach } from 'mocha';
export { describe, test } from 'mocha';
import { describe, test } from 'mocha';

export function beforeEach(callback: (context: TestContext) => any) {
  originalBeforeEach(function () {
    const testName = this.currentTest!.fullTitle();
    return callback({ fullName: testName });
  });
}

(test as any).skipIf = function (condition: boolean) {
  if (condition) {
    return test.skip;
  } else {
    return test;
  }
};
