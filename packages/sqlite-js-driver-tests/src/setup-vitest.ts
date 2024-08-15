import type { TestContext } from './test.js';

import { beforeEach as originalBeforeEach } from 'vitest';

export { describe, test } from 'vitest';

export function beforeEach(callback: (context: TestContext) => any) {
  originalBeforeEach(({ expect }) => {
    const testName = expect.getState().currentTestName!;
    return callback({ fullName: testName });
  });
}
