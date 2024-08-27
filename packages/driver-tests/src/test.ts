// A lib to allow testing with vitest or mocha
import type {
  test as testType,
  describe as describeType,
  expect as expectType
} from 'vitest';

export interface TestContext {
  fullName: string;
}

export const isVitest = true || process.env.VITEST == 'true';
export const isMocha = !isVitest;

let testImpl, describeImpl, beforeEachImpl, expectImpl;

if (isMocha) {
  const { test, describe, beforeEach } = await import('./setup-mocha.js');
  const { expect } = await import('expect');
  expectImpl = expect;
  testImpl = test;
  describeImpl = describe;
  beforeEachImpl = beforeEach;
} else {
  const { test, describe, beforeEach } = await import('./setup-vitest.js');
  const { expect } = await import('vitest');
  expectImpl = expect;
  testImpl = test;
  describeImpl = describe;
  beforeEachImpl = beforeEach;
}

export function beforeEach(callback: (context: TestContext) => any) {
  return beforeEachImpl!(callback);
}

export const test = testImpl as typeof testType;
export const describe = describeImpl as typeof describeType;
export const expect = expectImpl as typeof expectType;
