// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.
// This module is browser compatible.

/**
 * Error thrown when an assertion fails.
 *
 * @example Usage
 * ```ts no-eval
 * import { AssertionError } from "@std/assert";
 *
 * try {
 *   throw new AssertionError("foo", { cause: "bar" });
 * } catch (error) {
 *   if (error instanceof AssertionError) {
 *     error.message === "foo"; // true
 *     error.cause === "bar"; // true
 *   }
 * }
 * ```
 */
export class AssertionError extends Error {
  /** Constructs a new instance.
   *
   * @param message The error message.
   * @param options Additional options. This argument is still unstable. It may change in the future release.
   */
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'AssertionError';
  }
}

/**
 * Make an assertion that actual is not null or undefined.
 * If not then throw.
 *
 * @example Usage
 * ```ts no-eval
 * import { assertExists } from "@std/assert";
 *
 * assertExists("something"); // Doesn't throw
 * assertExists(undefined); // Throws
 * ```
 *
 * @typeParam T The type of the actual value.
 * @param actual The actual value to check.
 * @param msg The optional message to include in the error if the assertion fails.
 */
export function assertExists<T>(
  actual: T,
  msg?: string
): asserts actual is NonNullable<T> {
  if (actual === undefined || actual === null) {
    const msgSuffix = msg ? `: ${msg}` : '.';
    msg = `Expected actual: "${actual}" to not be null or undefined${msgSuffix}`;
    throw new AssertionError(msg);
  }
}

/** Any constructor */
// deno-lint-ignore no-explicit-any
export type AnyConstructor = new (...args: any[]) => any;
/** Gets constructor type */
export type GetConstructorType<T extends AnyConstructor> = T extends new ( // deno-lint-ignore no-explicit-any
  ...args: any
) => infer C
  ? C
  : never;

/**
 * Make an assertion that `obj` is an instance of `type`.
 * If not then throw.
 *
 * @example Usage
 * ```ts no-eval
 * import { assertInstanceOf } from "@std/assert";
 *
 * assertInstanceOf(new Date(), Date); // Doesn't throw
 * assertInstanceOf(new Date(), Number); // Throws
 * ```
 *
 * @typeParam T The expected type of the object.
 * @param actual The object to check.
 * @param expectedType The expected class constructor.
 * @param msg The optional message to display if the assertion fails.
 */
export function assertInstanceOf<T extends AnyConstructor>(
  actual: unknown,
  expectedType: T,
  msg = ''
): asserts actual is GetConstructorType<T> {
  if (actual instanceof expectedType) return;

  const msgSuffix = msg ? `: ${msg}` : '.';
  const expectedTypeStr = expectedType.name;

  let actualTypeStr = '';
  if (actual === null) {
    actualTypeStr = 'null';
  } else if (actual === undefined) {
    actualTypeStr = 'undefined';
  } else if (typeof actual === 'object') {
    actualTypeStr = actual.constructor?.name ?? 'Object';
  } else {
    actualTypeStr = typeof actual;
  }

  if (expectedTypeStr === actualTypeStr) {
    msg = `Expected object to be an instance of "${expectedTypeStr}"${msgSuffix}`;
  } else if (actualTypeStr === 'function') {
    msg = `Expected object to be an instance of "${expectedTypeStr}" but was not an instanced object${msgSuffix}`;
  } else {
    msg = `Expected object to be an instance of "${expectedTypeStr}" but was "${actualTypeStr}"${msgSuffix}`;
  }

  throw new AssertionError(msg);
}
