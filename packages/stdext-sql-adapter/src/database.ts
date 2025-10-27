/** Various options that can be configured when opening Database connection. */
export interface DatabaseOpenOptions {
  /** Whether to open database only in read-only mode. By default, this is false. */
  readonly?: boolean;
  /** Whether to create a new database file at specified path if one does not exist already. By default this is true. */
  create?: boolean;
  /** Raw SQLite C API flags. Specifying this ignores all other options. */
  flags?: number;
  /** Opens an in-memory database. */
  memory?: boolean;
  /** Whether to support BigInt columns. False by default, integers larger than 32 bit will be inaccurate. */
  int64?: boolean;
  /** Apply agressive optimizations that are not possible with concurrent clients. */
  unsafeConcurrency?: boolean;
  /** Enable or disable extension loading */
  enableLoadExtension?: boolean;
}

/** Transaction function created using `Database#transaction`. */
export type Transaction<T extends (...args: any[]) => void> = ((
  ...args: Parameters<T>
) => ReturnType<T>) & {
  /** BEGIN */
  default: Transaction<T>;
  /** BEGIN DEFERRED */
  deferred: Transaction<T>;
  /** BEGIN IMMEDIATE */
  immediate: Transaction<T>;
  /** BEGIN EXCLUSIVE */
  exclusive: Transaction<T>;
};

/**
 * Options for user-defined functions.
 *
 * @link https://www.sqlite.org/c3ref/c_deterministic.html
 */
export interface FunctionOptions {
  varargs?: boolean;
  deterministic?: boolean;
  directOnly?: boolean;
  innocuous?: boolean;
  subtype?: boolean;
}

/**
 * Options for user-defined aggregate functions.
 */
export interface AggregateFunctionOptions extends FunctionOptions {
  start: any | (() => any);
  step: (aggregate: any, ...args: any[]) => void;
  final?: (aggregate: any) => any;
}
