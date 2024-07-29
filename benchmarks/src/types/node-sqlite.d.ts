declare module 'node:sqlite' {
  export class DatabaseSync {
    constructor(location: string, options?: { open?: boolean });

    close(): void;
    exec(sql: string): void;
    open(): void;
    prepare(sql: string): StatementSync;
  }

  export class StatementSync {
    all(namedParameters?: object, ...anonymousParameters: any[]): any[];
    expandedSQL(): string;
    get(...anonymousParameters: any[]): object | undefined;
    run(...anonymousParameters: any[]): {
      changes: number | bigint;
      lastInsertRowid: number | bigint;
    };
    setAllowBareNamedParameters(enabled: boolean): void;
    setReadBigInts(enabled: boolean): void;
    sourceSQL(): string;
  }
}
