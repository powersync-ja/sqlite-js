export declare class DatabaseSync {
  constructor(location: string, options?: { open?: boolean });

  close(): void;
  exec(sql: string): void;
  open(): void;
  prepare(sql: string): StatementSync;
}

export declare class StatementSync {
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

export async function loadNodeSqlite() {
  return await import('node:sqlite' as any);
}
