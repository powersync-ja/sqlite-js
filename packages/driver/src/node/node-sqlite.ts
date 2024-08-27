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
  try {
    return await import('node:sqlite' as any);
  } catch (e: any) {
    if (!isNodeVersionAtLeast('22.5.0')) {
      throw new Error(`${e.message}\nNode >= 22.5.0 is required`);
    }
    if (!process.env.NODE_OPTIONS?.includes('--experimental-sqlite')) {
      throw new Error(`${e.message}\nUse NODE_OPTIONS=--experimental-sqlite`);
    }
    throw e;
  }
}

function isNodeVersionAtLeast(requiredVersion: string) {
  const currentVersion = process.version.slice(1).split('.').map(Number);
  const [requiredMajor, requiredMinor, requiredPatch] = requiredVersion
    .split('.')
    .map(Number);

  return (
    currentVersion[0] > requiredMajor ||
    (currentVersion[0] === requiredMajor &&
      (currentVersion[1] > requiredMinor ||
        (currentVersion[1] === requiredMinor &&
          currentVersion[2] >= requiredPatch)))
  );
}
