import { OPFSCoopSyncVFS2 } from './OPFSCoopSyncVFS2';
import { sqlite3, module, WaSqliteConnection } from './wa-sqlite-driver';
import { setupDriverWorker } from './worker_threads';
import { IDBBatchAtomicVFS } from 'wa-sqlite/src/examples/IDBBatchAtomicVFS.js';
import { OPFSAdaptiveVFS } from 'wa-sqlite/src/examples/OPFSAdaptiveVFS.js';
import { OPFSPermutedVFS } from 'wa-sqlite/src/examples/OPFSPermutedVFS.js';

let vfs: any | null = null;
setupDriverWorker({
  async openConnection(options) {
    // Register a custom file system.
    if (vfs != null) {
      throw new Error('Can only open one connection');
    }
    vfs = await OPFSCoopSyncVFS2.create(
      'test.db',
      module,
      options.readonly ?? false
    );
    // IDBBatchAtomicVFS - breaks hard (database disk image is malformed)
    // vfs = await (IDBBatchAtomicVFS as any).create('test.db', module);
    // OPFSAdaptiveVFS - works great
    // vfs = await (OPFSAdaptiveVFS as any).create('test.db', module, {
    //   ifAvailable: true,
    //   mode: 'shared'
    // });

    // database disk image is malformed
    // vfs = await (OPFSPermutedVFS as any).create('test.db', module);

    // @ts-ignore
    sqlite3.vfs_register(vfs as any, true);

    const con = await WaSqliteConnection.open(options.path);
    // using stmt = await con.prepare('PRAGMA busy_timeout = 10000');
    // await stmt.step();
    return con;
  }
});
