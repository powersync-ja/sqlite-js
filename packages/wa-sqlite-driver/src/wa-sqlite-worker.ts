import { sqlite3, module, WaSqliteConnection } from './wa-sqlite-driver';
import { setupDriverWorker } from './worker_threads';
import { IDBBatchAtomicVFS } from '@journeyapps/wa-sqlite/src/examples/IDBBatchAtomicVFS.js';
import { OPFSCoopSyncVFS } from '@journeyapps/wa-sqlite/src/examples/OPFSCoopSyncVFS.js';

// Register a custom file system.
// @ts-ignore
const vfs = await OPFSCoopSyncVFS.create('test.db', module, {
  lockPolicy: 'exclusive'
});
// @ts-ignore
sqlite3.vfs_register(vfs as any, true);

setupDriverWorker({
  async openConnection(options) {
    return await WaSqliteConnection.open(options.path);
  }
});
