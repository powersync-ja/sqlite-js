import { OPFSCoopSyncVFS2 } from './OPFSCoopSyncVFS2';
import { sqlite3, module, WaSqliteConnection } from './wa-sqlite-driver';
import { setupDriverWorker } from './worker_threads';

// Register a custom file system.
const vfs = await OPFSCoopSyncVFS2.create('test.db', module);
// @ts-ignore
sqlite3.vfs_register(vfs as any, true);

setupDriverWorker({
  async openConnection(options) {
    return await WaSqliteConnection.open(options.path, vfs);
  }
});
