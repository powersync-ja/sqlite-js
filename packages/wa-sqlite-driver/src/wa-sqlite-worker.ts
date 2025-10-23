import { OPFSCoopSyncVFS2 } from './OPFSCoopSyncVFS2';
import { sqlite3, module, WaSqliteConnection } from './wa-sqlite-driver';
import { setupDriverWorker } from './worker_threads';

let vfs: OPFSCoopSyncVFS2 | null = null;
setupDriverWorker({
  async openConnection(options) {
    // Register a custom file system.
    if (vfs != null) {
      throw new Error('Can only open one connection');
    }
    console.log('open', options);
    vfs = await OPFSCoopSyncVFS2.create(
      'test.db',
      module,
      options.readonly ?? false
    );
    // @ts-ignore
    sqlite3.vfs_register(vfs as any, true);

    return await WaSqliteConnection.open(options.path, vfs);
  }
});
