import * as fs from 'node:fs/promises';
import * as path from 'node:path';

export async function deleteDb(dbPath: string) {
  const dir = path.dirname(dbPath);
  try {
    await fs.mkdir(dir);
  } catch (e) {}
  try {
    await fs.rm(dbPath);
  } catch (e) {}
}
