import { describe, test } from '@sqlite-js/driver-tests';
import { MptestRunner, sanitizeForFilename } from './mptest-runner.js';

async function runMptestScript(script: string): Promise<void> {
  const dbPath = `mptest-${sanitizeForFilename(script)}-${Math.random()
    .toString(36)
    .slice(2)}.db`;
  const runner = new MptestRunner(dbPath);
  try {
    await runner.runScript(script);
  } finally {
    await runner.close();
  }
}

describe('mptest scripts', { timeout: 60_000 }, () => {
  test('mptest/multiwrite01.test', async () => {
    await runMptestScript('mptest/multiwrite01.test');
  });

  test('mptest/config02.test', async () => {
    await runMptestScript('mptest/config02.test');
  });

  test('mptest/crash01.test', async () => {
    await runMptestScript('mptest/crash01.test');
  });
});
