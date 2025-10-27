import { beforeEach, describe, expect, test } from '@sqlite-js/driver-tests';
import { waSqliteSingleWorker, waSqliteWorkerPool } from '../../lib/index.js';

describe('concurrency tests', () => {
  let dbPath: string;

  const open = async () => {
    const db = await waSqliteWorkerPool(dbPath);
    return db;
  };

  beforeEach((context) => {
    let testNameSanitized = context.fullName.replaceAll(
      /[\s\/\\>\.\-\:]+/g,
      '_'
    );

    if (testNameSanitized.length > 10) {
      testNameSanitized =
        testNameSanitized.substring(testNameSanitized.length - 7) +
        String(Math.random()).substring(1, 4);
    }
    dbPath = `test-db/${testNameSanitized}.db`;
  });

  test('concurrent select', async () => {
    await using driver = await open();
    {
      await using connection = await driver.reserveConnection();
      using s1 = connection.prepare(
        'create table test_data(id integer primary key, data text)'
      );
      await s1.run();
      using s2 = connection.prepare(
        "insert into test_data(data) values('test')"
      );
      await s2.run();
    }

    let promises: Promise<void>[] = [];

    for (let i = 0; i < 5; i++) {
      const p = (async () => {
        const start = Date.now();
        await using connection = await driver.reserveConnection({
          readonly: true
        });

        using b = connection.prepare('begin immediate');
        await b.run();
        using s = connection.prepare('select * from test_data');
        const rows = await s.all();

        expect(rows).toEqual([{ id: 1, data: 'test' }]);
        await new Promise((resolve) => setTimeout(resolve, 500));

        using e = connection.prepare('commit');
        await e.run();
        console.log('tx done in', Date.now() - start);
      })();
      promises.push(p);
    }
    await Promise.all(promises);

    for (let i = 0; i < 5; i++) {
      const p = (async () => {
        const start = Date.now();
        await using connection = await driver.reserveConnection({
          readonly: true
        });

        using b = connection.prepare('begin immediate');
        await b.run();
        using s = connection.prepare('select * from test_data');
        const rows = await s.all();

        expect(rows).toEqual([{ id: 1, data: 'test' }]);
        await new Promise((resolve) => setTimeout(resolve, 500));

        using e = connection.prepare('commit');
        await e.run();
        console.log('tx done in', Date.now() - start);
      })();
      promises.push(p);
    }
    await Promise.all(promises);
  });
});
