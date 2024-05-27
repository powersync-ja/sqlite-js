'use strict';

/*
	Every benchmark trial will be executed once for each SQLite3 driver listed
	below. Each driver has a function to open a new database connection on a
	given filename and a list of PRAGMA statements.
 */

module.exports = new Map([
  // [
  //   'better-sqlite3',
  //   async (filename, pragma) => {
  //     const db = require('better-sqlite3')(filename);
  //     for (const str of pragma) db.pragma(str);
  //     return db;
  //   }
  // ],
  // [
  //   'node-sqlite3',
  //   async (filename, pragma) => {
  //     const driver = require('sqlite3').Database;
  //     const db = await require('sqlite').open({ filename, driver });
  //     for (const str of pragma) await db.run(`PRAGMA ${str}`);
  //     return db;
  //   }
  // ],
  // [
  //   'sjp',
  //   async (filename, pragma) => {
  //     const { ConnectionPoolImpl } = await import('../lib/impl.js');
  //     const { betterSqliteAsyncPool } = await import(
  //       '../lib/drivers/better-sqlite3-async-driver.js'
  //     );
  //     const db = new ConnectionPoolImpl(betterSqliteAsyncPool(filename));
  //     for (const str of pragma) await db.execute(`PRAGMA ${str}`);
  //     return db;
  //   }
  // ],
  [
    'sjp-sync',
    async (filename, pragma) => {
      const { ConnectionPoolImpl } = await import('../lib/impl.js');
      const { betterSqlitePool } = await import(
        '../lib/drivers/better-sqlite3-driver.js'
      );
      const db = new ConnectionPoolImpl(betterSqlitePool(filename));
      for (const str of pragma) await db.execute(`PRAGMA ${str}`);
      return db;
    }
  ]
]);
