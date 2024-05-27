'use strict';
exports.readonly = true; // Iterating over 100 rows (`.iterate()`)

exports['better-sqlite3'] = (db, { table, columns, count }) => {
  const stmt = db.prepare(
    `SELECT ${columns.join(', ')} FROM ${table} WHERE rowid >= ? LIMIT 100`
  );
  let rowid = -100;
  return () => {
    for (const row of stmt.iterate(((rowid += 100) % count) + 1)) {
      console.log('row', row);
    }
  };
};

exports['node-sqlite3'] = async (db, { table, columns, count }) => {
  const sql = `SELECT ${columns.join(', ')} FROM ${table} WHERE rowid = ?`;
  let rowid = -100;
  return () => {
    rowid += 100;
    let index = 0;
    return (function next() {
      if (index === 100) return;
      return db.get(sql, ((rowid + index++) % count) + 1).then(next);
    })();
  };
};

exports['sjp'] = (db, { table, columns, count }) => {
  const sql = `SELECT ${columns.join(', ')} FROM ${table} WHERE rowid >= ? LIMIT 100`;
  console.log('select-iterate wtf');
  let rowid = -100;
  return async () => {
    try {
      console.log('iter', sql);
      for await (const chunk of db.executeStreamed(sql, [
        ((rowid += 100) % count) + 1
      ])) {
        // throw new Error('chunk: ' + chunk);
        // console.log('chunk', chunk);
      }
    } catch (e) {
      console.error(e);
    } finally {
      console.error('wtf');
    }
    console.log('done iter', sql);
  };
};

exports['sjp-sync'] = exports['sjp'];
