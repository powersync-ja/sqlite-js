import Database from "better-sqlite3";

import * as worker_threads from "worker_threads";

let db = null;

worker_threads.parentPort.addListener("message", (value) => {
  const [message, args] = value;
  if (message == "open") {
    db = new Database(args.path);
  } else if (message == "execute") {
    if (args.args == null) {
      const rs = db.prepare(args.query).all();
      worker_threads.parentPort.postMessage(rs);
    } else {
      const rs = db.prepare(args.query).all(args.args);
      worker_threads.parentPort.postMessage(rs);
    }
  } else if (message == "stream") {
    const bindArgs = args == undefined ? [] : [args];
    const statement = db.prepare(args.query);
    if (!statement.reader) {
      statement.run(...bindArgs);
      worker_threads.parentPort.postMessage(["close"]);
      return;
    }
    if (args.options?.bigint) {
      statement.safeIntegers();
    }
    statement.raw();
    const columns = statement.columns().map((c) => c.name);
    worker_threads.parentPort.postMessage(["columns", columns]);

    let buffer = [];
    for (let row of statement.iterate(...bindArgs)) {
      buffer.push(row);
      if (buffer.length > 10) {
        worker_threads.parentPort.postMessage(["rows", buffer]);
        buffer = [];
      }
    }
    if (buffer.length > 0) {
      worker_threads.parentPort.postMessage(["rows", buffer]);
    }
    worker_threads.parentPort.postMessage(["close"]);
  }
});
