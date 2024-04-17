import { betterSqliteAsyncPool } from "../src/drivers/better-sqlite3-async-driver.js";
import { describeDriverTests } from "./tests/drivers.js";

describeDriverTests("better-sqlite3-async-pool", betterSqliteAsyncPool);
