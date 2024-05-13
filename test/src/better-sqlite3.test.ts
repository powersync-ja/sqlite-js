import { betterSqlitePool } from '../../lib/drivers/better-sqlite3-driver.js';
import { describeDriverTests } from './tests/drivers.js';

describeDriverTests('better-sqlite3', betterSqlitePool);
