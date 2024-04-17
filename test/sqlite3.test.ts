import { sqlite3Pool } from "../src/drivers/sqlite3-driver.js";
import { describeDriverTests } from "./tests/drivers.js";

describeDriverTests("sqlite3", sqlite3Pool);
