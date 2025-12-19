/**
 * SQLite driver schema management tests (Node.js)
 *
 * Tests ensureTable() and ensureConstraints() for better-sqlite3 driver.
 * Run with: npm run test:node
 */

import {describe, test, expect} from "./node-test-utils.js";
import {table} from "./table.js";
import {z} from "zod";
import {SchemaDriftError} from "./errors.js";
import SQLiteDriver from "../sqlite.js";

describe("SQLiteDriver Schema Management", () => {
	test("ensureTable() creates new table", async () => {
		const driver = new SQLiteDriver(":memory:");

		const users = table("users", {
			id: z.number().int().db.primary(),
			email: z.string(),
		});

		const result = await driver.ensureTable(users);
		expect(result.applied).toBe(true);

		await driver.close();
	});

	test("ensureTable() detects missing unique constraint", async () => {
		const driver = new SQLiteDriver(":memory:");

		// Create table without unique constraint
		await driver.run(
			["CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"] as any,
			[],
		);

		const users = table("users", {
			id: z.number().int().db.primary(),
			email: z.string().db.unique(),
		});

		await expect(driver.ensureTable(users)).rejects.toThrow(SchemaDriftError);

		await driver.close();
	});

	test("ensureConstraints() throws error for foreign keys on SQLite", async () => {
		const driver = new SQLiteDriver(":memory:");

		const users = table("users", {
			id: z.number().int().db.primary(),
		});

		// Create posts table WITHOUT foreign key first
		const posts1 = table("posts", {
			id: z.number().int().db.primary(),
			userId: z.number().int(),
		});

		await driver.ensureTable(users);
		await driver.ensureTable(posts1);

		// Now try to add FK with ensureConstraints
		const posts2 = table("posts", {
			id: z.number().int().db.primary(),
			userId: z.number().int().db.references(users, "author"),
		});

		// SQLite cannot add foreign keys to existing tables
		await expect(driver.ensureConstraints(posts2)).rejects.toThrow(
			/Adding foreign key constraints to existing SQLite tables requires table rebuild/,
		);

		await driver.close();
	});
});
