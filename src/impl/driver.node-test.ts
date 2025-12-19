/**
 * Node.js driver tests for SQLite, PostgreSQL, and MySQL
 *
 * Mirrors the key tests from driver.test.ts but uses Node drivers instead of BunDriver.
 * Validates CRUD operations, data types, transactions, and error handling.
 *
 * Run with: npm run test:node
 */

import {describe, test, expect} from "./node-test-utils.js";
import {Database, table, ident} from "../zen.js";
import {z} from "zod";
import type {Driver} from "./database.js";

// Connection strings matching docker-compose.yml
const SQLITE_URL = ":memory:";
const POSTGRES_URL = "postgresql://testuser:testpass@localhost:15432/test_db";
const MYSQL_URL = "mysql://testuser:testpass@localhost:13306/test_db";

// Helper for string IDs - MySQL requires VARCHAR (not TEXT) for indexed columns
const stringId = () => z.string().max(255);
const stringField = () => z.string().max(255);

// Unique run ID to avoid table name conflicts
const runId = Date.now().toString(36);

interface DialectConfig {
	name: string;
	url: string;
	available: boolean;
	createDriver: () => Promise<Driver>;
}

// Check database availability and set up dialect configs
const dialects: DialectConfig[] = [];

// SQLite (always available with better-sqlite3)
try {
	const {default: SQLiteDriver} = await import("../sqlite.js");
	dialects.push({
		name: "sqlite",
		url: SQLITE_URL,
		available: true,
		createDriver: async () => new SQLiteDriver(SQLITE_URL),
	});
} catch {
	// better-sqlite3 not installed
}

// PostgreSQL
try {
	const {default: PostgresDriver} = await import("../postgres.js");
	const testDriver = new PostgresDriver(POSTGRES_URL);
	try {
		await testDriver.run(["SELECT 1"] as any, []);
		await testDriver.close();
		dialects.push({
			name: "postgresql",
			url: POSTGRES_URL,
			available: true,
			createDriver: async () => new PostgresDriver(POSTGRES_URL),
		});
	} catch {
		// PostgreSQL not running
	}
} catch {
	// postgres package not installed
}

// MySQL
try {
	const {default: MySQLDriver} = await import("../mysql.js");
	const testDriver = new MySQLDriver(MYSQL_URL);
	try {
		await testDriver.run(["SELECT 1"] as any, []);
		await testDriver.close();
		dialects.push({
			name: "mysql",
			url: MYSQL_URL,
			available: true,
			createDriver: async () => new MySQLDriver(MYSQL_URL),
		});
	} catch {
		// MySQL not running
	}
} catch {
	// mysql2 package not installed
}

console.log(
	`\n  Node Driver Tests: ${dialects.map((d) => d.name).join(", ") || "none available"}`,
);

// Run tests for each available dialect
for (const dialect of dialects) {
	let testId = 0;

	describe(`[${dialect.name}] Node Driver`, () => {
		// =========================================================================
		// CRUD Operations
		// =========================================================================

		describe("CRUD", () => {
			test("insert and get", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Users = table(`node_crud_insert_${runId}_${testId}`, {
						id: stringId().db.primary(),
						name: stringField(),
						email: stringField(),
					});

					await db.ensureTable(Users);

					const inserted = await db.insert(Users, {
						id: "1",
						name: "Alice",
						email: "alice@test.com",
					});

					expect(inserted.id).toBe("1");
					expect(inserted.name).toBe("Alice");
					expect(inserted.email).toBe("alice@test.com");

					const fetched = await db.get(Users, "1");
					expect(fetched).not.toBeNull();
					expect(fetched!.name).toBe("Alice");
				} finally {
					await driver.close();
				}
			});

			test("update", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Users = table(`node_crud_update_${runId}_${testId}`, {
						id: stringId().db.primary(),
						name: stringField(),
					});

					await db.ensureTable(Users);
					await db.insert(Users, {id: "1", name: "Alice"});

					const updated = await db.update(Users, {name: "Alicia"}, "1");
					expect(updated!.name).toBe("Alicia");

					const fetched = await db.get(Users, "1");
					expect(fetched!.name).toBe("Alicia");
				} finally {
					await driver.close();
				}
			});

			test("delete", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Users = table(`node_crud_delete_${runId}_${testId}`, {
						id: stringId().db.primary(),
						name: stringField(),
					});

					await db.ensureTable(Users);
					await db.insert(Users, {id: "1", name: "Alice"});

					await db.delete(Users, "1");

					const fetched = await db.get(Users, "1");
					expect(fetched).toBeNull();
				} finally {
					await driver.close();
				}
			});

			test("all with where clause", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Users = table(`node_crud_all_${runId}_${testId}`, {
						id: stringId().db.primary(),
						name: stringField(),
						age: z.number().int(),
					});

					await db.ensureTable(Users);
					await db.insert(Users, {id: "1", name: "Alice", age: 30});
					await db.insert(Users, {id: "2", name: "Bob", age: 25});
					await db.insert(Users, {id: "3", name: "Charlie", age: 35});

					const all = await db.all(Users)``;
					expect(all.length).toBe(3);

					const older = await db.all(Users)`WHERE age >= ${30}`;
					expect(older.length).toBe(2);
				} finally {
					await driver.close();
				}
			});

			test("query with ident()", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const tableName = `node_crud_count_${runId}_${testId}`;
					const Users = table(tableName, {
						id: stringId().db.primary(),
						active: z.boolean(),
					});

					await db.ensureTable(Users);
					await db.insert(Users, {id: "1", active: true});
					await db.insert(Users, {id: "2", active: false});
					await db.insert(Users, {id: "3", active: true});

					const rows = await db.query<{
						cnt: number | string;
					}>`SELECT COUNT(*) as cnt FROM ${ident(tableName)}`;
					expect(Number(rows[0].cnt)).toBe(3);
				} finally {
					await driver.close();
				}
			});
		});

		// =========================================================================
		// Data Types
		// =========================================================================

		describe("Data Types", () => {
			test("handles strings", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Strings = table(`node_types_string_${runId}_${testId}`, {
						id: stringId().db.primary(),
						short: z.string().max(50),
						long: z.string(),
						unicode: z.string(),
					});

					await db.ensureTable(Strings);

					const data = {
						id: "1",
						short: "hello",
						long: "a".repeat(1000),
						unicode: "你好世界 🌍 مرحبا",
					};

					await db.insert(Strings, data);
					const fetched = await db.get(Strings, "1");

					expect(fetched!.short).toBe(data.short);
					expect(fetched!.long).toBe(data.long);
					expect(fetched!.unicode).toBe(data.unicode);
				} finally {
					await driver.close();
				}
			});

			test("handles integers", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Numbers = table(`node_types_int_${runId}_${testId}`, {
						id: stringId().db.primary(),
						small: z.number().int(),
						large: z.number().int(),
						negative: z.number().int(),
					});

					await db.ensureTable(Numbers);

					const data = {
						id: "1",
						small: 42,
						large: 2147483647,
						negative: -999999,
					};

					await db.insert(Numbers, data);
					const fetched = await db.get(Numbers, "1");

					expect(fetched!.small).toBe(data.small);
					expect(fetched!.large).toBe(data.large);
					expect(fetched!.negative).toBe(data.negative);
				} finally {
					await driver.close();
				}
			});

			test("handles booleans", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Booleans = table(`node_types_bool_${runId}_${testId}`, {
						id: stringId().db.primary(),
						active: z.boolean(),
						verified: z.boolean(),
					});

					await db.ensureTable(Booleans);

					await db.insert(Booleans, {id: "1", active: true, verified: false});
					await db.insert(Booleans, {id: "2", active: false, verified: true});

					const row1 = await db.get(Booleans, "1");
					expect(!!row1!.active).toBe(true);
					expect(!!row1!.verified).toBe(false);

					const row2 = await db.get(Booleans, "2");
					expect(!!row2!.active).toBe(false);
					expect(!!row2!.verified).toBe(true);
				} finally {
					await driver.close();
				}
			});

			test("handles nullable fields", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Nullable = table(`node_types_nullable_${runId}_${testId}`, {
						id: stringId().db.primary(),
						nickname: z.string().nullable(),
						bio: z.string().nullable(),
					});

					await db.ensureTable(Nullable);

					await db.insert(Nullable, {id: "1", nickname: "Ali", bio: null});
					await db.insert(Nullable, {id: "2", nickname: null, bio: "Hello"});

					const row1 = await db.get(Nullable, "1");
					expect(row1!.nickname).toBe("Ali");
					expect(row1!.bio).toBeNull();

					const row2 = await db.get(Nullable, "2");
					expect(row2!.nickname).toBeNull();
					expect(row2!.bio).toBe("Hello");
				} finally {
					await driver.close();
				}
			});
		});

		// =========================================================================
		// Transactions
		// =========================================================================

		describe("Transactions", () => {
			test("commits on success", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Users = table(`node_tx_commit_${runId}_${testId}`, {
						id: stringId().db.primary(),
						name: stringField(),
					});

					await db.ensureTable(Users);

					await db.transaction(async (tx) => {
						await tx.insert(Users, {id: "1", name: "Alice"});
						await tx.insert(Users, {id: "2", name: "Bob"});
					});

					const all = await db.all(Users)``;
					expect(all.length).toBe(2);
				} finally {
					await driver.close();
				}
			});

			test("rolls back on error", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Users = table(`node_tx_rollback_${runId}_${testId}`, {
						id: stringId().db.primary(),
						name: stringField(),
					});

					await db.ensureTable(Users);

					try {
						await db.transaction(async (tx) => {
							await tx.insert(Users, {id: "1", name: "Alice"});
							throw new Error("Simulated failure");
						});
					} catch {
						// Expected
					}

					const all = await db.all(Users)``;
					expect(all.length).toBe(0);
				} finally {
					await driver.close();
				}
			});
		});

		// =========================================================================
		// Error Handling
		// =========================================================================

		describe("Error Handling", () => {
			test("throws on duplicate primary key", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Users = table(`node_err_dup_${runId}_${testId}`, {
						id: stringId().db.primary(),
						name: stringField(),
					});

					await db.ensureTable(Users);
					await db.insert(Users, {id: "1", name: "Alice"});

					await expect(
						db.insert(Users, {id: "1", name: "Bob"}),
					).rejects.toThrow();
				} finally {
					await driver.close();
				}
			});

			test("throws on unique constraint violation", async () => {
				testId++;
				const driver = await dialect.createDriver();
				const db = new Database(driver);
				await db.open(1);

				try {
					const Users = table(`node_err_unique_${runId}_${testId}`, {
						id: stringId().db.primary(),
						email: stringField().db.unique(),
					});

					await db.ensureTable(Users);
					await db.ensureConstraints(Users);
					await db.insert(Users, {id: "1", email: "test@test.com"});

					await expect(
						db.insert(Users, {id: "2", email: "test@test.com"}),
					).rejects.toThrow();
				} finally {
					await driver.close();
				}
			});
		});
	});
}

// If no dialects available, create a skip placeholder
if (dialects.length === 0) {
	describe.skip("Node Driver Tests (no drivers available)", () => {
		test("skipped", () => {});
	});
}
