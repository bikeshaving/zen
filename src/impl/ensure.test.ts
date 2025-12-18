/**
 * End-to-end tests for db.ensureTable() and db.ensureConstraints()
 *
 * Tests the full flow with real databases:
 * - SQLite: Always runs (in-memory)
 * - PostgreSQL: Runs if available (docker compose up)
 * - MySQL: Runs if available (docker compose up)
 */

import {describe, it, expect, beforeEach, afterAll, beforeAll} from "bun:test";
import {Database, table, z} from "../zen.js";
import BunDriver from "../bun.js";

// =============================================================================
// Test Database Configurations
// =============================================================================

interface TestDialect {
	name: string;
	url: string;
	available: boolean;
	driver?: BunDriver;
}

const dialects: TestDialect[] = [
	{name: "sqlite", url: ":memory:", available: true},
	{
		name: "postgresql",
		url: "postgres://zealot:zealot@localhost:5432/zealot_test",
		available: false,
	},
	{
		name: "mysql",
		url: "mysql://zealot:zealot@localhost:3306/zealot_test",
		available: false,
	},
];

// Check which databases are available before running tests
beforeAll(async () => {
	for (const dialect of dialects) {
		if (dialect.name === "sqlite") continue; // SQLite always available

		try {
			const driver = new BunDriver(dialect.url);
			// Try a simple query to verify connection
			const strings = ["SELECT 1 as test"] as unknown as TemplateStringsArray;
			await driver.all(strings, []);
			dialect.available = true;
			dialect.driver = driver;
		} catch {
			// Database not available, will skip tests
			dialect.available = false;
		}
	}

	const available = dialects.filter((d) => d.available).map((d) => d.name);
	const skipped = dialects.filter((d) => !d.available).map((d) => d.name);

	if (skipped.length > 0) {
		console.log(`\n  Dialects available: ${available.join(", ")}`);
		console.log(`  Dialects skipped (not running): ${skipped.join(", ")}`);
		console.log(`  Run 'docker compose up -d' to test all dialects\n`);
	}
});

afterAll(async () => {
	// Close any open connections
	for (const dialect of dialects) {
		if (dialect.driver) {
			await dialect.driver.close();
		}
	}
});

// =============================================================================
// Helper to run tests for each available dialect
// =============================================================================

function describeDialect(
	dialectName: string,
	fn: (getDb: () => Promise<{driver: BunDriver; db: Database}>) => void,
) {
	const dialect = dialects.find((d) => d.name === dialectName)!;

	describe(`[${dialectName}]`, () => {
		if (!dialect.available && dialectName !== "sqlite") {
			it.skip("database not available", () => {});
			return;
		}

		let driver: BunDriver;
		let db: Database;

		const getDb = async () => {
			if (dialectName === "sqlite") {
				// Fresh in-memory database for each call
				driver = new BunDriver(":memory:");
				db = new Database(driver);
				await db.open(1);
			} else {
				// Reuse connection but with unique table names
				if (!driver) {
					driver = new BunDriver(dialect.url);
					db = new Database(driver);
					await db.open(1);
				}
			}
			return {driver, db};
		};

		fn(getDb);
	});
}

// =============================================================================
// Tests for each dialect
// =============================================================================

for (const dialectConfig of dialects) {
	describeDialect(dialectConfig.name, (getDb) => {
		describe("ensureTable", () => {
			let db: Database;
			let testId = 0;

			beforeEach(async () => {
				const result = await getDb();
				db = result.db;
				testId++;
			});

			it("creates a new table with all columns", async () => {
				const Users = table(`users_${testId}`, {
					id: z.string().db.primary(),
					name: z.string(),
					email: z.string(),
				});

				const result = await db.ensureTable(Users);

				expect(result.applied).toBe(true);

				// Verify table was created by inserting a row
				const user = await db.insert(Users, {
					id: "1",
					name: "Alice",
					email: "alice@example.com",
				});
				expect(user.id).toBe("1");
			});

			it("creates a table with primary key", async () => {
				const Users = table(`users_pk_${testId}`, {
					id: z.string().db.primary(),
					name: z.string(),
				});

				await db.ensureTable(Users);

				// Insert should work
				await db.insert(Users, {id: "1", name: "Alice"});

				// Duplicate primary key should fail
				await expect(
					db.insert(Users, {id: "1", name: "Bob"}),
				).rejects.toThrow();
			});

			it("creates a table with indexes", async () => {
				const Users = table(`users_idx_${testId}`, {
					id: z.string().db.primary(),
					email: z.string().db.index(),
				});

				const result = await db.ensureTable(Users);

				expect(result.applied).toBe(true);

				// Verify index exists by checking performance characteristics
				// (or we could query sqlite_master)
				await db.insert(Users, {id: "1", email: "alice@example.com"});
			});

			it("creates a table with unique constraint", async () => {
				const Users = table(`users_uniq_${testId}`, {
					id: z.string().db.primary(),
					email: z.string().db.unique(),
				});

				await db.ensureTable(Users);

				await db.insert(Users, {id: "1", email: "alice@example.com"});

				// Duplicate email should fail
				await expect(
					db.insert(Users, {id: "2", email: "alice@example.com"}),
				).rejects.toThrow();
			});

			it("creates a table with foreign key", async () => {
				const Users = table(`users_fk_${testId}`, {
					id: z.string().db.primary(),
					name: z.string(),
				});

				const Posts = table(`posts_fk_${testId}`, {
					id: z.string().db.primary(),
					authorId: z.string().db.references(Users, {as: "author"}),
					title: z.string(),
				});

				// Create Users first (FK dependency)
				await db.ensureTable(Users);
				await db.ensureTable(Posts);

				// Insert user
				await db.insert(Users, {id: "1", name: "Alice"});

				// Insert post with valid author
				await db.insert(Posts, {id: "1", authorId: "1", title: "Hello"});

				// Note: FK enforcement varies by dialect/config
			});

			it("is idempotent - calling twice does nothing", async () => {
				const Users = table(`users_idemp_${testId}`, {
					id: z.string().db.primary(),
					name: z.string(),
				});

				const result1 = await db.ensureTable(Users);
				expect(result1.applied).toBe(true);

				const result2 = await db.ensureTable(Users);
				expect(result2.applied).toBe(false);
			});

			it("adds missing columns to existing table", async () => {
				// Create initial table with just id
				const tableName = `users_evolve_${testId}`;
				const UsersV1 = table(tableName, {
					id: z.string().db.primary(),
				});

				await db.ensureTable(UsersV1);
				await db.insert(UsersV1, {id: "1"});

				// Evolve schema to add email column
				const UsersV2 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string().optional(),
				});

				const result = await db.ensureTable(UsersV2);
				expect(result.applied).toBe(true);

				// Insert with new column should work
				await db.insert(UsersV2, {id: "2", email: "bob@example.com"});

				// Old row should still be readable
				const user = await db.get(UsersV2, "1");
				expect(user).toBeDefined();
				expect(user!.id).toBe("1");
			});

			it("adds missing non-unique indexes to existing table", async () => {
				const tableName = `users_addidx_${testId}`;
				const UsersV1 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string(),
				});

				await db.ensureTable(UsersV1);

				// Add index
				const UsersV2 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string().db.index(),
				});

				const result = await db.ensureTable(UsersV2);
				expect(result.applied).toBe(true);
			});

			it("throws SchemaDriftError when existing table missing unique constraint", async () => {
				// Create table without unique constraint
				const tableName = `users_drift_${testId}`;
				const UsersV1 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string(),
				});

				await db.ensureTable(UsersV1);

				// Try to ensure with unique constraint
				const UsersV2 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string().db.unique(),
				});

				await expect(db.ensureTable(UsersV2)).rejects.toMatchObject({
					name: "SchemaDriftError",
					drift: "missing unique:email",
				});
			});
		});

		describe("ensureConstraints", () => {
			let db: Database;
			let testId = 0;

			beforeEach(async () => {
				const result = await getDb();
				db = result.db;
				testId++;
			});

			it("adds unique constraint to existing table", async () => {
				// Create table without unique constraint
				const tableName = `users_adduniq_${testId}`;
				const UsersV1 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string(),
				});

				await db.ensureTable(UsersV1);
				await db.insert(UsersV1, {id: "1", email: "alice@example.com"});

				// Schema with unique constraint
				const UsersV2 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string().db.unique(),
				});

				// This should work since there are no duplicates
				const result = await db.ensureConstraints(UsersV2);
				expect(result.applied).toBe(true);

				// Now duplicate should fail
				await expect(
					db.insert(UsersV2, {id: "2", email: "alice@example.com"}),
				).rejects.toThrow();
			});

			it("throws ConstraintPreflightError when duplicates exist", async () => {
				const tableName = `users_dups_${testId}`;
				const UsersV1 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string(),
				});

				await db.ensureTable(UsersV1);

				// Insert duplicate emails
				await db.insert(UsersV1, {id: "1", email: "same@example.com"});
				await db.insert(UsersV1, {id: "2", email: "same@example.com"});

				// Schema with unique constraint
				const UsersV2 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string().db.unique(),
				});

				await expect(db.ensureConstraints(UsersV2)).rejects.toMatchObject({
					name: "ConstraintPreflightError",
					constraint: "unique:email",
				});
			});

			it("is idempotent when constraints already exist", async () => {
				const Users = table(`users_constidemp_${testId}`, {
					id: z.string().db.primary(),
					email: z.string().db.unique(),
				});

				// Create table with constraint
				await db.ensureTable(Users);

				// ensureConstraints should be a no-op
				const result = await db.ensureConstraints(Users);
				expect(result.applied).toBe(false);
			});

			it("throws when table does not exist", async () => {
				const Users = table(`nonexistent_${testId}`, {
					id: z.string().db.primary(),
					email: z.string().db.unique(),
				});

				await expect(db.ensureConstraints(Users)).rejects.toThrow(
					/does not exist/,
				);
			});
		});

		describe("copyColumn", () => {
			let db: Database;
			let testId = 0;

			beforeEach(async () => {
				const result = await getDb();
				db = result.db;
				testId++;
			});

			it("copies data from old column to new column", async () => {
				// Create table with old column name
				const tableName = `users_copy_${testId}`;
				const UsersV1 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string(),
				});

				await db.ensureTable(UsersV1);
				await db.insert(UsersV1, {id: "1", email: "alice@example.com"});
				await db.insert(UsersV1, {id: "2", email: "bob@example.com"});

				// Add new column
				const UsersV2 = table(tableName, {
					id: z.string().db.primary(),
					email: z.string(),
					emailAddress: z.string().optional(),
				});

				await db.ensureTable(UsersV2);

				// Copy data
				const updated = await db.copyColumn(UsersV2, "email", "emailAddress");
				expect(updated).toBe(2);

				// Verify data was copied
				const user = await db.get(UsersV2, "1");
				expect(user?.emailAddress).toBe("alice@example.com");
			});

			it("is idempotent - only copies where target is NULL", async () => {
				const tableName = `users_copyidemp_${testId}`;
				const Users = table(tableName, {
					id: z.string().db.primary(),
					oldName: z.string(),
					newName: z.string().nullable(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", oldName: "Alice", newName: null});
				await db.insert(Users, {id: "2", oldName: "Bob", newName: "Robert"});

				// First copy
				const updated1 = await db.copyColumn(Users, "oldName", "newName");
				expect(updated1).toBe(1); // Only row 1 updated

				// Second copy should be no-op
				const updated2 = await db.copyColumn(Users, "oldName", "newName");
				expect(updated2).toBe(0);
			});
		});
	});
}
