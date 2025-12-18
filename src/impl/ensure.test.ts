/**
 * End-to-end tests for db.ensureTable() and db.ensureConstraints()
 *
 * Tests the full flow with real databases:
 * - SQLite: Always runs (in-memory)
 * - PostgreSQL: Runs if available (docker compose up)
 * - MySQL: Runs if available (docker compose up)
 */

import {describe, it, expect, afterAll, beforeAll} from "bun:test";
import {Database, table, z} from "../zen.js";
import BunDriver from "../bun.js";

// =============================================================================
// Test Database Configurations
// =============================================================================

interface TestDialect {
	name: string;
	url: string;
	available: boolean;
}

const dialects: TestDialect[] = [
	{name: "sqlite", url: ":memory:", available: true},
	{
		name: "postgresql",
		url: "postgres://zealot:zealot@localhost:15432/zealot_test",
		available: false,
	},
	{
		name: "mysql",
		url: "mysql://zealot:zealot@localhost:13306/zealot_test",
		available: false,
	},
];

// Helper for string IDs - MySQL requires VARCHAR (not TEXT) for indexed columns
// Using max(255) generates VARCHAR(255) instead of TEXT
const stringId = () => z.string().max(255);
const stringField = () => z.string().max(255);

// Unique run ID to avoid table name conflicts across test runs
const runId = Date.now().toString(36);

// Check which databases are available before running tests
beforeAll(async () => {
	for (const dialect of dialects) {
		if (dialect.name === "sqlite") continue; // SQLite always available

		try {
			const driver = new BunDriver(dialect.url);
			const strings = ["SELECT 1 as test"] as unknown as TemplateStringsArray;
			await driver.all(strings, []);
			dialect.available = true;
			await driver.close();
		} catch {
			dialect.available = false;
		}
	}

	const available = dialects.filter((d) => d.available).map((d) => d.name);
	const skipped = dialects.filter((d) => !d.available).map((d) => d.name);

	console.log(
		`\n  Dialects: ${available.join(", ")}${skipped.length > 0 ? ` (skipped: ${skipped.join(", ")})` : ""}`,
	);
	if (skipped.length > 0) {
		console.log(`  Run 'docker compose up -d' to test all dialects\n`);
	}
});

// =============================================================================
// Helper to create a fresh database for each test
// =============================================================================

async function createTestDb(
	dialect: TestDialect,
): Promise<{driver: BunDriver; db: Database}> {
	const driver = new BunDriver(dialect.url);
	const db = new Database(driver);
	await db.open(1);
	return {driver, db};
}

// =============================================================================
// Test definitions for each dialect
// =============================================================================

for (const dialect of dialects) {
	describe(`[${dialect.name}]`, () => {
		// Track test ID for unique table names (needed for persistent DBs)
		let testId = 0;

		// Cleanup drivers after each dialect's tests
		const drivers: BunDriver[] = [];
		afterAll(async () => {
			for (const d of drivers) {
				await d.close();
			}
		});

		// Helper to skip if dialect unavailable
		const maybeSkip = () => {
			if (!dialect.available) {
				return true;
			}
			return false;
		};

		describe("ensureTable", () => {
			it("creates a new table with all columns", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const Users = table(`u_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
					email: stringField(),
				});

				const result = await db.ensureTable(Users);
				expect(result.applied).toBe(true);

				const user = await db.insert(Users, {
					id: "1",
					name: "Alice",
					email: "alice@example.com",
				});
				expect(user.id).toBe("1");
			});

			it("creates a table with primary key", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const Users = table(`upk_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: z.string(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", name: "Alice"});

				await expect(
					db.insert(Users, {id: "1", name: "Bob"}),
				).rejects.toThrow();
			});

			it("creates a table with indexes", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const Users = table(`uidx_${runId}_${testId}`, {
					id: stringId().db.primary(),
					email: stringField().db.index(),
				});

				const result = await db.ensureTable(Users);
				expect(result.applied).toBe(true);

				await db.insert(Users, {id: "1", email: "alice@example.com"});
			});

			it("creates a table with unique constraint", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const Users = table(`uuniq_${runId}_${testId}`, {
					id: stringId().db.primary(),
					email: stringField().db.unique(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", email: "alice@example.com"});

				await expect(
					db.insert(Users, {id: "2", email: "alice@example.com"}),
				).rejects.toThrow();
			});

			it("creates a table with foreign key", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const Users = table(`ufk_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: z.string(),
				});

				const Posts = table(`pfk_${runId}_${testId}`, {
					id: stringId().db.primary(),
					authorId: stringField().db.references(Users, {as: "author"}),
					title: z.string(),
				});

				await db.ensureTable(Users);
				await db.ensureTable(Posts);

				await db.insert(Users, {id: "1", name: "Alice"});
				await db.insert(Posts, {id: "1", authorId: "1", title: "Hello"});
			});

			it("is idempotent - calling twice does nothing", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const Users = table(`uidem_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: z.string(),
				});

				const result1 = await db.ensureTable(Users);
				expect(result1.applied).toBe(true);

				const result2 = await db.ensureTable(Users);
				expect(result2.applied).toBe(false);
			});

			it("adds missing columns to existing table", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const tableName = `uevol_${runId}_${testId}`;
				const UsersV1 = table(tableName, {
					id: stringId().db.primary(),
				});

				await db.ensureTable(UsersV1);
				await db.insert(UsersV1, {id: "1"});

				const UsersV2 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField().optional(),
				});

				const result = await db.ensureTable(UsersV2);
				expect(result.applied).toBe(true);

				await db.insert(UsersV2, {id: "2", email: "bob@example.com"});

				const user = await db.get(UsersV2, "1");
				expect(user).toBeDefined();
				expect(user!.id).toBe("1");
			});

			it("adds missing non-unique indexes to existing table", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const tableName = `uaddidx_${runId}_${testId}`;
				const UsersV1 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField(),
				});

				await db.ensureTable(UsersV1);

				const UsersV2 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField().db.index(),
				});

				const result = await db.ensureTable(UsersV2);
				expect(result.applied).toBe(true);
			});

			it("throws SchemaDriftError when existing table missing unique constraint", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const tableName = `udrift_${runId}_${testId}`;
				const UsersV1 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField(),
				});

				await db.ensureTable(UsersV1);

				const UsersV2 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField().db.unique(),
				});

				await expect(db.ensureTable(UsersV2)).rejects.toMatchObject({
					name: "SchemaDriftError",
					drift: "missing unique:email",
				});
			});
		});

		describe("ensureConstraints", () => {
			it("adds unique constraint to existing table", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const tableName = `uadduniq_${runId}_${testId}`;
				const UsersV1 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField(),
				});

				await db.ensureTable(UsersV1);
				await db.insert(UsersV1, {id: "1", email: "alice@example.com"});

				const UsersV2 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField().db.unique(),
				});

				const result = await db.ensureConstraints(UsersV2);
				expect(result.applied).toBe(true);

				await expect(
					db.insert(UsersV2, {id: "2", email: "alice@example.com"}),
				).rejects.toThrow();
			});

			it("throws ConstraintPreflightError when duplicates exist", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const tableName = `udups_${runId}_${testId}`;
				const UsersV1 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField(),
				});

				await db.ensureTable(UsersV1);
				await db.insert(UsersV1, {id: "1", email: "same@example.com"});
				await db.insert(UsersV1, {id: "2", email: "same@example.com"});

				const UsersV2 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField().db.unique(),
				});

				await expect(db.ensureConstraints(UsersV2)).rejects.toMatchObject({
					name: "ConstraintPreflightError",
					constraint: "unique:email",
				});
			});

			it("is idempotent when constraints already exist", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const Users = table(`uconst_${runId}_${testId}`, {
					id: stringId().db.primary(),
					email: stringField().db.unique(),
				});

				await db.ensureTable(Users);

				const result = await db.ensureConstraints(Users);
				expect(result.applied).toBe(false);
			});

			it("throws when table does not exist", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const Users = table(`nonex_${runId}_${testId}`, {
					id: stringId().db.primary(),
					email: stringField().db.unique(),
				});

				await expect(db.ensureConstraints(Users)).rejects.toThrow(
					/does not exist/,
				);
			});
		});

		describe("copyColumn", () => {
			it("copies data from old column to new column", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const tableName = `ucopy_${runId}_${testId}`;
				const UsersV1 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField(),
				});

				await db.ensureTable(UsersV1);
				await db.insert(UsersV1, {id: "1", email: "alice@example.com"});
				await db.insert(UsersV1, {id: "2", email: "bob@example.com"});

				const UsersV2 = table(tableName, {
					id: stringId().db.primary(),
					email: stringField(),
					emailAddress: z.string().optional(),
				});

				await db.ensureTable(UsersV2);

				const updated = await db.copyColumn(UsersV2, "email", "emailAddress");
				expect(updated).toBe(2);

				const user = await db.get(UsersV2, "1");
				expect(user?.emailAddress).toBe("alice@example.com");
			});

			it("is idempotent - only copies where target is NULL", async () => {
				if (maybeSkip()) return;
				testId++;

				const {driver, db} = await createTestDb(dialect);
				drivers.push(driver);

				const tableName = `ucopyid_${runId}_${testId}`;
				const Users = table(tableName, {
					id: stringId().db.primary(),
					oldName: z.string(),
					newName: z.string().nullable(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", oldName: "Alice", newName: null});
				await db.insert(Users, {id: "2", oldName: "Bob", newName: "Robert"});

				const updated1 = await db.copyColumn(Users, "oldName", "newName");
				expect(updated1).toBe(1);

				const updated2 = await db.copyColumn(Users, "oldName", "newName");
				expect(updated2).toBe(0);
			});
		});
	});
}
