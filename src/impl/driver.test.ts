/**
 * Multi-dialect driver tests
 *
 * Tests real database behavior across SQLite, PostgreSQL, and MySQL.
 * Validates CRUD operations, data types, transactions, and error handling.
 *
 * - SQLite: Always runs (in-memory)
 * - PostgreSQL: Runs if available (docker compose up)
 * - MySQL: Runs if available (docker compose up)
 */

import {describe, it, expect, afterAll, beforeAll, beforeEach} from "bun:test";
import {Database, table, z, ident} from "../zen.js";
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
		url: "postgres://testuser:testpass@localhost:15432/test_db",
		available: false,
	},
	{
		name: "mysql",
		url: "mysql://testuser:testpass@localhost:13306/test_db",
		available: false,
	},
];

// Helper for string IDs - MySQL requires VARCHAR (not TEXT) for indexed columns
const stringId = () => z.string().max(255);
const stringField = () => z.string().max(255);

// Unique run ID to avoid table name conflicts across test runs
const runId = Date.now().toString(36);

// Check which databases are available before running tests
beforeAll(async () => {
	for (const dialect of dialects) {
		if (dialect.name === "sqlite") continue;

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
});

// =============================================================================
// Test Definitions
// =============================================================================

for (const dialect of dialects) {
	describe(`[${dialect.name}]`, () => {
		let testId = 0;
		let driver: BunDriver;
		let db: Database;

		beforeEach(async () => {
			if (!dialect.available) return;

			// Close previous driver if exists
			if (driver) {
				await driver.close();
			}

			// Create fresh driver and db for each test
			driver = new BunDriver(dialect.url);
			db = new Database(driver);
			await db.open(1);
			testId++;
		});

		afterAll(async () => {
			if (driver) {
				await driver.close();
			}
		});

		const maybeSkip = () => !dialect.available;

		// =========================================================================
		// CRUD Operations
		// =========================================================================

		describe("CRUD", () => {
			it("insert and get", async () => {
				if (maybeSkip()) return;

				const Users = table(`crud_insert_${runId}_${testId}`, {
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
			});

			it("update", async () => {
				if (maybeSkip()) return;

				const Users = table(`crud_update_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", name: "Alice"});

				// API: update(table, data, id)
				const updated = await db.update(Users, {name: "Alicia"}, "1");
				expect(updated!.name).toBe("Alicia");

				const fetched = await db.get(Users, "1");
				expect(fetched!.name).toBe("Alicia");
			});

			it("delete", async () => {
				if (maybeSkip()) return;

				const Users = table(`crud_delete_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", name: "Alice"});

				await db.delete(Users, "1");

				const fetched = await db.get(Users, "1");
				expect(fetched).toBeNull();
			});

			it("all with where clause", async () => {
				if (maybeSkip()) return;

				const Users = table(`crud_all_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
					age: z.number().int(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", name: "Alice", age: 30});
				await db.insert(Users, {id: "2", name: "Bob", age: 25});
				await db.insert(Users, {id: "3", name: "Charlie", age: 35});

				// Get all using tagged template
				const all = await db.all(Users)``;
				expect(all.length).toBe(3);

				// Filter with where clause (raw SQL in tagged template)
				const older = await db.all(Users)`WHERE age >= ${30}`;
				expect(older.length).toBe(2);
			});

			it("count via query()", async () => {
				if (maybeSkip()) return;

				const tableName = `crud_count_${runId}_${testId}`;
				const Users = table(tableName, {
					id: stringId().db.primary(),
					active: z.boolean(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", active: true});
				await db.insert(Users, {id: "2", active: false});
				await db.insert(Users, {id: "3", active: true});

				// Count using query() with ident() for dialect-aware quoting
				const rows = await db.query<{
					cnt: number | string;
				}>`SELECT COUNT(*) as cnt FROM ${ident(tableName)}`;
				// PostgreSQL returns bigint as string, so convert to number
				expect(Number(rows[0].cnt)).toBe(3);
			});

			it("exists check via get", async () => {
				if (maybeSkip()) return;

				const Users = table(`crud_exists_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);

				expect(await db.get(Users, "1")).toBeNull();

				await db.insert(Users, {id: "1", name: "Alice"});

				expect(await db.get(Users, "1")).not.toBeNull();
			});
		});

		// =========================================================================
		// Data Types
		// =========================================================================

		describe("Data Types", () => {
			it("handles strings", async () => {
				if (maybeSkip()) return;

				const Strings = table(`types_string_${runId}_${testId}`, {
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
			});

			it("handles integers", async () => {
				if (maybeSkip()) return;

				const Numbers = table(`types_int_${runId}_${testId}`, {
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
			});

			it("handles floats", async () => {
				if (maybeSkip()) return;

				const Floats = table(`types_float_${runId}_${testId}`, {
					id: stringId().db.primary(),
					price: z.number(),
					ratio: z.number(),
				});

				await db.ensureTable(Floats);

				const data = {
					id: "1",
					price: 19.99,
					ratio: 0.123456789,
				};

				await db.insert(Floats, data);
				const fetched = await db.get(Floats, "1");

				expect(fetched!.price).toBeCloseTo(data.price, 2);
				expect(fetched!.ratio).toBeCloseTo(data.ratio, 6);
			});

			it("handles booleans", async () => {
				if (maybeSkip()) return;

				const Booleans = table(`types_bool_${runId}_${testId}`, {
					id: stringId().db.primary(),
					active: z.boolean(),
					verified: z.boolean(),
				});

				await db.ensureTable(Booleans);

				await db.insert(Booleans, {id: "1", active: true, verified: false});
				await db.insert(Booleans, {id: "2", active: false, verified: true});

				const row1 = await db.get(Booleans, "1");
				// SQLite stores as INTEGER (0/1), need to check truthy/falsy
				expect(!!row1!.active).toBe(true);
				expect(!!row1!.verified).toBe(false);

				const row2 = await db.get(Booleans, "2");
				expect(!!row2!.active).toBe(false);
				expect(!!row2!.verified).toBe(true);
			});

			it("handles enums", async () => {
				if (maybeSkip()) return;

				const Status = z.enum(["pending", "active", "completed"]);
				const Tasks = table(`types_enum_${runId}_${testId}`, {
					id: stringId().db.primary(),
					status: Status,
				});

				await db.ensureTable(Tasks);

				await db.insert(Tasks, {id: "1", status: "pending"});
				await db.insert(Tasks, {id: "2", status: "active"});

				const task1 = await db.get(Tasks, "1");
				expect(task1!.status).toBe("pending");

				const task2 = await db.get(Tasks, "2");
				expect(task2!.status).toBe("active");
			});

			it("handles nullable fields", async () => {
				if (maybeSkip()) return;

				const Nullable = table(`types_nullable_${runId}_${testId}`, {
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
			});

			it("handles optional fields", async () => {
				if (maybeSkip()) return;

				const Optional = table(`types_optional_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
					nickname: stringField().optional(),
				});

				await db.ensureTable(Optional);

				// Insert without optional field
				await db.insert(Optional, {id: "1", name: "Alice"});

				// Insert with optional field
				await db.insert(Optional, {id: "2", name: "Bob", nickname: "Bobby"});

				const row1 = await db.get(Optional, "1");
				expect(row1!.name).toBe("Alice");
				// Optional fields stored as NULL, returned as null or undefined
				expect(row1!.nickname == null).toBe(true);

				const row2 = await db.get(Optional, "2");
				expect(row2!.name).toBe("Bob");
				expect(row2!.nickname).toBe("Bobby");
			});

			it("handles db.inserted() defaults", async () => {
				if (maybeSkip()) return;

				const Defaults = table(`types_default_${runId}_${testId}`, {
					id: stringId().db.primary(),
					status: stringField().db.inserted(() => "pending"),
					count: z
						.number()
						.int()
						.db.inserted(() => 0),
				});

				await db.ensureTable(Defaults);

				// Insert using defaults (don't provide status or count)
				await db.insert(Defaults, {id: "1"});

				const row = await db.get(Defaults, "1");
				expect(row!.status).toBe("pending");
				expect(row!.count).toBe(0);
			});
		});

		// =========================================================================
		// Transactions
		// =========================================================================

		describe("Transactions", () => {
			it("commits on success", async () => {
				if (maybeSkip()) return;

				const Users = table(`tx_commit_${runId}_${testId}`, {
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
			});

			it("rolls back on error", async () => {
				if (maybeSkip()) return;

				const Users = table(`tx_rollback_${runId}_${testId}`, {
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
			});

			it("isolates changes until commit", async () => {
				if (maybeSkip()) return;

				const Users = table(`tx_isolate_${runId}_${testId}`, {
					id: stringId().db.primary(),
					balance: z.number().int(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", balance: 100});

				await db.transaction(async (tx) => {
					const user = await tx.get(Users, "1");
					const currentBalance = (user as {balance: number})!.balance;
					await tx.update(Users, {balance: currentBalance - 50}, "1");

					// Within transaction, balance is updated
					const updated = await tx.get(Users, "1");
					expect((updated as {balance: number})!.balance).toBe(50);
				});

				// After commit, change persists
				const final = await db.get(Users, "1");
				expect(final!.balance).toBe(50);
			});
		});

		// =========================================================================
		// Error Handling
		// =========================================================================

		describe("Errors", () => {
			it("throws on duplicate primary key", async () => {
				if (maybeSkip()) return;

				const Users = table(`err_dup_pk_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", name: "Alice"});

				await expect(
					db.insert(Users, {id: "1", name: "Bob"}),
				).rejects.toThrow();
			});

			it("throws on duplicate unique field", async () => {
				if (maybeSkip()) return;

				const Users = table(`err_dup_uniq_${runId}_${testId}`, {
					id: stringId().db.primary(),
					email: stringField().db.unique(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", email: "alice@test.com"});

				await expect(
					db.insert(Users, {id: "2", email: "alice@test.com"}),
				).rejects.toThrow();
			});

			it("returns null for non-existent get", async () => {
				if (maybeSkip()) return;

				const Users = table(`err_notfound_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);

				const user = await db.get(Users, "nonexistent");
				expect(user).toBeNull();
			});

			it("returns null for update non-existent", async () => {
				if (maybeSkip()) return;

				const Users = table(`err_update_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);

				// update returns null when record not found
				const result = await db.update(Users, {name: "Alice"}, "nonexistent");
				expect(result).toBeNull();
			});
		});

		// =========================================================================
		// Foreign Keys
		// =========================================================================

		describe("Foreign Keys", () => {
			it("creates tables with references", async () => {
				if (maybeSkip()) return;

				const Users = table(`fk_users_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				const Posts = table(`fk_posts_${runId}_${testId}`, {
					id: stringId().db.primary(),
					authorId: stringField().db.references(Users, {as: "author"}),
					title: stringField(),
				});

				await db.ensureTable(Users);
				await db.ensureTable(Posts);

				// Create a user first
				await db.insert(Users, {id: "1", name: "Alice"});

				// Can create post with valid author
				const post = await db.insert(Posts, {
					id: "1",
					authorId: "1",
					title: "Hello",
				});
				expect(post.authorId).toBe("1");

				// Note: FK enforcement depends on DB config
				// SQLite requires PRAGMA foreign_keys=ON (not enabled by default)
				// PostgreSQL and MySQL enforce by default
			});
		});

		// =========================================================================
		// Query Operators (raw SQL)
		// =========================================================================

		describe("Query Operators", () => {
			it("equality and inequality", async () => {
				if (maybeSkip()) return;

				const Users = table(`op_eq_${runId}_${testId}`, {
					id: stringId().db.primary(),
					status: stringField(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", status: "active"});
				await db.insert(Users, {id: "2", status: "inactive"});

				const active = await db.all(Users)`WHERE status = ${"active"}`;
				expect(active.length).toBe(1);
				expect(active[0].id).toBe("1");

				const notActive = await db.all(Users)`WHERE status != ${"active"}`;
				expect(notActive.length).toBe(1);
				expect(notActive[0].id).toBe("2");
			});

			it("comparison operators", async () => {
				if (maybeSkip()) return;

				const Items = table(`op_cmp_${runId}_${testId}`, {
					id: stringId().db.primary(),
					price: z.number(),
				});

				await db.ensureTable(Items);
				await db.insert(Items, {id: "1", price: 10});
				await db.insert(Items, {id: "2", price: 20});
				await db.insert(Items, {id: "3", price: 30});

				const gt15 = await db.all(Items)`WHERE price > ${15}`;
				expect(gt15.length).toBe(2);

				const gte20 = await db.all(Items)`WHERE price >= ${20}`;
				expect(gte20.length).toBe(2);

				const lt20 = await db.all(Items)`WHERE price < ${20}`;
				expect(lt20.length).toBe(1);

				const lte20 = await db.all(Items)`WHERE price <= ${20}`;
				expect(lte20.length).toBe(2);
			});

			it("LIKE pattern matching", async () => {
				if (maybeSkip()) return;

				const Users = table(`op_like_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", name: "Alice"});
				await db.insert(Users, {id: "2", name: "Alicia"});
				await db.insert(Users, {id: "3", name: "Bob"});

				const aliNames = await db.all(Users)`WHERE name LIKE ${"Ali%"}`;
				expect(aliNames.length).toBe(2);
			});

			it("IN clause", async () => {
				if (maybeSkip()) return;

				const Users = table(`op_in_${runId}_${testId}`, {
					id: stringId().db.primary(),
					role: stringField(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", role: "admin"});
				await db.insert(Users, {id: "2", role: "user"});
				await db.insert(Users, {id: "3", role: "guest"});

				const adminOrUser = await db.all(
					Users,
				)`WHERE role IN (${"admin"}, ${"user"})`;
				expect(adminOrUser.length).toBe(2);
			});

			it("NULL checks", async () => {
				if (maybeSkip()) return;

				const Users = table(`op_null_${runId}_${testId}`, {
					id: stringId().db.primary(),
					nickname: z.string().nullable(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", nickname: "Ali"});
				await db.insert(Users, {id: "2", nickname: null});

				const withNickname = await db.all(Users)`WHERE nickname IS NOT NULL`;
				expect(withNickname.length).toBe(1);

				const withoutNickname = await db.all(Users)`WHERE nickname IS NULL`;
				expect(withoutNickname.length).toBe(1);
			});
		});

		// =========================================================================
		// Ordering and Pagination
		// =========================================================================

		describe("Ordering and Pagination", () => {
			it("orderBy ascending", async () => {
				if (maybeSkip()) return;

				const Users = table(`order_asc_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", name: "Charlie"});
				await db.insert(Users, {id: "2", name: "Alice"});
				await db.insert(Users, {id: "3", name: "Bob"});

				const sorted = await db.all(Users)`ORDER BY name ASC`;

				expect(sorted[0].name).toBe("Alice");
				expect(sorted[1].name).toBe("Bob");
				expect(sorted[2].name).toBe("Charlie");
			});

			it("orderBy descending", async () => {
				if (maybeSkip()) return;

				const Items = table(`order_desc_${runId}_${testId}`, {
					id: stringId().db.primary(),
					price: z.number(),
				});

				await db.ensureTable(Items);
				await db.insert(Items, {id: "1", price: 10});
				await db.insert(Items, {id: "2", price: 30});
				await db.insert(Items, {id: "3", price: 20});

				const sorted = await db.all(Items)`ORDER BY price DESC`;

				expect(sorted[0].price).toBe(30);
				expect(sorted[1].price).toBe(20);
				expect(sorted[2].price).toBe(10);
			});

			it("limit", async () => {
				if (maybeSkip()) return;

				const Users = table(`limit_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", name: "Alice"});
				await db.insert(Users, {id: "2", name: "Bob"});
				await db.insert(Users, {id: "3", name: "Charlie"});

				const limited = await db.all(Users)`LIMIT 2`;
				expect(limited.length).toBe(2);
			});

			it("offset", async () => {
				if (maybeSkip()) return;

				const Users = table(`offset_${runId}_${testId}`, {
					id: stringId().db.primary(),
					name: stringField(),
				});

				await db.ensureTable(Users);
				await db.insert(Users, {id: "1", name: "Alice"});
				await db.insert(Users, {id: "2", name: "Bob"});
				await db.insert(Users, {id: "3", name: "Charlie"});

				const page2 = await db.all(Users)`ORDER BY name ASC LIMIT 2 OFFSET 1`;

				expect(page2.length).toBe(2);
				expect(page2[0].name).toBe("Bob");
				expect(page2[1].name).toBe("Charlie");
			});
		});
	});
}
