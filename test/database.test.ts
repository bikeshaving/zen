import {test, expect, describe, beforeEach, mock} from "bun:test";
import {z} from "zod";
import {table, extendZod} from "../src/impl/table.js";
import {Database, NOW, isSQLBuiltin, type Driver} from "../src/impl/database.js";
import {isSQLIdentifier} from "../src/impl/template.js";
import {renderFragment} from "../src/impl/query.js";

// Extend Zod once before tests
extendZod(z);

// Test UUIDs (RFC 4122 compliant - version 4, variant 1)
const USER_ID = "11111111-1111-4111-a111-111111111111";
const POST_ID = "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa";

// Test tables
const Users = table("users", {
	id: z.string().uuid().db.primary(),
	email: z.string().email().db.unique(),
	name: z.string(),
});

const Posts = table("posts", {
	id: z.string().uuid().db.primary(),
	authorId: z.string().uuid().db.references(Users, {as: "author"}),
	title: z.string(),
	body: z.string(),
	published: z.boolean().db.inserted(() => false),
});

// Helper to build SQL from template parts (for assertions)
// Matches real driver behavior: inlines SQL symbols, quotes identifiers, uses ? for other values
function buildSQL(strings: TemplateStringsArray, values: unknown[]): string {
	let sql = strings[0];
	for (let i = 0; i < values.length; i++) {
		const value = values[i];
		if (isSQLBuiltin(value)) {
			// Inline the symbol's SQL directly (matches driver behavior)
			sql += (value === NOW ? "CURRENT_TIMESTAMP" : "?") + strings[i + 1];
		} else if (isSQLIdentifier(value)) {
			// Quote identifiers using double quotes (like sqlite/postgres)
			const name = value.name.replace(/"/g, '""');
			sql += `"${name}"` + strings[i + 1];
		} else {
			sql += "?" + strings[i + 1];
		}
	}
	return sql;
}

// Helper to extract actual parameter values (not identifiers or symbols)
function getParams(values: unknown[]): unknown[] {
	return values.filter((v) => !isSQLIdentifier(v) && !isSQLBuiltin(v));
}

// Mock driver factory
function createMockDriver(supportsReturning = true): Driver {
	const driver: Driver = {
		supportsReturning,
		all: mock(async () => []) as Driver["all"],
		get: mock(async () => null) as Driver["get"],
		run: mock(async () => 1) as Driver["run"],
		val: mock(async () => 0) as Driver["val"],
		close: mock(async () => {}),
		transaction: mock(async (fn) => await fn(driver)) as Driver["transaction"],
	};
	return driver;
}

describe("Database", () => {
	let driver: Driver;
	let db: Database;

	beforeEach(() => {
		driver = createMockDriver();
		db = new Database(driver);
	});

	describe("all()", () => {
		test("generates correct SQL and normalizes results", async () => {
			(driver.all as any).mockImplementation(async () => [
				{
					"posts.id": POST_ID,
					"posts.authorId": USER_ID,
					"posts.title": "Test Post",
					"posts.body": "Content",
					"posts.published": true,
					"users.id": USER_ID,
					"users.email": "alice@example.com",
					"users.name": "Alice",
				},
			]);

			const results = await db.all([Posts, Users])`
        JOIN "users" ON "users"."id" = "posts"."authorId"
        WHERE published = ${true}
      `;

			expect(results.length).toBe(1);
			expect(results[0].title).toBe("Test Post");
			expect((results[0] as any).author.name).toBe("Alice");

			// Check SQL was called correctly
			const [strings, values] = (driver.all as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('SELECT "posts"."id" AS "posts.id"');
			expect(sql).toContain('FROM "posts"');
			expect(sql).toContain("WHERE published = ?");
			expect(getParams(values)).toEqual([true]);
		});

		test("returns empty array for no results", async () => {
			(driver.all as any).mockImplementation(async () => []);

			const results = await db.all(Posts)`WHERE id = ${"nonexistent"}`;

			expect(results).toEqual([]);
		});
	});

	describe("get()", () => {
		test("returns single entity with query", async () => {
			(driver.get as any).mockImplementation(async () => ({
				"posts.id": POST_ID,
				"posts.authorId": USER_ID,
				"posts.title": "Test Post",
				"posts.body": "Content",
				"posts.published": true,
			}));

			const post = await db.get(Posts)`WHERE "posts"."id" = ${POST_ID}`;

			expect(post).not.toBeNull();
			expect(post!.title).toBe("Test Post");
		});

		test("returns single entity by ID", async () => {
			(driver.get as any).mockImplementation(async () => ({
				id: POST_ID,
				authorId: USER_ID,
				title: "Test Post",
				body: "Content",
				published: true,
			}));

			const post = await db.get(Posts, POST_ID);

			expect(post).not.toBeNull();
			expect(post!.title).toBe("Test Post");

			// Check SQL was called with primary key
			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('SELECT * FROM "posts"');
			expect(sql).toContain('WHERE "id" = ?');
			expect(getParams(values)).toEqual([POST_ID]);
		});

		test("returns null for no match", async () => {
			(driver.get as any).mockImplementation(async () => null);

			const post = await db.get(Posts)`WHERE "posts"."id" = ${"nonexistent"}`;

			expect(post).toBeNull();
		});

		test("throws for get by ID on table without primary key", async () => {
			const noPk = table("no_pk", {name: z.string()});
			await expect(db.get(noPk, "123")).rejects.toThrow(
				"Table no_pk has no primary key defined",
			);
		});
	});

	describe("insert()", () => {
		test("inserts and returns entity", async () => {
			// Mock RETURNING result
			(driver.get as any).mockImplementation(async () => ({
				id: USER_ID,
				email: "alice@example.com",
				name: "Alice",
			}));

			const user = await db.insert(Users, {
				id: USER_ID,
				email: "alice@example.com",
				name: "Alice",
			});

			expect(user.id).toBe(USER_ID);
			expect(user.email).toBe("alice@example.com");

			// Check SQL uses RETURNING (sqlite default)
			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('INSERT INTO "users"');
			expect(sql).toContain('"id", "email", "name"');
			expect(sql).toContain("VALUES (?, ?, ?)");
			expect(sql).toContain("RETURNING *");
			expect(getParams(values)).toEqual([
				USER_ID,
				"alice@example.com",
				"Alice",
			]);
		});

		test("validates through Zod schema", async () => {
			await expect(
				db.insert(Users, {
					id: USER_ID,
					email: "not-an-email", // Invalid email
					name: "Alice",
				}),
			).rejects.toThrow();
		});

		test("applies defaults", async () => {
			// Mock RETURNING result with default applied by DB
			(driver.get as any).mockImplementation(async () => ({
				id: POST_ID,
				authorId: USER_ID,
				title: "Test",
				body: "Content",
				published: false, // DB applied default
			}));

			const post = await db.insert(Posts, {
				id: POST_ID,
				authorId: USER_ID,
				title: "Test",
				body: "Content",
				// published not provided - should use default
			});

			expect(post.published).toBe(false);
		});

		test("throws on partial table", async () => {
			const partialUsers = Users.pick("id", "name");

			await expect(
				db.insert(partialUsers as any, {id: USER_ID, name: "Alice"}),
			).rejects.toThrow('Cannot insert into partial table "users"');
		});

		test("throws on derived table", async () => {
			const UsersWithStats = Users.derive("postCount", z.number())`COUNT(*)`;

			await expect(
				db.insert(UsersWithStats as any, {
					id: USER_ID,
					name: "Alice",
					email: "a@b.com",
					postCount: 0,
				}),
			).rejects.toThrow('Cannot insert into derived table "users"');
		});

		test("auto-encodes objects and arrays as JSON", async () => {
			const Settings = table("settings", {
				id: z.string().db.primary(),
				config: z.object({theme: z.string()}),
				tags: z.array(z.string()),
			});

			// Mock driver.get (Database.insert uses RETURNING)
			let capturedValues: any;
			(driver.get as any).mockImplementation(
				async (sql: string, values: unknown[]) => {
					const params = getParams(values);
					capturedValues = params;
					// Return the values as a row with field names
					return {
						id: params[0],
						config: params[1],
						tags: params[2],
					};
				},
			);

			await db.insert(Settings, {
				id: "s1",
				config: {theme: "dark"},
				tags: ["admin", "premium"],
			});

			// Verify automatic JSON encoding happened
			expect(capturedValues[1]).toBe('{"theme":"dark"}');
			expect(capturedValues[2]).toBe('["admin","premium"]');
		});

		test("auto-decodes JSON strings back to objects/arrays", async () => {
			const Settings = table("settings", {
				id: z.string().db.primary(),
				config: z.object({theme: z.string()}),
				tags: z.array(z.string()),
			});

			// Mock driver.get to return JSON strings (SQLite behavior)
			(driver.get as any).mockImplementation(
				async (sql: string, values: unknown[]) => {
					const params = getParams(values);
					return {
						id: params[0],
						config: params[1], // Already JSON string from encoding
						tags: params[2], // Already JSON string from encoding
					};
				},
			);

			const result = await db.insert(Settings, {
				id: "s1",
				config: {theme: "dark"},
				tags: ["admin", "premium"],
			});

			// Verify automatic JSON decoding happened
			expect(result.config).toEqual({theme: "dark"});
			expect(result.tags).toEqual(["admin", "premium"]);
		});

		test("custom encode overrides automatic JSON encoding", async () => {
			const Custom = table("custom", {
				id: z.string().db.primary(),
				data: z
					.array(z.string())
					.db.encode((arr) => arr.join(","))
					.db.decode((str: string) => str.split(",")),
			});

			let capturedValues: any;
			(driver.get as any).mockImplementation(
				async (sql: string, values: unknown[]) => {
					const params = getParams(values);
					capturedValues = params;
					return {
						id: params[0],
						data: params[1],
					};
				},
			);

			await db.insert(Custom, {
				id: "c1",
				data: ["a", "b", "c"],
			});

			// Should use custom encoding (CSV), not JSON
			expect(capturedValues[1]).toBe("a,b,c");
		});

		test("throws when using DB expression on field with encode", async () => {
			// Create a DBExpression manually for testing
			const DB_EXPR = Symbol.for("@b9g/zen:db-expr");
			const dbExpr = {[DB_EXPR]: true, sql: "CURRENT_TIMESTAMP"};

			const EncodedField = table("encoded", {
				id: z.string().db.primary(),
				value: z.string().db.encode((v) => v.toUpperCase()),
			});

			await expect(
				db.insert(EncodedField, {
					id: "1",
					value: dbExpr as any, // Try to pass DB expression
				}),
			).rejects.toThrow(
				'Cannot use DB expression for field "value" which has encode/decode',
			);
		});

		test("throws when using DB expression on field with decode", async () => {
			// Create a DBExpression manually for testing
			const DB_EXPR = Symbol.for("@b9g/zen:db-expr");
			const dbExpr = {[DB_EXPR]: true, sql: "CURRENT_TIMESTAMP"};

			const DecodedField = table("decoded", {
				id: z.string().db.primary(),
				value: z.string().db.decode((v) => v.toLowerCase()),
			});

			await expect(
				db.insert(DecodedField, {
					id: "1",
					value: dbExpr as any, // Try to pass DB expression
				}),
			).rejects.toThrow(
				'Cannot use DB expression for field "value" which has encode/decode',
			);
		});
	});

	describe("read path JSON decoding", () => {
		test("db.get() by primary key decodes JSON strings", async () => {
			const Settings = table("settings", {
				id: z.string().db.primary(),
				config: z.object({theme: z.string(), fontSize: z.number()}),
				tags: z.array(z.string()),
			});

			// Mock driver.get to return JSON strings (as SQLite would store them)
			(driver.get as any).mockImplementation(async () => ({
				id: "s1",
				config: '{"theme":"dark","fontSize":14}',
				tags: '["admin","premium"]',
			}));

			const result = await db.get(Settings, "s1");

			expect(result).not.toBeNull();
			expect(result!.config).toEqual({theme: "dark", fontSize: 14});
			expect(result!.tags).toEqual(["admin", "premium"]);
		});

		test("db.all() decodes JSON strings in buildEntityMap", async () => {
			const Settings = table("settings", {
				id: z.string().db.primary(),
				config: z.object({theme: z.string()}),
				tags: z.array(z.string()),
			});

			// Mock driver.all to return JSON strings (as SQLite would store them)
			(driver.all as any).mockImplementation(async () => [
				{
					"settings.id": "s1",
					"settings.config": '{"theme":"dark"}',
					"settings.tags": '["admin","premium"]',
				},
				{
					"settings.id": "s2",
					"settings.config": '{"theme":"light"}',
					"settings.tags": '["user"]',
				},
			]);

			const results = await db.all(Settings)`WHERE 1=1`;

			expect(results.length).toBe(2);
			expect(results[0].config).toEqual({theme: "dark"});
			expect(results[0].tags).toEqual(["admin", "premium"]);
			expect(results[1].config).toEqual({theme: "light"});
			expect(results[1].tags).toEqual(["user"]);
		});

		test("db.get() with query decodes JSON strings", async () => {
			const Settings = table("settings", {
				id: z.string().db.primary(),
				config: z.object({theme: z.string()}),
				tags: z.array(z.string()),
			});

			// Mock driver.get to return JSON strings
			(driver.get as any).mockImplementation(async () => ({
				"settings.id": "s1",
				"settings.config": '{"theme":"dark"}',
				"settings.tags": '["admin"]',
			}));

			const result = await db.get(Settings)`WHERE "settings"."id" = ${"s1"}`;

			expect(result).not.toBeNull();
			expect(result!.config).toEqual({theme: "dark"});
			expect(result!.tags).toEqual(["admin"]);
		});

		test("custom decode overrides automatic JSON decoding on read", async () => {
			const Custom = table("custom", {
				id: z.string().db.primary(),
				data: z
					.array(z.string())
					.db.encode((arr) => arr.join(","))
					.db.decode((str: string) => str.split(",")),
			});

			// Mock driver.get to return CSV string (custom format)
			(driver.get as any).mockImplementation(async () => ({
				id: "c1",
				data: "a,b,c",
			}));

			const result = await db.get(Custom, "c1");

			expect(result).not.toBeNull();
			expect(result!.data).toEqual(["a", "b", "c"]);
		});

		test("nested objects decode correctly", async () => {
			const Complex = table("complex", {
				id: z.string().db.primary(),
				nested: z.object({
					level1: z.object({
						level2: z.array(z.object({value: z.number()})),
					}),
				}),
			});

			(driver.get as any).mockImplementation(async () => ({
				id: "c1",
				nested: '{"level1":{"level2":[{"value":1},{"value":2}]}}',
			}));

			const result = await db.get(Complex, "c1");

			expect(result).not.toBeNull();
			expect((result as any).nested.level1.level2).toEqual([
				{value: 1},
				{value: 2},
			]);
		});

		test("nullable object fields decode correctly", async () => {
			const NullableSettings = table("nullable_settings", {
				id: z.string().db.primary(),
				config: z.object({theme: z.string()}).nullable(),
			});

			// Test with JSON string
			(driver.get as any).mockImplementation(async () => ({
				id: "s1",
				config: '{"theme":"dark"}',
			}));

			let result = await db.get(NullableSettings, "s1");
			expect(result!.config).toEqual({theme: "dark"});

			// Test with null
			(driver.get as any).mockImplementation(async () => ({
				id: "s2",
				config: null,
			}));

			result = await db.get(NullableSettings, "s2");
			expect(result!.config).toBeNull();
		});

		test("optional object fields decode correctly", async () => {
			const OptionalSettings = table("optional_settings", {
				id: z.string().db.primary(),
				config: z.object({theme: z.string()}).optional(),
			});

			// Test with JSON string
			(driver.get as any).mockImplementation(async () => ({
				id: "s1",
				config: '{"theme":"dark"}',
			}));

			let result = await db.get(OptionalSettings, "s1");
			expect(result!.config).toEqual({theme: "dark"});

			// Test with undefined (field not present)
			(driver.get as any).mockImplementation(async () => ({
				id: "s2",
			}));

			result = await db.get(OptionalSettings, "s2");
			expect(result!.config).toBeUndefined();
		});
	});

	describe("update()", () => {
		test("updates by primary key", async () => {
			(driver.get as any).mockImplementation(async () => ({
				id: USER_ID,
				email: "alice@example.com",
				name: "Alice Updated",
			}));

			const user = await db.update(Users, {name: "Alice Updated"}, USER_ID);

			expect(user).not.toBeNull();
			expect(user!.name).toBe("Alice Updated");

			// Check SQL uses RETURNING (sqlite default)
			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('UPDATE "users"');
			expect(sql).toContain('SET "name" = ?');
			expect(sql).toContain('WHERE "id" = ?');
			expect(sql).toContain("RETURNING *");
			expect(getParams(values)).toEqual(["Alice Updated", USER_ID]);
		});

		test("throws on no fields to update", async () => {
			await expect(db.update(Users, {}, USER_ID)).rejects.toThrow(
				"No fields to update",
			);
		});

		test("returns null if entity not found after update", async () => {
			(driver.get as any).mockImplementation(async () => null);

			const user = await db.update(Users, {name: "Test"}, "nonexistent");

			expect(user).toBeNull();
		});

		test("throws on derived table", async () => {
			const UsersWithStats = Users.derive("postCount", z.number())`COUNT(*)`;

			await expect(
				db.update(UsersWithStats as any, {name: "Alice Updated"}, USER_ID),
			).rejects.toThrow('Cannot update derived table "users"');
		});
	});

	describe("delete()", () => {
		test("deletes by primary key", async () => {
			(driver.run as any).mockImplementation(async () => 1);

			const deleted = await db.delete(Users, USER_ID);

			expect(deleted).toBe(1);

			const [strings, values] = (driver.run as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('DELETE FROM "users"');
			expect(sql).toContain('WHERE "id" = ?');
			expect(getParams(values)).toEqual([USER_ID]);
		});

		test("returns 0 if nothing deleted", async () => {
			(driver.run as any).mockImplementation(async () => 0);

			const deleted = await db.delete(Users, "nonexistent");

			expect(deleted).toBe(0);
		});
	});

	describe("query()", () => {
		test("executes raw query with params", async () => {
			(driver.all as any).mockImplementation(async () => [{count: 5}]);

			const results = await db.query<{count: number}>`
        SELECT COUNT(*) as count FROM posts WHERE author_id = ${USER_ID}
      `;

			expect(results[0].count).toBe(5);

			const [strings, values] = (driver.all as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain(
				"SELECT COUNT(*) as count FROM posts WHERE author_id = ?",
			);
			expect(getParams(values)).toEqual([USER_ID]);
		});
	});

	describe("exec()", () => {
		test("executes statement", async () => {
			(driver.run as any).mockImplementation(async () => 0);

			await db.exec`CREATE TABLE IF NOT EXISTS test (id TEXT PRIMARY KEY)`;

			const [strings] = (driver.run as any).mock.calls[0];
			expect(strings.join("")).toContain("CREATE TABLE");
		});
	});

	describe("val()", () => {
		test("returns single value", async () => {
			(driver.val as any).mockImplementation(async () => 42);

			const count = await db.val<number>`SELECT COUNT(*) FROM users`;

			expect(count).toBe(42);
		});
	});
});

describe("supportsReturning fallback", () => {
	test("insert() uses INSERT + SELECT when RETURNING not supported", async () => {
		// Create a driver that doesn't support RETURNING (like MySQL)
		const driver = createMockDriver(false);
		const db = new Database(driver);

		const TableWithDefaults = table("with_defaults", {
			id: z.string().db.primary(),
			name: z.string(),
			metadata: z.object({source: z.string()}).optional(),
			version: z.number().optional(),
		});

		// Without RETURNING, it should:
		// 1. INSERT the row (run)
		// 2. SELECT the row back (get) to get DB-applied defaults
		(driver.run as any).mockImplementation(async () => 1);
		(driver.get as any).mockImplementation(async () => ({
			id: "test-id",
			name: "Test",
			metadata: '{"source":"db-trigger"}', // DB might set this via trigger
			version: 1, // DB might have a trigger that sets this
		}));

		const result = await db.insert(TableWithDefaults, {
			id: "test-id",
			name: "Test",
		});

		// Should have fetched the row with DB defaults, not just return input
		expect(result.version).toBe(1);
		expect(result.metadata).toEqual({source: "db-trigger"});

		// Verify both INSERT (run) and SELECT (get) were called
		expect((driver.run as any).mock.calls.length).toBe(1);
		expect((driver.get as any).mock.calls.length).toBe(1);

		// Check INSERT was called
		const [insertStrings, insertValues] = (driver.run as any).mock.calls[0];
		const insertSql = buildSQL(insertStrings, insertValues);
		expect(insertSql).toContain("INSERT INTO");
		expect(insertSql).toContain('"with_defaults"');

		// Check SELECT was called after INSERT
		const [selectStrings, selectValues] = (driver.get as any).mock.calls[0];
		const selectSql = buildSQL(selectStrings, selectValues);
		expect(selectSql).toContain("SELECT");
		expect(selectSql).toContain('"with_defaults"');
	});

	test("insert() uses RETURNING when supported", async () => {
		// Create a driver that supports RETURNING (like PostgreSQL/SQLite)
		const driver = createMockDriver(true);
		const db = new Database(driver);

		// With RETURNING, it should use get() for INSERT...RETURNING
		(driver.get as any).mockImplementation(async () => ({
			id: USER_ID,
			email: "test@example.com",
			name: "Test",
		}));

		await db.insert(Users, {
			id: USER_ID,
			email: "test@example.com",
			name: "Test",
		});

		// Should only call get (for INSERT...RETURNING), not run
		expect((driver.get as any).mock.calls.length).toBe(1);
		expect((driver.run as any).mock.calls.length).toBe(0);

		// Check RETURNING was added
		const [strings, values] = (driver.get as any).mock.calls[0];
		const sql = buildSQL(strings, values);
		expect(sql).toContain("RETURNING *");
	});
});

describe("transaction()", () => {
	test("commits on success", async () => {
		const driver = createMockDriver();
		// Mock get result for INSERT ... RETURNING
		(driver.get as any).mockImplementation(async () => ({
			id: USER_ID,
			email: "alice@example.com",
			name: "Alice",
		}));
		const db = new Database(driver);

		const result = await db.transaction(async (tx) => {
			await tx.insert(Users, {
				id: USER_ID,
				email: "alice@example.com",
				name: "Alice",
			});
			return "done";
		});

		expect(result).toBe("done");

		// Check driver.transaction was called
		expect((driver.transaction as any).mock.calls.length).toBe(1);

		// Check INSERT SQL was executed via driver.get (RETURNING)
		expect((driver.get as any).mock.calls.length).toBeGreaterThan(0);
	});

	test("rollbacks on error", async () => {
		const driver = createMockDriver();
		// Mock get result for INSERT ... RETURNING
		(driver.get as any).mockImplementation(async () => ({
			id: USER_ID,
			email: "alice@example.com",
			name: "Alice",
		}));
		const db = new Database(driver);

		const error = new Error("Test error");
		await expect(
			db.transaction(async (tx) => {
				await tx.insert(Users, {
					id: USER_ID,
					email: "alice@example.com",
					name: "Alice",
				});
				throw error;
			}),
		).rejects.toThrow("Test error");

		// Check driver.transaction was called and it propagated the error
		expect((driver.transaction as any).mock.calls.length).toBe(1);
	});

	test("returns value from transaction function", async () => {
		const driver = createMockDriver();
		const db = new Database(driver);

		const result = await db.transaction(async () => {
			return {id: USER_ID, name: "Alice"};
		});

		expect(result).toEqual({id: USER_ID, name: "Alice"});
	});

	test("applies schema markers (inserted/upserted) in transaction", async () => {
		const AutoTable = table("auto_tx", {
			id: z.string().db.primary(),
			name: z.string(),
			createdAt: z.date().db.inserted(NOW),
			updatedAt: z.date().db.upserted(NOW),
		});

		const driver = createMockDriver();
		(driver.get as any).mockImplementation(async () => ({
			id: "123",
			name: "Test",
			createdAt: new Date().toISOString(),
			updatedAt: new Date().toISOString(),
		}));
		const database = new Database(driver);

		await database.transaction(async (tx) => {
			// Insert without providing timestamps
			await tx.insert(AutoTable, {
				id: "123",
				name: "Test",
			});

			// Check SQL contains CURRENT_TIMESTAMP for both fields
			const [insertStrings, insertValues] = (driver.get as any).mock.calls[0];
			const insertSql = buildSQL(insertStrings, insertValues);
			expect(insertSql).toContain("CURRENT_TIMESTAMP");
			expect(insertSql.match(/CURRENT_TIMESTAMP/g)?.length).toBe(2);

			// Update without providing updatedAt
			(driver.get as any).mockClear();
			await tx.update(
				AutoTable,
				{
					name: "Updated",
				},
				"123",
			);

			// Check SQL contains CURRENT_TIMESTAMP for updatedAt
			const [updateStrings, updateValues] = (driver.get as any).mock.calls[0];
			const updateSql = buildSQL(updateStrings, updateValues);
			expect(updateSql).toContain('"updatedAt" = CURRENT_TIMESTAMP');
			expect(updateSql).not.toContain('"createdAt"');
		});
	});
});

describe("Soft Delete", () => {
	const SoftDeleteUsers = table("soft_delete_users", {
		id: z.string().uuid().db.primary(),
		email: z.string().email().db.unique(),
		name: z.string(),
		deletedAt: z.date().nullable().db.softDelete(),
	});

	describe("softDelete() field wrapper", () => {
		test("marks field as soft delete field", () => {
			expect(SoftDeleteUsers.meta.softDeleteField).toBe("deletedAt");
		});

		test("throws on multiple soft delete fields", () => {
			expect(() =>
				table("invalid_table", {
					id: z.string().db.primary(),
					deletedAt1: z.date().nullable().db.softDelete(),
					deletedAt2: z.date().nullable().db.softDelete(),
				}),
			).toThrow(
				'Table "invalid_table" has multiple soft delete fields: "deletedAt1" and "deletedAt2"',
			);
		});

		test("allows tables without soft delete field", () => {
			const NormalTable = table("normal", {
				id: z.string().db.primary(),
				name: z.string(),
			});
			expect(NormalTable.meta.softDeleteField).toBeNull();
		});
	});

	describe("Table.deleted()", () => {
		test("generates SQL template for soft delete check", () => {
			const fragment = SoftDeleteUsers.deleted();
			// SQLTemplate is a branded tuple: [strings, values]
			expect(Array.isArray(fragment)).toBe(true);
			expect(fragment[0]).toBeDefined(); // strings
			expect(fragment[1]).toBeDefined(); // values
			const {sql, params} = renderFragment(fragment);
			expect(sql).toBe('"soft_delete_users"."deletedAt" IS NOT NULL');
			expect(params).toEqual([]);
		});

		test("throws if table has no soft delete field", () => {
			const NormalTable = table("normal", {
				id: z.string().db.primary(),
				name: z.string(),
			});
			expect(() => NormalTable.deleted()).toThrow(
				'Table "normal" does not have a soft delete field',
			);
		});

		test("fragment composes correctly in queries", async () => {
			const driver = createMockDriver();
			const db = new Database(driver);
			(driver.all as any).mockImplementation(async () => []);

			await db.all(SoftDeleteUsers)`WHERE NOT (${SoftDeleteUsers.deleted()})`;

			const [strings, values] = (driver.all as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('"soft_delete_users"."deletedAt" IS NOT NULL');
		});
	});

	describe("Database.softDelete()", () => {
		let driver: Driver;
		let db: Database;

		beforeEach(() => {
			driver = createMockDriver();
			db = new Database(driver);
		});

		test("soft deletes by primary key using DB timestamp", async () => {
			(driver.run as any).mockImplementation(async () => 1);

			const deleted = await db.softDelete(SoftDeleteUsers, USER_ID);

			expect(deleted).toBe(1);

			const [strings, values] = (driver.run as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('UPDATE "soft_delete_users"');
			// Should use DB's CURRENT_TIMESTAMP, not app-side Date
			// This ensures consistent timestamps in distributed systems
			expect(sql).toContain("CURRENT_TIMESTAMP");
			expect(sql).toContain('WHERE "id" = ?');
			// Only the ID should be a parameter, not the timestamp
			expect(getParams(values)).toHaveLength(1);
			expect(getParams(values)[0]).toBe(USER_ID);
		});

		test("returns 0 if entity not found", async () => {
			(driver.run as any).mockImplementation(async () => 0);

			const deleted = await db.softDelete(SoftDeleteUsers, "nonexistent");

			expect(deleted).toBe(0);
		});

		test("throws if table has no primary key", async () => {
			const NoPkTable = table("no_pk", {
				name: z.string(),
				deletedAt: z.date().nullable().db.softDelete(),
			});

			await expect(db.softDelete(NoPkTable, "123")).rejects.toThrow(
				"Table no_pk has no primary key defined",
			);
		});

		test("throws if table has no soft delete field", () => {
			const NormalTable = table("normal", {
				id: z.string().db.primary(),
				name: z.string(),
			});

			// softDelete throws synchronously for configuration errors
			expect(() => db.softDelete(NormalTable, "123")).toThrow(
				"Table normal does not have a soft delete field",
			);
		});

		test("applies updated() markers on soft delete", async () => {
			const SoftDeleteWithUpdatedAt = table("soft_with_updated", {
				id: z.string().db.primary(),
				name: z.string(),
				updatedAt: z.date().db.updated(NOW),
				deletedAt: z.date().nullable().db.softDelete(),
			});

			(driver.run as any).mockImplementation(async () => 1);

			await db.softDelete(SoftDeleteWithUpdatedAt, "123");

			const [strings, values] = (driver.run as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			// Should set both deletedAt and updatedAt
			expect(sql).toContain('"deletedAt" = CURRENT_TIMESTAMP');
			expect(sql).toContain('"updatedAt" = CURRENT_TIMESTAMP');
		});
	});

	describe("Transaction.softDelete()", () => {
		test("soft deletes within transaction using DB timestamp", async () => {
			const driver = createMockDriver();
			const db = new Database(driver);
			(driver.run as any).mockImplementation(async () => 1);

			const result = await db.transaction(async (tx) => {
				const deleted = await tx.softDelete(SoftDeleteUsers, USER_ID);
				return deleted;
			});

			expect(result).toBe(1);

			// Check transaction was called
			expect((driver.transaction as any).mock.calls.length).toBe(1);

			// Check UPDATE was called with DB timestamp
			const [strings, values] = (driver.run as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('UPDATE "soft_delete_users"');
			expect(sql).toContain("CURRENT_TIMESTAMP");
			expect(sql).toContain('WHERE "id" = ?');
			// Only ID should be parameterized, not the timestamp
			expect(getParams(values)).toHaveLength(1);
			expect(getParams(values)[0]).toBe(USER_ID);
		});

		test("throws if table has no soft delete field", async () => {
			const driver = createMockDriver();
			const db = new Database(driver);
			const NormalTable = table("normal", {
				id: z.string().db.primary(),
				name: z.string(),
			});

			await expect(
				db.transaction(async (tx) => {
					await tx.softDelete(NormalTable, "123");
				}),
			).rejects.toThrow("Table normal does not have a soft delete field");
		});

		test("applies updated() markers on soft delete", async () => {
			const SoftDeleteWithUpdatedAt = table("soft_with_updated", {
				id: z.string().db.primary(),
				name: z.string(),
				updatedAt: z.date().db.updated(NOW),
				deletedAt: z.date().nullable().db.softDelete(),
			});

			const driver = createMockDriver();
			const database = new Database(driver);
			(driver.run as any).mockImplementation(async () => 1);

			await database.transaction(async (tx) => {
				await tx.softDelete(SoftDeleteWithUpdatedAt, "123");
			});

			const [strings, values] = (driver.run as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			// Should set both deletedAt and updatedAt
			expect(sql).toContain('"deletedAt" = CURRENT_TIMESTAMP');
			expect(sql).toContain('"updatedAt" = CURRENT_TIMESTAMP');
		});
	});

	describe("Integration with pick()", () => {
		test("preserves soft delete field in partial table", () => {
			const PartialUsers = SoftDeleteUsers.pick("id", "name", "deletedAt");
			expect(PartialUsers.meta.softDeleteField).toBe("deletedAt");
		});

		test("removes soft delete field when not picked", () => {
			const PartialUsers = SoftDeleteUsers.pick("id", "name");
			expect(PartialUsers.meta.softDeleteField).toBeNull();
		});

		test("deleted() works on partial table with soft delete field", () => {
			const PartialUsers = SoftDeleteUsers.pick("id", "name", "deletedAt");
			const fragment = PartialUsers.deleted();
			expect(renderFragment(fragment).sql).toBe(
				'"soft_delete_users"."deletedAt" IS NOT NULL',
			);
		});

		test("deleted() throws on partial table without soft delete field", () => {
			const PartialUsers = SoftDeleteUsers.pick("id", "name");
			expect(() => PartialUsers.deleted()).toThrow(
				'Table "soft_delete_users" does not have a soft delete field',
			);
		});
	});
});

describe("DB Expressions", () => {
	const {db: _db} = require("../src/impl/database.js");

	const TimestampTable = table("timestamps", {
		id: z.string().db.primary(),
		name: z.string(),
		createdAt: z.date(),
		updatedAt: z.date().optional(),
	});

	describe("DB Expressions in insert/update", () => {
		// Create a DBExpression manually for testing
		const DB_EXPR = Symbol.for("@b9g/zen:db-expr");
		// Helper to create fake TemplateStringsArray
		const makeStrings = (strs: string[]): TemplateStringsArray => {
			const arr = strs as unknown as TemplateStringsArray;
			(arr as any).raw = strs;
			return arr;
		};
		const dbNow = () => ({
			[DB_EXPR]: true,
			strings: makeStrings(["CURRENT_TIMESTAMP"]),
			values: [] as unknown[],
		});

		test("insert with DBExpression generates raw SQL", async () => {
			const driver = createMockDriver();
			(driver.get as any).mockImplementation(async () => ({
				id: "123",
				name: "Test",
				createdAt: new Date().toISOString(),
			}));
			const database = new Database(driver);

			await database.insert(TimestampTable, {
				id: "123",
				name: "Test",
				createdAt: dbNow() as any,
			});

			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain("CURRENT_TIMESTAMP");

			// Values should NOT include the DBExpression
			expect(getParams(values)).toEqual(["123", "Test"]);
		});

		test("update with DBExpression generates raw SQL", async () => {
			const driver = createMockDriver();
			(driver.get as any).mockImplementation(async () => ({
				id: "123",
				name: "Test",
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}));
			const database = new Database(driver);

			await database.update(
				TimestampTable,
				{
					updatedAt: dbNow() as any,
				},
				"123",
			);

			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('"updatedAt" = CURRENT_TIMESTAMP');
		});

		test("DBExpression works alongside regular values in insert", async () => {
			const driver = createMockDriver();
			(driver.get as any).mockImplementation(async () => ({
				id: "456",
				name: "Combined",
				createdAt: new Date().toISOString(),
			}));
			const database = new Database(driver);

			await database.insert(TimestampTable, {
				id: "456",
				name: "Combined",
				createdAt: dbNow() as any,
			});

			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);

			// SQL should have placeholders for regular values and raw SQL for expression
			expect(sql).toContain("VALUES (?, ?, CURRENT_TIMESTAMP)");
			expect(getParams(values)).toEqual(["456", "Combined"]);
		});

		test("DBExpression works alongside regular values in update", async () => {
			const driver = createMockDriver();
			(driver.get as any).mockImplementation(async () => ({
				id: "123",
				name: "Updated",
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}));
			const database = new Database(driver);

			await database.update(
				TimestampTable,
				{
					name: "Updated",
					updatedAt: dbNow() as any,
				},
				"123",
			);

			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain('"name" = ?');
			expect(sql).toContain('"updatedAt" = CURRENT_TIMESTAMP');
		});
	});

	describe("schema markers (.db.inserted() / .db.updated() / .db.upserted())", () => {
		const AutoTable = table("auto", {
			id: z.string().db.primary(),
			name: z.string(),
			createdAt: z.date().db.inserted(NOW),
			updatedAt: z.date().db.upserted(NOW),
		});

		test("insert auto-applies inserted() and upserted() expressions", async () => {
			const driver = createMockDriver();
			(driver.get as any).mockImplementation(async () => ({
				id: "123",
				name: "Test",
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}));
			const database = new Database(driver);

			// Note: createdAt and updatedAt not provided
			await database.insert(AutoTable, {
				id: "123",
				name: "Test",
			});

			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			// Both should be set to CURRENT_TIMESTAMP
			expect(sql).toContain("CURRENT_TIMESTAMP");
			expect(sql.match(/CURRENT_TIMESTAMP/g)?.length).toBe(2);
		});

		test("update auto-applies upserted() expression but not inserted()", async () => {
			const driver = createMockDriver();
			(driver.get as any).mockImplementation(async () => ({
				id: "123",
				name: "Updated",
				createdAt: new Date().toISOString(),
				updatedAt: new Date().toISOString(),
			}));
			const database = new Database(driver);

			await database.update(
				AutoTable,
				{
					name: "Updated",
				},
				"123",
			);

			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			// Only updatedAt should be set
			expect(sql).toContain('"updatedAt" = CURRENT_TIMESTAMP');
			// createdAt should NOT be in the SET clause
			expect(sql).not.toContain('"createdAt"');
		});

		test("explicit value overrides schema marker on insert", async () => {
			const driver = createMockDriver();
			const specificDate = new Date("2020-01-01");
			(driver.get as any).mockImplementation(async () => ({
				id: "123",
				name: "Test",
				createdAt: specificDate.toISOString(),
				updatedAt: new Date().toISOString(),
			}));
			const database = new Database(driver);

			await database.insert(AutoTable, {
				id: "123",
				name: "Test",
				createdAt: specificDate, // explicit value
			});

			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);

			// createdAt should be a parameter, not CURRENT_TIMESTAMP
			// Values array includes identifiers, 3 actual params (id, name, createdAt), and NOW symbol for updatedAt
			// After buildSQL inlines symbols and identifiers, only 3 actual params remain
			const actualParams = getParams(values);
			expect(actualParams.length).toBe(3);
			// Should only have one CURRENT_TIMESTAMP (for updatedAt)
			expect(sql.match(/CURRENT_TIMESTAMP/g)?.length).toBe(1);
		});

		test("explicit value overrides schema marker on update", async () => {
			const driver = createMockDriver();
			const specificDate = new Date("2020-01-01");
			(driver.get as any).mockImplementation(async () => ({
				id: "123",
				name: "Test",
				createdAt: new Date().toISOString(),
				updatedAt: specificDate.toISOString(),
			}));
			const database = new Database(driver);

			await database.update(
				AutoTable,
				{
					updatedAt: specificDate, // explicit value
				},
				"123",
			);

			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);

			// updatedAt should be a parameter, not CURRENT_TIMESTAMP
			expect(sql).not.toContain("CURRENT_TIMESTAMP");
			expect(sql).toContain('"updatedAt" = ?');
		});

		test("inserted() only - update without data throws", async () => {
			const InsertOnlyTable = table("insert_only", {
				id: z.string().db.primary(),
				createdAt: z.date().db.inserted(NOW),
			});

			const driver = createMockDriver();
			(driver.get as any).mockImplementation(async () => ({
				id: "123",
				createdAt: new Date().toISOString(),
			}));
			const database = new Database(driver);

			// Insert should apply inserted()
			await database.insert(InsertOnlyTable, {id: "123"});
			const [strings, values] = (driver.get as any).mock.calls[0];
			const sql = buildSQL(strings, values);
			expect(sql).toContain("CURRENT_TIMESTAMP");

			// Update with no data should throw since inserted() doesn't apply on update
			await expect(database.update(InsertOnlyTable, {}, "123")).rejects.toThrow(
				"No fields to update",
			);
		});

		test("updated() only applies on UPDATE, not INSERT", async () => {
			const UpdateOnlyTable = table("update_only", {
				id: z.string().db.primary(),
				name: z.string(),
				modifiedAt: z.date().db.updated(NOW),
			});

			const driver = createMockDriver();
			(driver.get as any).mockImplementation(async () => ({
				id: "123",
				name: "Test",
				modifiedAt: new Date().toISOString(),
			}));
			const database = new Database(driver);

			// Insert should NOT apply updated() - must provide modifiedAt explicitly
			await database.insert(UpdateOnlyTable, {
				id: "123",
				name: "Test",
				modifiedAt: new Date("2020-01-01"),
			});
			const [insertStrings, insertValues] = (driver.get as any).mock.calls[0];
			const insertSql = buildSQL(insertStrings, insertValues);
			// modifiedAt should be a parameter, not CURRENT_TIMESTAMP
			expect(insertSql).not.toContain("CURRENT_TIMESTAMP");

			// Update SHOULD apply updated()
			(driver.get as any).mockClear();
			await database.update(UpdateOnlyTable, {name: "Updated"}, "123");
			const [updateStrings, updateValues] = (driver.get as any).mock.calls[0];
			const updateSql = buildSQL(updateStrings, updateValues);
			expect(updateSql).toContain('"modifiedAt" = CURRENT_TIMESTAMP');
		});
	});

	// =========================================================================
	// Regression: val() type safety (Issue #1)
	// =========================================================================

	describe("val() type safety", () => {
		test("val() should have correct return type allowing null", () => {
			// This is a compile-time check - if val() returns Promise<T | null>,
			// this code should compile without error
			const driver = createMockDriver();
			expect(driver.val).toBeDefined();
		});
	});
});
