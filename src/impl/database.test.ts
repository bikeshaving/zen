import {test, expect, describe, beforeEach, mock} from "bun:test";
import {z} from "zod";
import {table, primary, unique, references, softDelete} from "./table.js";
import {Database, type Driver} from "./database.js";

// Test UUIDs (RFC 4122 compliant - version 4, variant 1)
const USER_ID = "11111111-1111-4111-a111-111111111111";
const POST_ID = "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa";

// Test tables
const Users = table("users", {
	id: primary(z.string().uuid()),
	email: unique(z.string().email()),
	name: z.string(),
});

const Posts = table("posts", {
	id: primary(z.string().uuid()),
	authorId: references(z.string().uuid(), Users, {as: "author"}),
	title: z.string(),
	body: z.string(),
	published: z.boolean().default(false),
});

// Mock driver factory (default: SQLite-style escaping)
function createMockDriver(
	dialect: "sqlite" | "mysql" | "postgresql" = "sqlite",
): Driver {
	return {
		dialect: dialect,
		all: mock(async () => []) as Driver["all"],
		get: mock(async () => null) as Driver["get"],
		run: mock(async () => 1) as Driver["run"],
		val: mock(async () => 0) as Driver["val"],
		escapeIdentifier: (name: string) => {
			if (dialect === "mysql") {
				return `\`${name.replace(/`/g, "``")}\``;
			}
			return `"${name.replace(/"/g, '""')}"`;
		},
		close: mock(async () => {}),
		transaction: mock(async (fn) => await fn()) as Driver["transaction"],
		insert: mock(async (_tableName, data) => data) as Driver["insert"],
		update: mock(
			async (_tableName, _pk, _id, data) => data,
		) as Driver["update"],
	};
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
			const [sql, params] = (driver.all as any).mock.calls[0];
			expect(sql).toContain('SELECT "posts"."id" AS "posts.id"');
			expect(sql).toContain('FROM "posts"');
			expect(sql).toContain("WHERE published = ?");
			expect(params).toEqual([true]);
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
			const [sql, params] = (driver.get as any).mock.calls[0];
			expect(sql).toContain('SELECT * FROM "posts"');
			expect(sql).toContain('WHERE "id" = ?');
			expect(params).toEqual([POST_ID]);
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
			const [sql, params] = (driver.get as any).mock.calls[0];
			expect(sql).toContain('INSERT INTO "users"');
			expect(sql).toContain('"id", "email", "name"');
			expect(sql).toContain("VALUES (?, ?, ?)");
			expect(sql).toContain("RETURNING *");
			expect(params).toEqual([USER_ID, "alice@example.com", "Alice"]);
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
	});

	describe("update()", () => {
		test("updates by primary key", async () => {
			(driver.get as any).mockImplementation(async () => ({
				id: USER_ID,
				email: "alice@example.com",
				name: "Alice Updated",
			}));

			const user = await db.update(Users, USER_ID, {name: "Alice Updated"});

			expect(user).not.toBeNull();
			expect(user!.name).toBe("Alice Updated");

			// Check SQL uses RETURNING (sqlite default)
			const [sql, params] = (driver.get as any).mock.calls[0];
			expect(sql).toContain('UPDATE "users"');
			expect(sql).toContain('SET "name" = ?');
			expect(sql).toContain('WHERE "id" = ?');
			expect(sql).toContain("RETURNING *");
			expect(params).toEqual(["Alice Updated", USER_ID]);
		});

		test("throws on no fields to update", async () => {
			await expect(db.update(Users, USER_ID, {})).rejects.toThrow(
				"No fields to update",
			);
		});

		test("returns null if entity not found after update", async () => {
			(driver.get as any).mockImplementation(async () => null);

			const user = await db.update(Users, "nonexistent", {name: "Test"});

			expect(user).toBeNull();
		});
	});

	describe("delete()", () => {
		test("deletes by primary key", async () => {
			(driver.run as any).mockImplementation(async () => 1);

			const deleted = await db.delete(Users, USER_ID);

			expect(deleted).toBe(true);

			const [sql, params] = (driver.run as any).mock.calls[0];
			expect(sql).toContain('DELETE FROM "users"');
			expect(sql).toContain('WHERE "id" = ?');
			expect(params).toEqual([USER_ID]);
		});

		test("returns false if nothing deleted", async () => {
			(driver.run as any).mockImplementation(async () => 0);

			const deleted = await db.delete(Users, "nonexistent");

			expect(deleted).toBe(false);
		});
	});

	describe("query()", () => {
		test("executes raw query with params", async () => {
			(driver.all as any).mockImplementation(async () => [{count: 5}]);

			const results = await db.query<{count: number}>`
        SELECT COUNT(*) as count FROM posts WHERE author_id = ${USER_ID}
      `;

			expect(results[0].count).toBe(5);

			const [sql, params] = (driver.all as any).mock.calls[0];
			expect(sql).toBe(
				"SELECT COUNT(*) as count FROM posts WHERE author_id = ?",
			);
			expect(params).toEqual([USER_ID]);
		});
	});

	describe("exec()", () => {
		test("executes statement", async () => {
			(driver.run as any).mockImplementation(async () => 0);

			await db.exec`CREATE TABLE IF NOT EXISTS test (id TEXT PRIMARY KEY)`;

			const [sql] = (driver.run as any).mock.calls[0];
			expect(sql).toContain("CREATE TABLE");
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

describe("PostgreSQL dialect", () => {
	test("uses numbered placeholders", async () => {
		const driver = createMockDriver("postgresql");
		const db = new Database(driver);

		await db.query`SELECT * FROM users WHERE id = ${USER_ID} AND active = ${true}`;

		const [sql] = (driver.all as any).mock.calls[0];
		expect(sql).toContain("$1");
		expect(sql).toContain("$2");
		expect(sql).not.toContain("?");
	});
});

describe("MySQL dialect", () => {
	test("uses backtick quoting", async () => {
		const driver = createMockDriver("mysql");
		const db = new Database(driver);

		await db.insert(Users, {
			id: USER_ID,
			email: "test@example.com",
			name: "Test",
		});

		const [sql] = (driver.run as any).mock.calls[0];
		expect(sql).toContain("INSERT INTO `users`");
		expect(sql).toContain("`id`, `email`, `name`");
	});
});

describe("escapeIdentifier", () => {
	test("SQLite/PostgreSQL escapes double quotes", () => {
		const driver = createMockDriver("sqlite");
		expect(driver.escapeIdentifier("users")).toBe('"users"');
		expect(driver.escapeIdentifier('table"name')).toBe('"table""name"');
		expect(driver.escapeIdentifier('foo"bar"baz')).toBe('"foo""bar""baz"');
	});

	test("MySQL escapes backticks", () => {
		const driver = createMockDriver("mysql");
		expect(driver.escapeIdentifier("users")).toBe("`users`");
		expect(driver.escapeIdentifier("table`name")).toBe("`table``name`");
		expect(driver.escapeIdentifier("foo`bar`baz")).toBe("`foo``bar``baz`");
	});
});

describe("transaction()", () => {
	test("commits on success", async () => {
		const driver = createMockDriver();
		// Mock insert result
		(driver.insert as any).mockImplementation(
			async (_tableName: string, data: any) => data,
		);
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

		// Check INSERT was called via driver.insert
		expect((driver.insert as any).mock.calls.length).toBe(1);
	});

	test("rollbacks on error", async () => {
		const driver = createMockDriver();
		// Mock insert result
		(driver.insert as any).mockImplementation(
			async (_tableName: string, data: any) => data,
		);
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
});

describe("Soft Delete", () => {
	const SoftDeleteUsers = table("soft_delete_users", {
		id: primary(z.string().uuid()),
		email: unique(z.string().email()),
		name: z.string(),
		deletedAt: softDelete(z.date().nullable()),
	});

	describe("softDelete() field wrapper", () => {
		test("marks field as soft delete field", () => {
			expect(SoftDeleteUsers._meta.softDeleteField).toBe("deletedAt");
		});

		test("throws on multiple soft delete fields", () => {
			expect(() =>
				table("invalid_table", {
					id: primary(z.string()),
					deletedAt1: softDelete(z.date().nullable()),
					deletedAt2: softDelete(z.date().nullable()),
				}),
			).toThrow(
				'Table "invalid_table" has multiple soft delete fields: "deletedAt1" and "deletedAt2"',
			);
		});

		test("allows tables without soft delete field", () => {
			const NormalTable = table("normal", {
				id: primary(z.string()),
				name: z.string(),
			});
			expect(NormalTable._meta.softDeleteField).toBeNull();
		});
	});

	describe("Table.deleted()", () => {
		test("generates SQL fragment for soft delete check", () => {
			const fragment = SoftDeleteUsers.deleted();
			expect(fragment).toHaveProperty("sql");
			expect(fragment).toHaveProperty("params");
			expect(fragment.sql).toBe(
				'"soft_delete_users"."deletedAt" IS NOT NULL',
			);
			expect(fragment.params).toEqual([]);
		});

		test("throws if table has no soft delete field", () => {
			const NormalTable = table("normal", {
				id: primary(z.string()),
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

			const [sql] = (driver.all as any).mock.calls[0];
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

		test("soft deletes by primary key", async () => {
			(driver.run as any).mockImplementation(async () => 1);

			const deleted = await db.softDelete(SoftDeleteUsers, USER_ID);

			expect(deleted).toBe(true);

			const [sql, params] = (driver.run as any).mock.calls[0];
			expect(sql).toContain('UPDATE "soft_delete_users"');
			expect(sql).toContain('SET "deletedAt" = ?');
			expect(sql).toContain('WHERE "id" = ?');
			expect(params).toHaveLength(2);
			expect(params[0]).toBe(USER_ID);
			expect(params[1]).toBeInstanceOf(Date);
		});

		test("returns false if entity not found", async () => {
			(driver.run as any).mockImplementation(async () => 0);

			const deleted = await db.softDelete(SoftDeleteUsers, "nonexistent");

			expect(deleted).toBe(false);
		});

		test("throws if table has no primary key", async () => {
			const NoPkTable = table("no_pk", {
				name: z.string(),
				deletedAt: softDelete(z.date().nullable()),
			});

			await expect(db.softDelete(NoPkTable, "123")).rejects.toThrow(
				"Table no_pk has no primary key defined",
			);
		});

		test("throws if table has no soft delete field", async () => {
			const NormalTable = table("normal", {
				id: primary(z.string()),
				name: z.string(),
			});

			await expect(db.softDelete(NormalTable, "123")).rejects.toThrow(
				'Table normal does not have a soft delete field',
			);
		});
	});

	describe("Transaction.softDelete()", () => {
		test("soft deletes within transaction", async () => {
			const driver = createMockDriver();
			const db = new Database(driver);
			(driver.run as any).mockImplementation(async () => 1);

			const result = await db.transaction(async (tx) => {
				const deleted = await tx.softDelete(SoftDeleteUsers, USER_ID);
				return deleted;
			});

			expect(result).toBe(true);

			// Check transaction was called
			expect((driver.transaction as any).mock.calls.length).toBe(1);

			// Check UPDATE was called
			const [sql, params] = (driver.run as any).mock.calls[0];
			expect(sql).toContain('UPDATE "soft_delete_users"');
			expect(sql).toContain('SET "deletedAt" = ?');
			expect(sql).toContain('WHERE "id" = ?');
			expect(params).toHaveLength(2);
			expect(params[0]).toBe(USER_ID);
			expect(params[1]).toBeInstanceOf(Date);
		});

		test("throws if table has no soft delete field", async () => {
			const driver = createMockDriver();
			const db = new Database(driver);
			const NormalTable = table("normal", {
				id: primary(z.string()),
				name: z.string(),
			});

			await expect(
				db.transaction(async (tx) => {
					await tx.softDelete(NormalTable, "123");
				}),
			).rejects.toThrow('Table normal does not have a soft delete field');
		});
	});

	describe("Integration with pick()", () => {
		test("preserves soft delete field in partial table", () => {
			const PartialUsers = SoftDeleteUsers.pick("id", "name", "deletedAt");
			expect(PartialUsers._meta.softDeleteField).toBe("deletedAt");
		});

		test("removes soft delete field when not picked", () => {
			const PartialUsers = SoftDeleteUsers.pick("id", "name");
			expect(PartialUsers._meta.softDeleteField).toBeNull();
		});

		test("deleted() works on partial table with soft delete field", () => {
			const PartialUsers = SoftDeleteUsers.pick("id", "name", "deletedAt");
			const fragment = PartialUsers.deleted();
			expect(fragment.sql).toBe(
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
