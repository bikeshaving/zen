import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, extendZod} from "../src/impl/table.js";
import {generateDDL, type SQLDialect} from "../src/impl/ddl.js";
import {renderDDL} from "../src/impl/sql.js";

// Extend Zod once before tests
extendZod(z);

// Helper to generate DDL string for tests
function ddl(
	tbl: ReturnType<typeof table>,
	dialect: SQLDialect = "sqlite",
): string {
	const template = generateDDL(tbl, {dialect});
	return renderDDL(template[0], template.slice(1), dialect);
}

describe("DDL generation", () => {
	test("basic table", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const result = ddl(users, "sqlite");

		expect(result).toContain('CREATE TABLE IF NOT EXISTS "users"');
		expect(result).toContain('"id" TEXT NOT NULL PRIMARY KEY');
		expect(result).toContain('"name" TEXT NOT NULL');
	});

	test("primary key and unique", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email().db.unique(),
		});

		const result = ddl(users, "sqlite");

		expect(result).toContain('"id" TEXT NOT NULL PRIMARY KEY');
		expect(result).toContain('"email" TEXT NOT NULL UNIQUE');
	});

	test("optional and nullable fields", () => {
		const profiles = table("profiles", {
			id: z.string().uuid().db.primary(),
			bio: z.string().optional(),
			avatar: z.string().nullable(),
		});

		const result = ddl(profiles, "sqlite");

		// Optional/nullable fields should not have NOT NULL
		expect(result).toContain('"bio" TEXT');
		expect(result).toContain('"avatar" TEXT');
		expect(result).not.toContain('"bio" TEXT NOT NULL');
		expect(result).not.toContain('"avatar" TEXT NOT NULL');
	});

	test("required fields", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			role: z.string(),
			active: z.boolean(),
			score: z.number(),
		});

		const result = ddl(users, "sqlite");

		expect(result).toContain('"role" TEXT NOT NULL');
		expect(result).toContain('"active" INTEGER NOT NULL'); // SQLite boolean
		expect(result).toContain('"score" REAL NOT NULL');
	});

	test("integer vs real", () => {
		const stats = table("stats", {
			id: z.string().uuid().db.primary(),
			count: z.number().int(),
			average: z.number(),
		});

		const result = ddl(stats, "sqlite");

		expect(result).toContain('"count" INTEGER');
		expect(result).toContain('"average" REAL');
	});

	test("enum as text", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			role: z.enum(["user", "admin", "moderator"]),
		});

		const result = ddl(users, "sqlite");

		expect(result).toContain('"role" TEXT NOT NULL');
	});

	test("date field", () => {
		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			createdAt: z.date(),
		});

		const result = ddl(posts, "sqlite");

		expect(result).toContain('"createdAt" TEXT');
	});

	test("indexed field", () => {
		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.index(),
		});

		const result = ddl(posts, "sqlite");

		expect(result).toContain('CREATE INDEX IF NOT EXISTS "idx_posts_authorId"');
		expect(result).toContain('ON "posts" ("authorId")');
	});

	test("compound indexes", () => {
		const posts = table(
			"posts",
			{
				id: z.string().uuid().db.primary(),
				authorId: z.string().uuid(),
				createdAt: z.date(),
			},
			{
				indexes: [["authorId", "createdAt"]],
			},
		);

		const result = ddl(posts, "sqlite");

		expect(result).toContain(
			'CREATE INDEX IF NOT EXISTS "idx_posts_authorId_createdAt"',
		);
		expect(result).toContain('("authorId", "createdAt")');
	});

	test("json fields", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			settings: z.object({theme: z.string(), notifications: z.boolean()}),
			tags: z.array(z.string()),
		});

		const result = ddl(users, "sqlite");

		expect(result).toContain('"settings" TEXT');
		expect(result).toContain('"tags" TEXT');
	});

	test("postgresql dialect", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			score: z.number(),
			active: z.boolean(),
			createdAt: z.date(),
			settings: z.object({theme: z.string()}),
		});

		const result = ddl(users, "postgresql");

		expect(result).toContain('"score" DOUBLE PRECISION');
		expect(result).toContain('"active" BOOLEAN');
		expect(result).toContain('"createdAt" TIMESTAMPTZ');
		expect(result).toContain('"settings" JSONB');
		// PostgreSQL uses separate PRIMARY KEY constraint
		expect(result).toContain('PRIMARY KEY ("id")');
	});

	test("mysql dialect", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string().max(100),
		});

		const result = ddl(users, "mysql");

		// MySQL uses backticks
		expect(result).toContain("CREATE TABLE IF NOT EXISTS `users`");
		expect(result).toContain("`id` TEXT");
		expect(result).toContain("`name` VARCHAR(100)");
	});

	test("foreign key constraint", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.references(users, "author"),
			title: z.string(),
		});

		const result = ddl(posts, "sqlite");

		expect(result).toContain(
			'FOREIGN KEY ("authorId") REFERENCES "users"("id")',
		);
	});

	test("foreign key with ondelete cascade", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.references(users, "author", {
				onDelete: "cascade",
			}),
			title: z.string(),
		});

		const result = ddl(posts, "sqlite");

		expect(result).toContain(
			'FOREIGN KEY ("authorId") REFERENCES "users"("id") ON DELETE CASCADE',
		);
	});

	test("foreign key with ondelete set null", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().nullable().db.references(users, "author", {
				onDelete: "set null",
			}),
			title: z.string(),
		});

		const result = ddl(posts, "sqlite");

		expect(result).toContain(
			'FOREIGN KEY ("authorId") REFERENCES "users"("id") ON DELETE SET NULL',
		);
	});

	test("foreign key with ondelete restrict", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.references(users, "author", {
				onDelete: "restrict",
			}),
			title: z.string(),
		});

		const result = ddl(posts, "sqlite");

		expect(result).toContain(
			'FOREIGN KEY ("authorId") REFERENCES "users"("id") ON DELETE RESTRICT',
		);
	});

	test("multiple foreign keys", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const categories = table("categories", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.references(users, "author", {
				onDelete: "cascade",
			}),
			categoryId: z.string().uuid().nullable().db.references(categories, "category", {
				onDelete: "set null",
			}),
			title: z.string(),
		});

		const result = ddl(posts, "sqlite");

		expect(result).toContain(
			'FOREIGN KEY ("authorId") REFERENCES "users"("id") ON DELETE CASCADE',
		);
		expect(result).toContain(
			'FOREIGN KEY ("categoryId") REFERENCES "categories"("id") ON DELETE SET NULL',
		);
	});

	test("foreign key with custom field reference", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email().db.unique(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorEmail: z.string().email().db.references(users, "author", {
				field: "email",
			}),
			title: z.string(),
		});

		const result = ddl(posts, "sqlite");

		expect(result).toContain(
			'FOREIGN KEY ("authorEmail") REFERENCES "users"("email")',
		);
	});

	test("foreign key in postgresql", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.references(users, "author", {
				onDelete: "cascade",
			}),
			title: z.string(),
		});

		const result = ddl(posts, "postgresql");

		expect(result).toContain(
			'FOREIGN KEY ("authorId") REFERENCES "users"("id") ON DELETE CASCADE',
		);
	});

	test("foreign key in mysql", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.references(users, "author", {
				onDelete: "cascade",
			}),
			title: z.string(),
		});

		const result = ddl(posts, "mysql");

		expect(result).toContain(
			"FOREIGN KEY (`authorId`) REFERENCES `users`(`id`) ON DELETE CASCADE",
		);
	});

	test("explicit column type with .db.type()", () => {
		const custom = table("custom", {
			id: z.string().uuid().db.primary(),
			// Array with custom CSV encoding - explicit TEXT type
			tags: z
				.array(z.string())
				.db.encode((arr) => arr.join(","))
				.db.decode((str: string) => str.split(","))
				.db.type("TEXT"),
		});

		const result = ddl(custom, "postgresql");

		// Should use TEXT, not JSONB (which would be inferred from z.array())
		expect(result).toContain('"tags" TEXT');
		expect(result).not.toContain("JSONB");
	});

	test("explicit column type overrides inferred type for object", () => {
		const custom = table("custom", {
			id: z.string().uuid().db.primary(),
			// Object stored as BLOB for some reason
			data: z.object({foo: z.string()}).db.type("BLOB"),
		});

		const result = ddl(custom, "sqlite");

		expect(result).toContain('"data" BLOB');
	});

	test("explicit column type with encode/decode", () => {
		const custom = table("custom", {
			id: z.string().uuid().db.primary(),
			tags: z
				.array(z.string())
				.db.encode((arr) => arr.join(","))
				.db.decode((str: string) => str.split(","))
				.db.type("TEXT"),
		});

		const result = ddl(custom, "sqlite");

		expect(result).toContain('"tags" TEXT');
	});

	test("explicit column type across dialects", () => {
		const custom = table("custom", {
			id: z.string().uuid().db.primary(),
			binary: z.string().db.type("BYTEA"),
		});

		const pgDdl = ddl(custom, "postgresql");
		const sqliteDdl = ddl(custom, "sqlite");

		// Explicit type is used as-is in all dialects
		expect(pgDdl).toContain('"binary" BYTEA');
		expect(sqliteDdl).toContain('"binary" BYTEA');
	});

	test("compound unique constraint", () => {
		const posts = table(
			"posts",
			{
				id: z.string().uuid().db.primary(),
				authorId: z.string().uuid(),
				slug: z.string(),
				title: z.string(),
			},
			{unique: [["authorId", "slug"]]},
		);

		const result = ddl(posts, "sqlite");

		expect(result).toContain('UNIQUE ("authorId", "slug")');
	});

	test("multiple compound unique constraints", () => {
		const items = table(
			"items",
			{
				id: z.string().uuid().db.primary(),
				a: z.string(),
				b: z.string(),
				c: z.string(),
			},
			{
				unique: [
					["a", "b"],
					["b", "c"],
				],
			},
		);

		const result = ddl(items, "sqlite");

		expect(result).toContain('UNIQUE ("a", "b")');
		expect(result).toContain('UNIQUE ("b", "c")');
	});

	test("compound foreign key", () => {
		const orderProducts = table("order_products", {
			orderId: z.string().uuid(),
			productId: z.string().uuid(),
			quantity: z.number().int(),
		});

		const orderItems = table(
			"order_items",
			{
				id: z.string().uuid().db.primary(),
				orderId: z.string().uuid(),
				productId: z.string().uuid(),
				price: z.number(),
			},
			{
				references: [
					{
						fields: ["orderId", "productId"],
						table: orderProducts,
						as: "orderProduct",
					},
				],
			},
		);

		const result = ddl(orderItems, "sqlite");

		expect(result).toContain(
			'FOREIGN KEY ("orderId", "productId") REFERENCES "order_products"("orderId", "productId")',
		);
	});

	test("compound foreign key with custom referenced fields", () => {
		const refTable = table("ref_table", {
			keyA: z.string(),
			keyB: z.string(),
			data: z.string(),
		});

		const childTable = table(
			"child_table",
			{
				id: z.string().uuid().db.primary(),
				fkA: z.string(),
				fkB: z.string(),
			},
			{
				references: [
					{
						fields: ["fkA", "fkB"],
						table: refTable,
						referencedFields: ["keyA", "keyB"],
						as: "ref",
					},
				],
			},
		);

		const result = ddl(childTable, "sqlite");

		expect(result).toContain(
			'FOREIGN KEY ("fkA", "fkB") REFERENCES "ref_table"("keyA", "keyB")',
		);
	});

	test("compound foreign key with onDelete", () => {
		const parent = table("parent", {
			a: z.string(),
			b: z.string(),
		});

		const child = table(
			"child",
			{
				id: z.string().uuid().db.primary(),
				parentA: z.string(),
				parentB: z.string(),
			},
			{
				references: [
					{
						fields: ["parentA", "parentB"],
						table: parent,
						referencedFields: ["a", "b"],
						as: "parent",
						onDelete: "cascade",
					},
				],
			},
		);

		const result = ddl(child, "sqlite");

		expect(result).toContain(
			'FOREIGN KEY ("parentA", "parentB") REFERENCES "parent"("a", "b") ON DELETE CASCADE',
		);
	});

	test("compound constraints in mysql dialect", () => {
		const parent = table("parent", {
			a: z.string(),
			b: z.string(),
		});

		const child = table(
			"child",
			{
				id: z.string().uuid().db.primary(),
				parentA: z.string(),
				parentB: z.string(),
				code: z.string(),
			},
			{
				unique: [["parentA", "code"]],
				references: [
					{
						fields: ["parentA", "parentB"],
						table: parent,
						referencedFields: ["a", "b"],
						as: "parent",
					},
				],
			},
		);

		const result = ddl(child, "mysql");

		expect(result).toContain("UNIQUE (`parentA`, `code`)");
		expect(result).toContain(
			"FOREIGN KEY (`parentA`, `parentB`) REFERENCES `parent`(`a`, `b`)",
		);
	});

	describe("autoIncrement", () => {
		test("SQLite auto-increment primary key", () => {
			const items = table("items", {
				id: z.number().int().db.primary().db.autoIncrement(),
				name: z.string(),
			});

			const result = ddl(items, "sqlite");

			// SQLite uses INTEGER PRIMARY KEY AUTOINCREMENT
			expect(result).toContain('"id" INTEGER PRIMARY KEY AUTOINCREMENT');
			// Should not have separate PRIMARY KEY constraint
			expect(result).not.toContain('PRIMARY KEY ("id")');
		});

		test("PostgreSQL auto-increment with GENERATED ALWAYS AS IDENTITY", () => {
			const items = table("items", {
				id: z.number().int().db.primary().db.autoIncrement(),
				name: z.string(),
			});

			const result = ddl(items, "postgresql");

			// PostgreSQL uses GENERATED ALWAYS AS IDENTITY (SQL standard)
			expect(result).toContain('"id" INTEGER GENERATED ALWAYS AS IDENTITY');
			// Should still have PRIMARY KEY constraint
			expect(result).toContain('PRIMARY KEY ("id")');
		});

		test("MySQL auto-increment", () => {
			const items = table("items", {
				id: z.number().int().db.primary().db.autoIncrement(),
				name: z.string(),
			});

			const result = ddl(items, "mysql");

			// MySQL uses AUTO_INCREMENT
			expect(result).toContain("`id` INTEGER AUTO_INCREMENT");
			// Should have PRIMARY KEY constraint
			expect(result).toContain("PRIMARY KEY (`id`)");
		});

		test("auto-increment excludes NOT NULL and DEFAULT", () => {
			const items = table("items", {
				id: z.number().int().db.primary().db.autoIncrement(),
				name: z.string(),
			});

			const sqliteDdl = ddl(items, "sqlite");
			const pgDdl = ddl(items, "postgresql");

			// Auto-increment columns should not have explicit NOT NULL (implicit)
			expect(sqliteDdl).not.toContain('"id" INTEGER NOT NULL');
			expect(pgDdl).not.toContain('"id" INTEGER NOT NULL');
			// Should not have DEFAULT clause
			expect(sqliteDdl).not.toContain("DEFAULT");
			expect(pgDdl).not.toContain("DEFAULT");
		});
	});
});
