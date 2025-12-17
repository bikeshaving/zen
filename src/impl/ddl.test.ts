import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, extendZod} from "./table.js";
import {generateDDL} from "./ddl.js";

// Extend Zod once before tests
extendZod(z);

describe("DDL generation", () => {
	test("basic table", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const ddl = generateDDL(users, {dialect: "sqlite"});

		expect(ddl).toContain('CREATE TABLE IF NOT EXISTS "users"');
		expect(ddl).toContain('"id" TEXT NOT NULL PRIMARY KEY');
		expect(ddl).toContain('"name" TEXT NOT NULL');
	});

	test("primary key and unique", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email().db.unique(),
		});

		const ddl = generateDDL(users, {dialect: "sqlite"});

		expect(ddl).toContain('"id" TEXT NOT NULL PRIMARY KEY');
		expect(ddl).toContain('"email" TEXT NOT NULL UNIQUE');
	});

	test("optional and nullable fields", () => {
		const profiles = table("profiles", {
			id: z.string().uuid().db.primary(),
			bio: z.string().optional(),
			avatar: z.string().nullable(),
		});

		const ddl = generateDDL(profiles, {dialect: "sqlite"});

		// Optional/nullable fields should not have NOT NULL
		expect(ddl).toContain('"bio" TEXT');
		expect(ddl).toContain('"avatar" TEXT');
		expect(ddl).not.toContain('"bio" TEXT NOT NULL');
		expect(ddl).not.toContain('"avatar" TEXT NOT NULL');
	});

	test("required fields", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			role: z.string(),
			active: z.boolean(),
			score: z.number(),
		});

		const ddl = generateDDL(users, {dialect: "sqlite"});

		expect(ddl).toContain('"role" TEXT NOT NULL');
		expect(ddl).toContain('"active" INTEGER NOT NULL'); // SQLite boolean
		expect(ddl).toContain('"score" REAL NOT NULL');
	});

	test("integer vs real", () => {
		const stats = table("stats", {
			id: z.string().uuid().db.primary(),
			count: z.number().int(),
			average: z.number(),
		});

		const ddl = generateDDL(stats, {dialect: "sqlite"});

		expect(ddl).toContain('"count" INTEGER');
		expect(ddl).toContain('"average" REAL');
	});

	test("enum as text", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			role: z.enum(["user", "admin", "moderator"]),
		});

		const ddl = generateDDL(users, {dialect: "sqlite"});

		expect(ddl).toContain('"role" TEXT NOT NULL');
	});

	test("date field", () => {
		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			createdAt: z.date(),
		});

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain('"createdAt" TEXT');
	});

	test("indexed field", () => {
		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.index(),
		});

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain('CREATE INDEX IF NOT EXISTS "idx_posts_authorId"');
		expect(ddl).toContain('ON "posts" ("authorId")');
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

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain(
			'CREATE INDEX IF NOT EXISTS "idx_posts_authorId_createdAt"',
		);
		expect(ddl).toContain('("authorId", "createdAt")');
	});

	test("json fields", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			settings: z.object({theme: z.string(), notifications: z.boolean()}),
			tags: z.array(z.string()),
		});

		const ddl = generateDDL(users, {dialect: "sqlite"});

		expect(ddl).toContain('"settings" TEXT');
		expect(ddl).toContain('"tags" TEXT');
	});

	test("postgresql dialect", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			score: z.number(),
			active: z.boolean(),
			createdAt: z.date(),
			settings: z.object({theme: z.string()}),
		});

		const ddl = generateDDL(users, {dialect: "postgresql"});

		expect(ddl).toContain('"score" DOUBLE PRECISION');
		expect(ddl).toContain('"active" BOOLEAN');
		expect(ddl).toContain('"createdAt" TIMESTAMPTZ');
		expect(ddl).toContain('"settings" JSONB');
		// PostgreSQL uses separate PRIMARY KEY constraint
		expect(ddl).toContain('PRIMARY KEY ("id")');
	});

	test("mysql dialect", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string().max(100),
		});

		const ddl = generateDDL(users, {dialect: "mysql"});

		// MySQL uses backticks
		expect(ddl).toContain("CREATE TABLE IF NOT EXISTS `users`");
		expect(ddl).toContain("`id` TEXT");
		expect(ddl).toContain("`name` VARCHAR(100)");
	});

	test("foreign key constraint", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.references(users, {as: "author"}),
			title: z.string(),
		});

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain('FOREIGN KEY ("authorId") REFERENCES "users"("id")');
	});

	test("foreign key with onDelete cascade", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.references(users, {
				as: "author",
				onDelete: "cascade",
			}),
			title: z.string(),
		});

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain(
			'FOREIGN KEY ("authorId") REFERENCES "users"("id") ON DELETE CASCADE',
		);
	});

	test("foreign key with onDelete set null", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().nullable().db.references(users, {
				as: "author",
				onDelete: "set null",
			}),
			title: z.string(),
		});

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain(
			'FOREIGN KEY ("authorId") REFERENCES "users"("id") ON DELETE SET NULL',
		);
	});

	test("foreign key with onDelete restrict", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.references(users, {
				as: "author",
				onDelete: "restrict",
			}),
			title: z.string(),
		});

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain(
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
			authorId: z.string().uuid().db.references(users, {
				as: "author",
				onDelete: "cascade",
			}),
			categoryId: z.string().uuid().nullable().db.references(categories, {
				as: "category",
				onDelete: "set null",
			}),
			title: z.string(),
		});

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain(
			'FOREIGN KEY ("authorId") REFERENCES "users"("id") ON DELETE CASCADE',
		);
		expect(ddl).toContain(
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
			authorEmail: z.string().email().db.references(users, {
				field: "email",
				as: "author",
			}),
			title: z.string(),
		});

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain(
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
			authorId: z.string().uuid().db.references(users, {
				as: "author",
				onDelete: "cascade",
			}),
			title: z.string(),
		});

		const ddl = generateDDL(posts, {dialect: "postgresql"});

		expect(ddl).toContain(
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
			authorId: z.string().uuid().db.references(users, {
				as: "author",
				onDelete: "cascade",
			}),
			title: z.string(),
		});

		const ddl = generateDDL(posts, {dialect: "mysql"});

		expect(ddl).toContain(
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

		const ddl = generateDDL(custom, {dialect: "postgresql"});

		// Should use TEXT, not JSONB (which would be inferred from z.array())
		expect(ddl).toContain('"tags" TEXT');
		expect(ddl).not.toContain("JSONB");
	});

	test("explicit column type overrides inferred type for object", () => {
		const custom = table("custom", {
			id: z.string().uuid().db.primary(),
			// Object stored as BLOB for some reason
			data: z.object({foo: z.string()}).db.type("BLOB"),
		});

		const ddl = generateDDL(custom, {dialect: "sqlite"});

		expect(ddl).toContain('"data" BLOB');
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

		const ddl = generateDDL(custom, {dialect: "sqlite"});

		expect(ddl).toContain('"tags" TEXT');
	});

	test("explicit column type across dialects", () => {
		const custom = table("custom", {
			id: z.string().uuid().db.primary(),
			binary: z.string().db.type("BYTEA"),
		});

		const pgDdl = generateDDL(custom, {dialect: "postgresql"});
		const sqliteDdl = generateDDL(custom, {dialect: "sqlite"});

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

		const ddl = generateDDL(posts, {dialect: "sqlite"});

		expect(ddl).toContain('UNIQUE ("authorId", "slug")');
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

		const ddl = generateDDL(items, {dialect: "sqlite"});

		expect(ddl).toContain('UNIQUE ("a", "b")');
		expect(ddl).toContain('UNIQUE ("b", "c")');
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

		const ddl = generateDDL(orderItems, {dialect: "sqlite"});

		expect(ddl).toContain(
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

		const ddl = generateDDL(childTable, {dialect: "sqlite"});

		expect(ddl).toContain(
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

		const ddl = generateDDL(child, {dialect: "sqlite"});

		expect(ddl).toContain(
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

		const ddl = generateDDL(child, {dialect: "mysql"});

		expect(ddl).toContain("UNIQUE (`parentA`, `code`)");
		expect(ddl).toContain(
			"FOREIGN KEY (`parentA`, `parentB`) REFERENCES `parent`(`a`, `b`)",
		);
	});

	describe("autoIncrement", () => {
		test("SQLite auto-increment primary key", () => {
			const items = table("items", {
				id: z.number().int().db.primary().db.autoIncrement(),
				name: z.string(),
			});

			const ddl = generateDDL(items, {dialect: "sqlite"});

			// SQLite uses INTEGER PRIMARY KEY AUTOINCREMENT
			expect(ddl).toContain('"id" INTEGER PRIMARY KEY AUTOINCREMENT');
			// Should not have separate PRIMARY KEY constraint
			expect(ddl).not.toContain('PRIMARY KEY ("id")');
		});

		test("PostgreSQL auto-increment with GENERATED ALWAYS AS IDENTITY", () => {
			const items = table("items", {
				id: z.number().int().db.primary().db.autoIncrement(),
				name: z.string(),
			});

			const ddl = generateDDL(items, {dialect: "postgresql"});

			// PostgreSQL uses GENERATED ALWAYS AS IDENTITY (SQL standard)
			expect(ddl).toContain('"id" INTEGER GENERATED ALWAYS AS IDENTITY');
			// Should still have PRIMARY KEY constraint
			expect(ddl).toContain('PRIMARY KEY ("id")');
		});

		test("MySQL auto-increment", () => {
			const items = table("items", {
				id: z.number().int().db.primary().db.autoIncrement(),
				name: z.string(),
			});

			const ddl = generateDDL(items, {dialect: "mysql"});

			// MySQL uses AUTO_INCREMENT
			expect(ddl).toContain("`id` INTEGER AUTO_INCREMENT");
			// Should have PRIMARY KEY constraint
			expect(ddl).toContain("PRIMARY KEY (`id`)");
		});

		test("auto-increment excludes NOT NULL and DEFAULT", () => {
			const items = table("items", {
				id: z.number().int().db.primary().db.autoIncrement(),
				name: z.string(),
			});

			const sqliteDdl = generateDDL(items, {dialect: "sqlite"});
			const pgDdl = generateDDL(items, {dialect: "postgresql"});

			// Auto-increment columns should not have explicit NOT NULL (implicit)
			expect(sqliteDdl).not.toContain('"id" INTEGER NOT NULL');
			expect(pgDdl).not.toContain('"id" INTEGER NOT NULL');
			// Should not have DEFAULT clause
			expect(sqliteDdl).not.toContain("DEFAULT");
			expect(pgDdl).not.toContain("DEFAULT");
		});
	});
});
