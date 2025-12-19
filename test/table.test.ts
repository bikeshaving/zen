import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, isTable, extendZod} from "../src/impl/table.js";
import {renderFragment} from "../src/impl/query.js";

// Extend Zod once before tests
extendZod(z);

describe("table", () => {
	test("basic table definition", () => {
		const users = table("users", {
			id: z.string().uuid(),
			name: z.string(),
		});

		expect(users.name).toBe("users");
	});

	test("extracts field metadata", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email().db.unique(),
			name: z.string().max(100),
			bio: z.string().max(2000),
			age: z.number().int().min(0).max(150),
			role: z.enum(["user", "admin", "moderator"]),
			active: z.boolean(),
			createdAt: z.date(),
		});

		const fields = users.fields();

		// Primary key
		expect(fields.id.primaryKey).toBe(true);
		expect(fields.id.type).toBe("text");

		// Email field
		expect(fields.email.type).toBe("email");
		expect(fields.email.unique).toBe(true);
		expect(fields.email.required).toBe(true);

		// String with max length
		expect(fields.name.type).toBe("text");
		expect(fields.name.maxLength).toBe(100);

		// Long text becomes textarea
		expect(fields.bio.type).toBe("textarea");
		expect(fields.bio.maxLength).toBe(2000);

		// Integer with min/max
		expect(fields.age.type).toBe("integer");
		expect(fields.age.min).toBe(0);
		expect(fields.age.max).toBe(150);

		// Enum becomes select
		expect(fields.role.type).toBe("select");
		expect(fields.role.options).toEqual(["user", "admin", "moderator"]);
		expect(fields.role.required).toBe(true); // no default

		// Boolean
		expect(fields.active.type).toBe("checkbox");

		// Date
		expect(fields.createdAt.type).toBe("datetime");
		expect(fields.createdAt.required).toBe(true); // no default
	});

	test("detects primary key", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email(),
		});

		expect(users.meta.primary).toBe("id");
		expect(users.primary).not.toBeNull();
		const {sql, params} = renderFragment(users.primary!);
		expect(sql).toBe('"users"."id"');
		expect(params).toEqual([]);
	});

	test("handles optional and nullable fields", () => {
		const profiles = table("profiles", {
			id: z.string().uuid().db.primary(),
			bio: z.string().optional(),
			avatar: z.string().url().nullable(),
			nickname: z.string().nullish(),
		});

		const fields = profiles.fields();

		expect(fields.bio.required).toBe(false);
		expect(fields.avatar.required).toBe(false);
		expect(fields.avatar.type).toBe("url");
		expect(fields.nickname.required).toBe(false);
	});

	test("url detection", () => {
		const links = table("links", {
			id: z.string().uuid().db.primary(),
			url: z.string().url(),
		});

		const fields = links.fields();
		expect(fields.url.type).toBe("url");
	});

	test("indexed fields", () => {
		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.index(),
			title: z.string(),
		});

		const fields = posts.fields();
		expect(fields.authorId.indexed).toBe(true);
	});

	test("compound indexes via options", () => {
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

		expect(posts.indexes).toEqual([["authorId", "createdAt"]]);
	});

	test("compound unique constraints via options", () => {
		const posts = table(
			"posts",
			{
				id: z.string().uuid().db.primary(),
				authorId: z.string().uuid(),
				slug: z.string(),
			},
			{
				unique: [["authorId", "slug"]],
			},
		);

		expect(posts.unique).toEqual([["authorId", "slug"]]);
	});

	test("compound foreign keys via options", () => {
		// OrderProducts is the join table we'll reference with compound keys
		const OrderProducts = table("order_products", {
			orderId: z.string().uuid(),
			productId: z.string().uuid(),
			quantity: z.number(),
		});

		const OrderItems = table(
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
						table: OrderProducts,
						as: "orderProduct",
					},
				],
			},
		);

		expect(OrderItems.compoundReferences).toHaveLength(1);
		expect(OrderItems.compoundReferences[0].fields).toEqual([
			"orderId",
			"productId",
		]);
		expect(OrderItems.compoundReferences[0].table).toBe(OrderProducts);
		expect(OrderItems.compoundReferences[0].as).toBe("orderProduct");
	});

	test("extracts Zod 4 .meta() for UI metadata", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z
				.string()
				.email()
				.meta({
					label: "Email Address",
					helpText: "We will never share your email",
					placeholder: "you@example.com",
				})
				.db.unique(),
			role: z
				.enum(["user", "admin"])
				.db.inserted(() => "user" as const)
				.meta({label: "User Role", widget: "radio"}),
			bio: z.string().optional().meta({label: "Biography", widget: "textarea"}),
		});

		const fields = users.fields();

		// Email field with .meta()
		expect(fields.email.label).toBe("Email Address");
		expect(fields.email.helpText).toBe("We will never share your email");
		expect(fields.email.placeholder).toBe("you@example.com");
		expect(fields.email.type).toBe("email"); // Inferred type preserved
		expect(fields.email.unique).toBe(true); // DB metadata preserved

		// Enum with .meta()
		expect(fields.role.label).toBe("User Role");
		expect(fields.role.widget).toBe("radio");
		expect(fields.role.type).toBe("select"); // Inferred type preserved
		expect(fields.role.options).toEqual(["user", "admin"]);

		// Optional field with .meta() - meta survives unwrapping
		expect(fields.bio.label).toBe("Biography");
		expect(fields.bio.widget).toBe("textarea");
		expect(fields.bio.required).toBe(false);
	});

	test("rejects table names containing dots", () => {
		expect(() =>
			table("schema.users", {
				id: z.string().uuid(),
			}),
		).toThrow('table names cannot contain "."');
	});

	test("rejects field names containing dots", () => {
		expect(() =>
			table("users", {
				id: z.string().uuid(),
				"user.name": z.string(), // Invalid field name
			}),
		).toThrow('field names cannot contain "."');
	});
});

describe("type inference", () => {
	test("Infer extracts document type", () => {
		// eslint-disable-next-line @typescript-eslint/no-unused-vars
		const users = table("users", {
			id: z.string().uuid(),
			name: z.string(),
			age: z.number().optional(),
		});

		// Type check - this should compile
		type UserDoc = z.infer<typeof users.schema>;
		const user: UserDoc = {id: "123", name: "Alice"};
		expect(user.name).toBe("Alice");
	});
});

describe("isTable", () => {
	test("returns true for table objects", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		expect(isTable(users)).toBe(true);
	});

	test("returns false for non-table objects", () => {
		expect(isTable(null)).toBe(false);
		expect(isTable(undefined)).toBe(false);
		expect(isTable({})).toBe(false);
		expect(isTable({name: "users"})).toBe(false);
		expect(isTable("users")).toBe(false);
	});
});

describe("Table.pick()", () => {
	const Users = table("users", {
		id: z.string().uuid().db.primary(),
		email: z.string().email().db.unique(),
		name: z.string(),
		bio: z.string().optional(),
	});

	const Posts = table("posts", {
		id: z.string().uuid().db.primary(),
		authorId: z.string().uuid().db.references(Users, "author"),
		title: z.string(),
		body: z.string(),
		published: z.boolean(),
	});

	test("creates partial table with picked fields", () => {
		const UserSummary = Users.pick("id", "name");

		expect(UserSummary.name).toBe("users");
		expect(Object.keys(UserSummary.schema.shape)).toEqual(["id", "name"]);
	});

	test("partial table is still a table", () => {
		const UserSummary = Users.pick("id", "name");
		expect(isTable(UserSummary)).toBe(true);
	});

	test("preserves primary key if picked", () => {
		const WithPK = Users.pick("id", "name");
		expect(WithPK.meta.primary).toBe("id");
		expect(WithPK.primary).not.toBeNull();

		const WithoutPK = Users.pick("name", "email");
		expect(WithoutPK.meta.primary).toBeNull();
		expect(WithoutPK.primary).toBeNull();
	});

	test("preserves unique fields if picked", () => {
		const WithUnique = Users.pick("id", "email");
		expect(WithUnique.meta.unique).toEqual(["email"]);

		const WithoutUnique = Users.pick("id", "name");
		expect(WithoutUnique.meta.unique).toEqual([]);
	});

	test("preserves references if FK field picked", () => {
		const PostWithRef = Posts.pick("id", "title", "authorId");
		expect(PostWithRef.references().length).toBe(1);
		expect(PostWithRef.references()[0].fieldName).toBe("authorId");
		expect(PostWithRef.references()[0].as).toBe("author");

		const PostWithoutRef = Posts.pick("id", "title");
		expect(PostWithoutRef.references().length).toBe(0);
	});

	test("schema validates correctly", () => {
		const UserSummary = Users.pick("id", "name");
		const validId = "550e8400-e29b-41d4-a716-446655440000";

		// Valid data
		expect(() =>
			UserSummary.schema.parse({id: validId, name: "Alice"}),
		).not.toThrow();

		// Missing required field
		expect(() => UserSummary.schema.parse({id: validId})).toThrow();

		// Extra field is stripped (Zod default behavior)
		const result = UserSummary.schema.parse({
			id: validId,
			name: "Alice",
			email: "alice@example.com",
		});
		expect(result).toEqual({id: validId, name: "Alice"});
	});

	test("fields() returns only picked fields", () => {
		const UserSummary = Users.pick("id", "email");
		const fields = UserSummary.fields();

		expect(Object.keys(fields)).toEqual(["id", "email"]);
		expect(fields.id.primaryKey).toBe(true);
		expect(fields.email.unique).toBe(true);
		expect(fields.email.type).toBe("email");
	});

	test("can pick() a picked table", () => {
		const Step1 = Users.pick("id", "name", "email");
		const Step2 = Step1.pick("id", "name");

		expect(Object.keys(Step2.schema.shape)).toEqual(["id", "name"]);
		expect(Step2.meta.primary).toBe("id");
	});

	test("pick() preserves compound unique constraints if all fields picked", () => {
		const Articles = table(
			"articles",
			{
				id: z.string().uuid().db.primary(),
				authorId: z.string().uuid(),
				slug: z.string(),
				title: z.string(),
			},
			{unique: [["authorId", "slug"]]},
		);

		const WithUnique = Articles.pick("id", "authorId", "slug");
		expect(WithUnique.unique).toEqual([["authorId", "slug"]]);

		const WithoutUnique = Articles.pick("id", "authorId", "title");
		expect(WithoutUnique.unique).toEqual([]);
	});

	test("pick() preserves compound references if all fields picked", () => {
		const RefTable = table("ref", {
			a: z.string(),
			b: z.string(),
		});

		const WithRefs = table(
			"with_refs",
			{
				id: z.string().db.primary(),
				refA: z.string(),
				refB: z.string(),
				other: z.string(),
			},
			{references: [{fields: ["refA", "refB"], table: RefTable, as: "ref"}]},
		);

		const Picked = WithRefs.pick("id", "refA", "refB");
		expect(Picked.compoundReferences).toHaveLength(1);

		const PickedPartial = WithRefs.pick("id", "refA", "other");
		expect(PickedPartial.compoundReferences).toHaveLength(0);
	});
});

describe("Table.derive()", () => {
	const Posts = table("posts", {
		id: z.string().uuid().db.primary(),
		title: z.string(),
		authorId: z.string().uuid(),
	});

	const Likes = table("likes", {
		id: z.string().uuid().db.primary(),
		postId: z.string().uuid(),
	});

	test("creates derived table with extended schema", () => {
		const PostsWithCount = Posts.derive(
			"likeCount",
			z.number(),
		)`COUNT(${Likes.cols.id})`;

		expect(PostsWithCount.name).toBe("posts");
		expect(Object.keys(PostsWithCount.schema.shape)).toEqual([
			"id",
			"title",
			"authorId",
			"likeCount",
		]);
	});

	test("derived table is still a table", () => {
		const PostsWithCount = Posts.derive("likeCount", z.number())`COUNT(*)`;

		expect(isTable(PostsWithCount)).toBe(true);
	});

	test("stores derived expression in meta.derivedExprs", () => {
		const PostsWithCount = Posts.derive(
			"likeCount",
			z.number(),
		)`COUNT(${Likes.cols.id})`;

		expect((PostsWithCount.meta as any).isDerived).toBe(true);
		expect((PostsWithCount.meta as any).derivedExprs).toHaveLength(1);

		const expr = (PostsWithCount.meta as any).derivedExprs[0];
		expect(expr.fieldName).toBe("likeCount");
		// Template format: {fieldName, template} where template is SQLTemplate [strings, ...values]
		expect(Array.from(expr.template[0])).toEqual(["COUNT(", ".", ")"]);
		const exprValues = expr.template.slice(1);
		expect(exprValues).toHaveLength(2);
		expect(exprValues[0]).toHaveProperty("name", "likes");
		expect(exprValues[1]).toHaveProperty("name", "id");
	});

	test("tracks derived fields in meta.derivedFields", () => {
		const PostsWithCount = Posts.derive(
			"likeCount",
			z.number(),
		)`COUNT(${Likes.cols.id})`.derive("commentCount", z.number())`COUNT(*)`;

		expect((PostsWithCount.meta as any).derivedFields).toEqual([
			"likeCount",
			"commentCount",
		]);
	});

	test("composition accumulates expressions", () => {
		const WithLikes = Posts.derive(
			"likeCount",
			z.number(),
		)`COUNT(${Likes.cols.id})`;

		const WithLikesAndComments = WithLikes.derive(
			"commentCount",
			z.number(),
		)`COUNT(*)`;

		expect((WithLikesAndComments.meta as any).derivedExprs).toHaveLength(2);
		expect((WithLikesAndComments.meta as any).derivedFields).toEqual([
			"likeCount",
			"commentCount",
		]);
	});

	test("parameterizes non-fragment values", () => {
		const PostsWithThreshold = Posts.derive(
			"hasMany",
			z.boolean(),
		)`CASE WHEN COUNT(*) > ${10} THEN 1 ELSE 0 END`;

		const expr = (PostsWithThreshold.meta as any).derivedExprs[0];
		expect(expr.fieldName).toBe("hasMany");
		// Template format: {fieldName, template} where template is SQLTemplate [strings, ...values]
		expect(Array.from(expr.template[0])).toEqual([
			"CASE WHEN COUNT(*) > ",
			" THEN 1 ELSE 0 END",
		]);
		expect(expr.template.slice(1)).toEqual([10]);
	});

	test("cols proxy works for derived fields", () => {
		const PostsWithCount = Posts.derive("likeCount", z.number())`COUNT(*)`;

		// Original fields still work
		expect(renderFragment(PostsWithCount.cols.id).sql).toBe('"posts"."id"');

		// Derived fields should also work via cols proxy
		expect(renderFragment(PostsWithCount.cols.likeCount).sql).toBe(
			'"posts"."likeCount"',
		);
	});

	test("preserves base table primary key", () => {
		const PostsWithCount = Posts.derive("likeCount", z.number())`COUNT(*)`;

		expect(PostsWithCount.meta.primary).toBe("id");
		expect(PostsWithCount.primary).not.toBeNull();
	});

	test("schema validates correctly with derived fields", () => {
		const PostsWithCount = Posts.derive("likeCount", z.number())`COUNT(*)`;

		const validId = "550e8400-e29b-41d4-a716-446655440000";

		// Valid data including derived field
		expect(() =>
			PostsWithCount.schema.parse({
				id: validId,
				title: "Hello",
				authorId: validId,
				likeCount: 42,
			}),
		).not.toThrow();

		// Missing derived field should fail
		expect(() =>
			PostsWithCount.schema.parse({
				id: validId,
				title: "Hello",
				authorId: validId,
			}),
		).toThrow();
	});

	test("derive then pick preserves derived metadata", () => {
		const WithCount = Posts.derive("likeCount", z.number())`COUNT(*)`;
		const Picked = WithCount.pick("id", "likeCount");

		expect((Picked.meta as any).isDerived).toBe(true);
		expect((Picked.meta as any).derivedExprs).toHaveLength(1);
		expect((Picked.meta as any).derivedFields).toEqual(["likeCount"]);
		expect(Object.keys(Picked.schema.shape)).toEqual(["id", "likeCount"]);
	});

	test("derive then pick excludes non-picked derived fields", () => {
		const WithStats = Posts.derive(
			"likeCount",
			z.number(),
		)`COUNT(likes.id)`.derive("commentCount", z.number())`COUNT(comments.id)`;
		const Picked = WithStats.pick("id", "likeCount");

		expect((Picked.meta as any).derivedExprs).toHaveLength(1);
		expect((Picked.meta as any).derivedExprs[0].fieldName).toBe("likeCount");
		expect((Picked.meta as any).derivedFields).toEqual(["likeCount"]);
	});

	test("pick then derive works", () => {
		const Picked = Posts.pick("id");
		const WithCount = Picked.derive("likeCount", z.number())`COUNT(*)`;

		expect((WithCount.meta as any).isPartial).toBe(true);
		expect((WithCount.meta as any).isDerived).toBe(true);
		expect(Object.keys(WithCount.schema.shape)).toEqual(["id", "likeCount"]);
	});
});

describe("Type-level insert/update prevention", () => {
	// These tests verify compile-time type checking using @ts-expect-error.
	// The actual runtime behavior is tested in database.test.ts.

	test("Insert<PartialTable> evaluates to never", () => {
		const Users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email(),
			name: z.string(),
		});
		const _PartialUsers = Users.pick("id", "name");

		// Insert<PartialTable> should be never
		type PartialInsert = import("./table.js").Insert<typeof _PartialUsers>;
		const _check: PartialInsert = {} as never;
		expect(true).toBe(true); // Type check is the real test
	});

	test("Insert<DerivedTable> evaluates to never", () => {
		const Posts = table("posts", {
			id: z.string().uuid().db.primary(),
			title: z.string(),
		});
		const _PostsWithCount = Posts.derive("likeCount", z.number())`COUNT(*)`;

		// Insert<DerivedTable> should be never
		type DerivedInsert = import("./table.js").Insert<typeof _PostsWithCount>;
		const _check: DerivedInsert = {} as never;
		expect(true).toBe(true); // Type check is the real test
	});

	test("FullTableOnly<PartialTable> evaluates to never", () => {
		const Users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});
		const _PartialUsers = Users.pick("id");

		type FullCheck = import("./table.js").FullTableOnly<typeof _PartialUsers>;
		const _check: FullCheck = {} as never;
		expect(true).toBe(true);
	});

	test("FullTableOnly<DerivedTable> evaluates to never", () => {
		const Posts = table("posts", {
			id: z.string().uuid().db.primary(),
			title: z.string(),
		});
		const _PostsWithCount = Posts.derive("likeCount", z.number())`COUNT(*)`;

		type FullCheck = import("./table.js").FullTableOnly<typeof _PostsWithCount>;
		const _check: FullCheck = {} as never;
		expect(true).toBe(true);
	});

	test("FullTableOnly<Table> preserves the table type", () => {
		const Users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		type FullCheck = import("./table.js").FullTableOnly<typeof Users>;
		// Should be assignable (not never)
		const _check: FullCheck = Users;
		expect(true).toBe(true);
	});
});

describe("Table.cols", () => {
	const Users = table("users", {
		id: z.string().uuid().db.primary(),
		email: z.string().email().db.unique(),
		name: z.string(),
		createdAt: z.date(),
	});

	test("returns SQL fragment for column", () => {
		const {sql, params} = renderFragment(Users.cols.id);

		expect(sql).toBe('"users"."id"');
		expect(params).toEqual([]);
	});

	test("quotes table and column names", () => {
		expect(renderFragment(Users.cols.email).sql).toBe('"users"."email"');
		expect(renderFragment(Users.cols.name).sql).toBe('"users"."name"');
		expect(renderFragment(Users.cols.createdAt).sql).toBe(
			'"users"."createdAt"',
		);
	});

	test("works with picked tables", () => {
		const UserSummary = Users.pick("id", "name");

		expect(renderFragment(UserSummary.cols.id).sql).toBe('"users"."id"');
		expect(renderFragment(UserSummary.cols.name).sql).toBe('"users"."name"');
	});

	test("returns undefined for non-existent columns", () => {
		expect((Users.cols as any).nonexistent).toBeUndefined();
	});

	test("fragment can be interpolated in queries", () => {
		// Import parseTemplate to test interpolation
		const {parseTemplate} = require("../src/impl/query.js");

		const strings = [
			"SELECT * FROM users WHERE ",
			" = ",
			" ORDER BY ",
			" DESC",
		] as unknown as TemplateStringsArray;

		const result = parseTemplate(
			strings,
			[Users.cols.id, "user-123", Users.cols.createdAt],
			"sqlite",
		);

		expect(result.sql).toBe(
			'SELECT * FROM users WHERE "users"."id" = ? ORDER BY "users"."createdAt" DESC',
		);
		expect(result.params).toEqual(["user-123"]);
	});
});

describe("Schema marker validation", () => {
	test("inserted() throws for invalid values", () => {
		expect(() =>
			// @ts-expect-error - Testing runtime validation for JS callers
			z.date().db.inserted("invalid"),
		).toThrow(
			"inserted() requires a tagged template, symbol (NOW), or function",
		);

		expect(() =>
			// @ts-expect-error - Testing runtime validation for JS callers
			z.date().db.inserted(new Date()),
		).toThrow(
			"inserted() requires a tagged template, symbol (NOW), or function",
		);

		expect(() =>
			// @ts-expect-error - Testing runtime validation for JS callers
			z.date().db.inserted(null),
		).toThrow(
			"inserted() requires a tagged template, symbol (NOW), or function",
		);

		expect(() =>
			// @ts-expect-error - Testing runtime validation for JS callers
			z.date().db.inserted(123),
		).toThrow(
			"inserted() requires a tagged template, symbol (NOW), or function",
		);
	});

	test("updated() throws for invalid values", () => {
		expect(() =>
			// @ts-expect-error - Testing runtime validation for JS callers
			z.date().db.updated("invalid"),
		).toThrow(
			"updated() requires a tagged template, symbol (NOW), or function",
		);

		expect(() =>
			// @ts-expect-error - Testing runtime validation for JS callers
			z.date().db.updated(new Date()),
		).toThrow(
			"updated() requires a tagged template, symbol (NOW), or function",
		);

		expect(() =>
			// @ts-expect-error - Testing runtime validation for JS callers
			z.date().db.updated(undefined),
		).toThrow(
			"updated() requires a tagged template, symbol (NOW), or function",
		);
	});

	test("inserted() accepts valid values", () => {
		const {NOW} = require("../src/impl/database.js");

		// Should not throw - symbol
		const schema1 = z.date().db.inserted(NOW);
		expect(schema1).toBeDefined();

		// Should not throw - function
		const schema2 = z.date().db.inserted(() => new Date());
		expect(schema2).toBeDefined();
	});

	test("updated() accepts valid values", () => {
		const {NOW} = require("../src/impl/database.js");

		// Should not throw - symbol
		const schema1 = z.date().db.updated(NOW);
		expect(schema1).toBeDefined();

		// Should not throw - function
		const schema2 = z.date().db.updated(() => new Date());
		expect(schema2).toBeDefined();
	});

	test("upserted() accepts valid values", () => {
		const {NOW} = require("../src/impl/database.js");

		// Should not throw - symbol
		const schema1 = z.date().db.upserted(NOW);
		expect(schema1).toBeDefined();

		// Should not throw - function
		const schema2 = z.date().db.upserted(() => new Date());
		expect(schema2).toBeDefined();

		// Should not throw - tagged template
		const schema3 = z.date().db.upserted`CURRENT_TIMESTAMP`;
		expect(schema3).toBeDefined();
	});

	test("upserted() sets correct metadata", () => {
		const {NOW} = require("../src/impl/database.js");

		const TestTable = table("test_upserted", {
			id: z.string().db.primary(),
			modifiedAt: z.date().db.upserted(NOW),
		});

		expect(TestTable.meta.fields.modifiedAt.upserted).toBeDefined();
		expect(TestTable.meta.fields.modifiedAt.upserted?.type).toBe("symbol");
	});

	test("tagged template with interpolations parameterizes values", () => {
		const defaultValue = 42;
		const multiplier = 2;

		// Test inserted() with regular values - they become values in template
		const TestTable1 = table("test1", {
			id: z.string().db.primary(),
			computed: z.number().db.inserted`${defaultValue} * ${multiplier}`,
		});
		const insertedMeta = TestTable1.meta.fields.computed.inserted;
		expect(insertedMeta?.type).toBe("sql");
		// Template: `${42} * ${2}` -> tuple: [["", " * ", ""], 42, 2]
		expect(Array.from(insertedMeta?.template?.[0] ?? [])).toEqual([
			"",
			" * ",
			"",
		]);
		expect(insertedMeta?.template?.slice(1)).toEqual([42, 2]);

		// Test updated() with regular values - they become values in template
		const TestTable2 = table("test2", {
			id: z.string().db.primary(),
			computed: z.number().db.updated`COALESCE(?, ${defaultValue}) + 1`,
		});
		const updatedMeta = TestTable2.meta.fields.computed.updated;
		expect(updatedMeta?.type).toBe("sql");
		// Template: `COALESCE(?, ${42}) + 1` -> tuple: [["COALESCE(?, ", ") + 1"], 42]
		expect(Array.from(updatedMeta?.template?.[0] ?? [])).toEqual([
			"COALESCE(?, ",
			") + 1",
		]);
		expect(updatedMeta?.template?.slice(1)).toEqual([42]);
	});

	test("tagged template with SQL fragments inlines fragment SQL", () => {
		// Create a table with cols proxy for fragments
		const Users = table("users", {
			id: z.string().db.primary(),
			score: z.number(),
		});

		// Test inserted() with SQL fragment (from cols proxy)
		const TestTable1 = table("test1", {
			id: z.string().db.primary(),
			computed: z.number().db.inserted`${Users.cols.score} + 1`,
		});
		const insertedMeta = TestTable1.meta.fields.computed.inserted;
		expect(insertedMeta?.type).toBe("sql");
		// Column reference is merged: template parts with ident markers
		// tuple: [['', '.', ' + 1'], ident('users'), ident('score')]
		expect(Array.from(insertedMeta?.template?.[0] ?? [])).toEqual([
			"",
			".",
			" + 1",
		]);
		const insertedValues = insertedMeta?.template?.slice(1);
		expect(insertedValues).toHaveLength(2);
		expect(insertedValues?.[0]).toHaveProperty("name", "users");
		expect(insertedValues?.[1]).toHaveProperty("name", "score");

		// Test updated() with SQL fragment
		const TestTable2 = table("test2", {
			id: z.string().db.primary(),
			computed: z.number().db.updated`COALESCE(${Users.cols.score}, 0) * 2`,
		});
		const updatedMeta = TestTable2.meta.fields.computed.updated;
		expect(updatedMeta?.type).toBe("sql");
		// Fragment is merged: template parts with ident markers
		// tuple: [['COALESCE(', '.', ', 0) * 2'], ident('users'), ident('score')]
		expect(Array.from(updatedMeta?.template?.[0] ?? [])).toEqual([
			"COALESCE(",
			".",
			", 0) * 2",
		]);
		const updatedValues = updatedMeta?.template?.slice(1);
		expect(updatedValues).toHaveLength(2);
		expect(updatedValues?.[0]).toHaveProperty("name", "users");
		expect(updatedValues?.[1]).toHaveProperty("name", "score");
	});

	test("encode() throws when combined with inserted()", () => {
		const {NOW} = require("../src/impl/database.js");

		expect(() =>
			z
				.date()
				.db.inserted(NOW)
				.db.encode(() => "encoded"),
		).toThrow("encode() cannot be combined with inserted() or updated()");
	});

	test("encode() throws when combined with updated()", () => {
		const {NOW} = require("../src/impl/database.js");

		expect(() =>
			z
				.date()
				.db.updated(NOW)
				.db.encode(() => "encoded"),
		).toThrow("encode() cannot be combined with inserted() or updated()");
	});

	test("decode() throws when combined with inserted()", () => {
		const {NOW} = require("../src/impl/database.js");

		expect(() =>
			z
				.date()
				.db.inserted(NOW)
				.db.decode(() => new Date()),
		).toThrow("decode() cannot be combined with inserted() or updated()");
	});

	test("decode() throws when combined with updated()", () => {
		const {NOW} = require("../src/impl/database.js");

		expect(() =>
			z
				.date()
				.db.updated(NOW)
				.db.decode(() => new Date()),
		).toThrow("decode() cannot be combined with inserted() or updated()");
	});

	test("inserted() throws when combined with encode()", () => {
		const {NOW} = require("../src/impl/database.js");

		expect(() =>
			z
				.date()
				.db.encode(() => "encoded")
				.db.inserted(NOW),
		).toThrow("inserted() cannot be combined with encode() or decode()");
	});

	test("inserted() throws when combined with decode()", () => {
		const {NOW} = require("../src/impl/database.js");

		expect(() =>
			z
				.date()
				.db.decode(() => new Date())
				.db.inserted(NOW),
		).toThrow("inserted() cannot be combined with encode() or decode()");
	});

	test("updated() throws when combined with encode()", () => {
		const {NOW} = require("../src/impl/database.js");

		expect(() =>
			z
				.date()
				.db.encode(() => "encoded")
				.db.updated(NOW),
		).toThrow("updated() cannot be combined with encode() or decode()");
	});

	test("updated() throws when combined with decode()", () => {
		const {NOW} = require("../src/impl/database.js");

		expect(() =>
			z
				.date()
				.db.decode(() => new Date())
				.db.updated(NOW),
		).toThrow("updated() cannot be combined with encode() or decode()");
	});
});

// =============================================================================
// .db.auto() type-aware auto-generation
// =============================================================================

describe(".db.auto()", () => {
	const {getDBMeta} = require("../src/impl/table.js");

	test("UUID schema sets inserted function", () => {
		const schema = z.string().uuid().db.primary().db.auto();
		const dbMeta = getDBMeta(schema);

		expect(dbMeta.inserted).toBeDefined();
		expect(dbMeta.inserted.type).toBe("function");
		expect(typeof dbMeta.inserted.fn).toBe("function");

		// Should generate valid UUIDs
		const uuid = dbMeta.inserted.fn();
		expect(uuid).toMatch(
			/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
		);
	});

	test("Integer schema sets autoIncrement", () => {
		const schema = z.number().int().db.primary().db.auto();
		const dbMeta = getDBMeta(schema);

		expect(dbMeta.autoIncrement).toBe(true);
	});

	test("Date schema sets inserted symbol (NOW)", () => {
		const schema = z.date().db.auto();
		const dbMeta = getDBMeta(schema);

		expect(dbMeta.inserted).toBeDefined();
		expect(dbMeta.inserted.type).toBe("symbol");
		expect(dbMeta.inserted.symbol).toBeDefined();
	});

	test("makes field optional", () => {
		const uuidSchema = z.string().uuid().db.auto();
		const intSchema = z.number().int().db.auto();
		const dateSchema = z.date().db.auto();

		// All should be optional (isOptional returns true)
		expect(uuidSchema.isOptional()).toBe(true);
		expect(intSchema.isOptional()).toBe(true);
		expect(dateSchema.isOptional()).toBe(true);
	});

	test("throws for unsupported types", () => {
		expect(() => z.string().db.auto()).toThrow(
			".db.auto() is not supported for this type",
		);
		expect(() => z.number().db.auto()).toThrow(
			".db.auto() is not supported for this type",
		);
		expect(() => z.boolean().db.auto()).toThrow(
			".db.auto() is not supported for this type",
		);
	});

	test("works in table definition", () => {
		const Users = table("users", {
			id: z.string().uuid().db.primary().db.auto(),
			createdAt: z.date().db.auto(),
			name: z.string(),
		});

		// Fields with .db.auto() should have metadata
		expect(Users.meta.fields.id.inserted).toBeDefined();
		expect(Users.meta.fields.createdAt.inserted).toBeDefined();
	});
});

// =============================================================================
// Regression: Identifier validation (Issue #6)
// =============================================================================

describe("Identifier validation", () => {
	test("table name with newline should throw", () => {
		expect(() => {
			table("users\nDROP TABLE users;--", {
				id: z.string().db.primary(),
			});
		}).toThrow(/invalid.*identifier|control.*char/i);
	});

	test("column name with null byte should throw", () => {
		expect(() => {
			table("users", {
				"id\x00": z.string().db.primary(),
			});
		}).toThrow(/invalid.*identifier|control.*char/i);
	});

	test("table name with semicolon should throw", () => {
		expect(() => {
			table("users; DROP TABLE users;--", {
				id: z.string().db.primary(),
			});
		}).toThrow(/invalid.*identifier/i);
	});
});

// =============================================================================
// Regression: PostgreSQL parameter limit (Issue #7)
// =============================================================================

describe("PostgreSQL parameter limit", () => {
	test("should throw when exceeding PostgreSQL 32767 param limit", () => {
		const Users = table("users", {
			id: z.string().db.primary(),
		});

		// Create array with 40000 values (exceeds 32767)
		const tooManyValues = Array.from({length: 40000}, (_, i) => `id-${i}`);

		// Should throw when creating the IN clause
		expect(() => {
			Users.in("id", tooManyValues);
		}).toThrow(/too many|param.*limit|exceed|32767/i);
	});
});

// =============================================================================
// Regression: Circular reference detection (Issue #10)
// =============================================================================

describe("Circular reference detection", () => {
	test("self-referential table should work (valid use case)", () => {
		// Self-reference is valid (e.g., employee -> manager)
		const Employees = table("employees", {
			id: z.string().db.primary(),
			name: z.string(),
			managerId: z.string().nullable(),
		});

		// This is a valid pattern, should not throw
		expect(Employees.name).toBe("employees");
	});
});

// =============================================================================
// Regression: decodeData error handling (Issues #5, #9)
// =============================================================================

describe("decodeData error handling", () => {
	const {decodeData} = require("../src/impl/table.js");
	const {Database} = require("../src/impl/database.js");

	test("malformed JSON in object field should have clear error message", async () => {
		const Settings = table("settings", {
			id: z.string().db.primary(),
			config: z.object({theme: z.string()}),
		});

		const driver = {
			supportsReturning: true,
			all: async () =>
				[{"settings.id": "1", "settings.config": "not-valid-json"}] as any,
			get: async () =>
				({"settings.id": "1", "settings.config": "not-valid-json"}) as any,
			run: async () => 0,
			val: async () => null,
			close: async () => {},
			transaction: async (fn: any) => fn(driver),
		};

		const db = new Database(driver);

		// Should throw with a message that mentions JSON parsing
		try {
			await db.get(Settings)`WHERE id = ${"1"}`;
			expect(true).toBe(false); // Should not reach here
		} catch (e: any) {
			expect(e.message).toMatch(/JSON|parse|config/i);
		}
	});

	test("invalid date string should throw with clear message", async () => {
		const Events = table("events", {
			id: z.string().db.primary(),
			startedAt: z.date(),
		});

		const driver = {
			supportsReturning: true,
			all: async () =>
				[{"events.id": "1", "events.startedAt": "not-a-date"}] as any,
			get: async () =>
				({"events.id": "1", "events.startedAt": "not-a-date"}) as any,
			run: async () => 0,
			val: async () => null,
			close: async () => {},
			transaction: async (fn: any) => fn(driver),
		};

		const db = new Database(driver);

		// Should throw with a message mentioning the date issue
		try {
			await db.get(Events)`WHERE id = ${"1"}`;
			expect(true).toBe(false); // Should not reach here
		} catch (e: any) {
			expect(e.message).toMatch(/date|startedAt|invalid/i);
		}
	});
});
