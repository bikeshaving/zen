import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, isTable, extendZod} from "./table.js";

// Extend Zod once before tests
extendZod(z);

describe("table", () => {
	test("basic table definition", () => {
		const users = table("users", {
			id: z.string().uuid(),
			name: z.string()});

		expect(users.name).toBe("users");
	});

	test("extracts field metadata", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email().db.unique(),
			name: z.string().max(100),
			bio: z.string().max(2000),
			age: z.number().int().min(0).max(150),
			role: z.enum(["user", "admin", "moderator"]).default("user"),
			active: z.boolean().default(true),
			createdAt: z.date().default(() => new Date())});

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
		expect(fields.role.default).toBe("user");
		expect(fields.role.required).toBe(false); // has default

		// Boolean
		expect(fields.active.type).toBe("checkbox");
		expect(fields.active.default).toBe(true);

		// Date
		expect(fields.createdAt.type).toBe("datetime");
		expect(fields.createdAt.required).toBe(false); // has default
	});

	test("detects primary key", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email()});

		expect(users._meta.primary).toBe("id");
		expect(users.primary).not.toBeNull();
		expect(users.primary!.sql).toBe('"users"."id"');
		expect(users.primary!.params).toEqual([]);
	});

	test("handles optional and nullable fields", () => {
		const profiles = table("profiles", {
			id: z.string().uuid().db.primary(),
			bio: z.string().optional(),
			avatar: z.string().url().nullable(),
			nickname: z.string().nullish()});

		const fields = profiles.fields();

		expect(fields.bio.required).toBe(false);
		expect(fields.avatar.required).toBe(false);
		expect(fields.avatar.type).toBe("url");
		expect(fields.nickname.required).toBe(false);
	});

	test("url detection", () => {
		const links = table("links", {
			id: z.string().uuid().db.primary(),
			url: z.string().url()});

		const fields = links.fields();
		expect(fields.url.type).toBe("url");
	});

	test("indexed fields", () => {
		const posts = table("posts", {
			id: z.string().uuid().db.primary(),
			authorId: z.string().uuid().db.index(),
			title: z.string()});

		const fields = posts.fields();
		expect(fields.authorId.indexed).toBe(true);
	});

	test("compound indexes via options", () => {
		const posts = table(
			"posts",
			{
				id: z.string().uuid().db.primary(),
				authorId: z.string().uuid(),
				createdAt: z.date()},
			{
				indexes: [["authorId", "createdAt"]]},
		);

		expect(posts.indexes).toEqual([["authorId", "createdAt"]]);
	});

	test("compound unique constraints via options", () => {
		const posts = table(
			"posts",
			{
				id: z.string().uuid().db.primary(),
				authorId: z.string().uuid(),
				slug: z.string()},
			{
				unique: [["authorId", "slug"]]},
		);

		expect(posts.unique).toEqual([["authorId", "slug"]]);
	});

	test("compound foreign keys via options", () => {
		const Orders = table("orders", {
			id: z.string().uuid().db.primary(),
			customerId: z.string().uuid()});

		const Products = table("products", {
			id: z.string().uuid().db.primary(),
			name: z.string()});

		const OrderProducts = table("order_products", {
			orderId: z.string().uuid(),
			productId: z.string().uuid(),
			quantity: z.number()});

		const OrderItems = table(
			"order_items",
			{
				id: z.string().uuid().db.primary(),
				orderId: z.string().uuid(),
				productId: z.string().uuid(),
				price: z.number()},
			{
				references: [{
					fields: ["orderId", "productId"],
					table: OrderProducts,
					as: "orderProduct",
				}]},
		);

		expect(OrderItems.compoundReferences).toHaveLength(1);
		expect(OrderItems.compoundReferences[0].fields).toEqual(["orderId", "productId"]);
		expect(OrderItems.compoundReferences[0].table).toBe(OrderProducts);
		expect(OrderItems.compoundReferences[0].as).toBe("orderProduct");
	});

	test("extracts Zod 4 .meta() for UI metadata", () => {
		const users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email().meta({
					label: "Email Address",
					helpText: "We will never share your email",
					placeholder: "you@example.com"}).db.unique(),
			role: z
				.enum(["user", "admin"])
				.default("user")
				.meta({label: "User Role", widget: "radio"}),
			bio: z.string().optional().meta({label: "Biography", widget: "textarea"})});

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
				id: z.string().uuid()}),
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
			age: z.number().optional()});

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
			name: z.string()});

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
		bio: z.string().optional()});

	const Posts = table("posts", {
		id: z.string().uuid().db.primary(),
		authorId: z.string().uuid().db.references(Users, {as: "author"}),
		title: z.string(),
		body: z.string(),
		published: z.boolean().default(false)});

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
		expect(WithPK._meta.primary).toBe("id");
		expect(WithPK.primary).not.toBeNull();

		const WithoutPK = Users.pick("name", "email");
		expect(WithoutPK._meta.primary).toBeNull();
		expect(WithoutPK.primary).toBeNull();
	});

	test("preserves unique fields if picked", () => {
		const WithUnique = Users.pick("id", "email");
		expect(WithUnique._meta.unique).toEqual(["email"]);

		const WithoutUnique = Users.pick("id", "name");
		expect(WithoutUnique._meta.unique).toEqual([]);
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
			email: "alice@example.com"});
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
		expect(Step2._meta.primary).toBe("id");
	});

	test("pick() preserves compound unique constraints if all fields picked", () => {
		const Articles = table(
			"articles",
			{
				id: z.string().uuid().db.primary(),
				authorId: z.string().uuid(),
				slug: z.string(),
				title: z.string()},
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
			b: z.string()});

		const WithRefs = table(
			"with_refs",
			{
				id: z.string().db.primary(),
				refA: z.string(),
				refB: z.string(),
				other: z.string()},
			{references: [{fields: ["refA", "refB"], table: RefTable, as: "ref"}]},
		);

		const Picked = WithRefs.pick("id", "refA", "refB");
		expect(Picked.compoundReferences).toHaveLength(1);

		const PickedPartial = WithRefs.pick("id", "refA", "other");
		expect(PickedPartial.compoundReferences).toHaveLength(0);
	});
});

describe("Table.cols", () => {
	const Users = table("users", {
		id: z.string().uuid().db.primary(),
		email: z.string().email().db.unique(),
		name: z.string(),
		createdAt: z.date()});

	test("returns SQL fragment for column", () => {
		const fragment = Users.cols.id;

		expect(fragment.sql).toBe('"users"."id"');
		expect(fragment.params).toEqual([]);
	});

	test("quotes table and column names", () => {
		expect(Users.cols.email.sql).toBe('"users"."email"');
		expect(Users.cols.name.sql).toBe('"users"."name"');
		expect(Users.cols.createdAt.sql).toBe('"users"."createdAt"');
	});

	test("works with picked tables", () => {
		const UserSummary = Users.pick("id", "name");

		expect(UserSummary.cols.id.sql).toBe('"users"."id"');
		expect(UserSummary.cols.name.sql).toBe('"users"."name"');
	});

	test("returns undefined for non-existent columns", () => {
		expect((Users.cols as any).nonexistent).toBeUndefined();
	});

	test("fragment can be interpolated in queries", () => {
		// Import parseTemplate to test interpolation
		const {parseTemplate} = require("./query.js");

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
	test("inserted() throws for non-DBExpression values", () => {
		expect(() =>
			z.date().db.inserted("invalid"),
		).toThrow("inserted() requires a DB expression");

		expect(() =>
			z.date().db.inserted(new Date()),
		).toThrow("inserted() requires a DB expression");

		expect(() =>
			z.date().db.inserted(null),
		).toThrow("inserted() requires a DB expression");

		expect(() =>
			z.date().db.inserted(123),
		).toThrow("inserted() requires a DB expression");
	});

	test("updated() throws for non-DBExpression values", () => {
		expect(() =>
			z.date().db.updated("invalid"),
		).toThrow("updated() requires a DB expression");

		expect(() =>
			z.date().db.updated(new Date()),
		).toThrow("updated() requires a DB expression");

		expect(() =>
			z.date().db.updated(undefined),
		).toThrow("updated() requires a DB expression");
	});

	test("inserted() accepts valid DBExpression", () => {
		const {db} = require("./database.js");

		// Should not throw
		const schema = z.date().db.inserted(db.now());
		expect(schema).toBeDefined();
	});

	test("updated() accepts valid DBExpression", () => {
		const {db} = require("./database.js");

		// Should not throw
		const schema = z.date().db.updated(db.now());
		expect(schema).toBeDefined();
	});

	test("encode() throws when combined with inserted()", () => {
		const {db} = require("./database.js");

		expect(() =>
			z.date().db.inserted(db.now()).db.encode(() => "encoded"),
		).toThrow("encode() cannot be combined with inserted() or updated()");
	});

	test("encode() throws when combined with updated()", () => {
		const {db} = require("./database.js");

		expect(() =>
			z.date().db.updated(db.now()).db.encode(() => "encoded"),
		).toThrow("encode() cannot be combined with inserted() or updated()");
	});

	test("decode() throws when combined with inserted()", () => {
		const {db} = require("./database.js");

		expect(() =>
			z.date().db.inserted(db.now()).db.decode(() => new Date()),
		).toThrow("decode() cannot be combined with inserted() or updated()");
	});

	test("decode() throws when combined with updated()", () => {
		const {db} = require("./database.js");

		expect(() =>
			z.date().db.updated(db.now()).db.decode(() => new Date()),
		).toThrow("decode() cannot be combined with inserted() or updated()");
	});

	test("inserted() throws when combined with encode()", () => {
		const {db} = require("./database.js");

		expect(() =>
			z.date().db.encode(() => "encoded").db.inserted(db.now()),
		).toThrow("inserted() cannot be combined with encode() or decode()");
	});

	test("inserted() throws when combined with decode()", () => {
		const {db} = require("./database.js");

		expect(() =>
			z.date().db.decode(() => new Date()).db.inserted(db.now()),
		).toThrow("inserted() cannot be combined with encode() or decode()");
	});

	test("updated() throws when combined with encode()", () => {
		const {db} = require("./database.js");

		expect(() =>
			z.date().db.encode(() => "encoded").db.updated(db.now()),
		).toThrow("updated() cannot be combined with encode() or decode()");
	});

	test("updated() throws when combined with decode()", () => {
		const {db} = require("./database.js");

		expect(() =>
			z.date().db.decode(() => new Date()).db.updated(db.now()),
		).toThrow("updated() cannot be combined with encode() or decode()");
	});
});
