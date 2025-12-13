import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, primary, unique, index, references, isTable} from "./table.js";

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
			id: primary(z.string().uuid()),
			email: unique(z.string().email()),
			name: z.string().max(100),
			bio: z.string().max(2000),
			age: z.number().int().min(0).max(150),
			role: z.enum(["user", "admin", "moderator"]).default("user"),
			active: z.boolean().default(true),
			createdAt: z.date().default(() => new Date()),
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
			id: primary(z.string().uuid()),
			email: z.string().email(),
		});

		expect(users.primaryKey()).toBe("id");
	});

	test("handles optional and nullable fields", () => {
		const profiles = table("profiles", {
			id: primary(z.string().uuid()),
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
			id: primary(z.string().uuid()),
			url: z.string().url(),
		});

		const fields = links.fields();
		expect(fields.url.type).toBe("url");
	});

	test("indexed fields", () => {
		const posts = table("posts", {
			id: primary(z.string().uuid()),
			authorId: index(z.string().uuid()),
			title: z.string(),
		});

		const fields = posts.fields();
		expect(fields.authorId.indexed).toBe(true);
	});

	test("compound indexes via options", () => {
		const posts = table(
			"posts",
			{
				id: primary(z.string().uuid()),
				authorId: z.string().uuid(),
				createdAt: z.date(),
			},
			{
				indexes: [["authorId", "createdAt"]],
			},
		);

		expect(posts.indexes).toEqual([["authorId", "createdAt"]]);
	});

	test("extracts Zod 4 .meta() for UI metadata", () => {
		const users = table("users", {
			id: primary(z.string().uuid()),
			email: unique(
				z.string().email().meta({
					label: "Email Address",
					helpText: "We will never share your email",
					placeholder: "you@example.com",
				}),
			),
			role: z
				.enum(["user", "admin"])
				.default("user")
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
			id: primary(z.string().uuid()),
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
		id: primary(z.string().uuid()),
		email: unique(z.string().email()),
		name: z.string(),
		bio: z.string().optional(),
	});

	const Posts = table("posts", {
		id: primary(z.string().uuid()),
		authorId: references(z.string().uuid(), Users, {as: "author"}),
		title: z.string(),
		body: z.string(),
		published: z.boolean().default(false),
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
		expect(WithPK.primaryKey()).toBe("id");

		const WithoutPK = Users.pick("name", "email");
		expect(WithoutPK.primaryKey()).toBeNull();
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
		expect(Step2.primaryKey()).toBe("id");
	});
});

describe("Table.cols", () => {
	const Users = table("users", {
		id: primary(z.string().uuid()),
		email: unique(z.string().email()),
		name: z.string(),
		createdAt: z.date(),
	});

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
