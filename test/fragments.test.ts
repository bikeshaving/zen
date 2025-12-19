import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, extendZod} from "../src/impl/table.js";
import {parseTemplate, renderFragment} from "../src/impl/query.js";

// Extend Zod once before tests
extendZod(z);

// Test tables (Uppercase plural convention)
const Users = table("users", {
	id: z.string().uuid().db.primary(),
	email: z.string().email(),
	name: z.string(),
	role: z.enum(["user", "admin"]),
	createdAt: z.date(),
});

const Posts = table("posts", {
	id: z.string().uuid().db.primary(),
	authorId: z.string().uuid().db.references(Users, {as: "author"}),
	title: z.string(),
	published: z.boolean(),
	viewCount: z.number().int(),
});

describe("Table.set()", () => {
	test("single field with quoted name", () => {
		const {sql, params} = renderFragment(Posts.set({title: "New Title"}));
		expect(sql).toBe('"title" = ?');
		expect(params).toEqual(["New Title"]);
	});

	test("multiple fields", () => {
		const {sql, params} = renderFragment(
			Posts.set({title: "New Title", published: true}),
		);
		expect(sql).toBe('"title" = ?, "published" = ?');
		expect(params).toEqual(["New Title", true]);
	});

	test("quotes field names", () => {
		const {sql, params} = renderFragment(Posts.set({viewCount: 42}));
		expect(sql).toBe('"viewCount" = ?');
		expect(params).toEqual([42]);
	});

	test("skips undefined values", () => {
		const {sql, params} = renderFragment(
			Posts.set({title: "New", published: undefined}),
		);
		expect(sql).toBe('"title" = ?');
		expect(params).toEqual(["New"]);
	});

	test("throws on empty object", () => {
		expect(() => Posts.set({})).toThrow(
			"set() requires at least one non-undefined field",
		);
	});

	test("throws when all values undefined", () => {
		expect(() => Posts.set({title: undefined})).toThrow(
			"set() requires at least one non-undefined field",
		);
	});
});

describe("Table.on()", () => {
	test("generates FK equality with qualified names", () => {
		const {sql, params} = renderFragment(Posts.on("authorId"));
		expect(sql).toBe('"users"."id" = "posts"."authorId"');
		expect(params).toEqual([]);
	});

	test("throws for non-FK field", () => {
		expect(() => Posts.on("title")).toThrow(
			'Field "title" is not a foreign key reference in table "posts"',
		);
	});
});

describe("Table.values()", () => {
	const uuid1 = "550e8400-e29b-41d4-a716-446655440001";
	const uuid2 = "550e8400-e29b-41d4-a716-446655440002";
	const uuid3 = "550e8400-e29b-41d4-a716-446655440003";

	test("single row with inferred columns", () => {
		const {sql, params} = renderFragment(
			Posts.values([{id: uuid1, title: "Hello"}]),
		);
		expect(sql).toBe('("id", "title") VALUES (?, ?)');
		expect(params).toEqual([uuid1, "Hello"]);
	});

	test("multiple rows", () => {
		const rows = [
			{id: uuid1, title: "First"},
			{id: uuid2, title: "Second"},
			{id: uuid3, title: "Third"},
		];
		const {sql, params} = renderFragment(Posts.values(rows));
		expect(sql).toBe('("id", "title") VALUES (?, ?), (?, ?), (?, ?)');
		expect(params).toEqual([uuid1, "First", uuid2, "Second", uuid3, "Third"]);
	});

	test("columns inferred from first row keys", () => {
		const {sql, params} = renderFragment(
			Posts.values([{title: "Hello", id: uuid1}]),
		);
		expect(sql).toBe('("title", "id") VALUES (?, ?)');
		// Order based on Object.keys() of first row
		expect(params).toEqual(["Hello", uuid1]);
	});

	test("validates rows against schema", () => {
		// viewCount must be an integer
		expect(() =>
			Posts.values([{id: uuid1, viewCount: "not a number"}] as any),
		).toThrow();
	});

	test("throws on empty rows", () => {
		expect(() => Posts.values([])).toThrow(
			"values() requires at least one row",
		);
	});

	test("throws on empty object", () => {
		expect(() => Posts.values([{}])).toThrow(
			"values() requires at least one column",
		);
	});

	test("throws if rows have mismatched columns", () => {
		expect(() =>
			Posts.values([
				{id: uuid1, title: "First"},
				{id: uuid2}, // Missing title
			]),
		).toThrow("All rows must have the same columns");
	});

	test("works in INSERT template", () => {
		const rows = [
			{id: uuid1, title: "First", published: true},
			{id: uuid2, title: "Second", published: false},
		];
		const strings = [
			"INSERT INTO posts ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[Posts.values(rows)],
			"sqlite",
		);

		expect(sql).toBe(
			'INSERT INTO posts ("id", "title", "published") VALUES (?, ?, ?), (?, ?, ?)',
		);
		expect(params).toEqual([uuid1, "First", true, uuid2, "Second", false]);
	});

	test("postgresql placeholders", () => {
		const rows = [
			{id: uuid1, title: "First"},
			{id: uuid2, title: "Second"},
		];
		const strings = [
			"INSERT INTO posts ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[Posts.values(rows)],
			"postgresql",
		);

		expect(sql).toBe(
			'INSERT INTO posts ("id", "title") VALUES ($1, $2), ($3, $4)',
		);
		expect(params).toEqual([uuid1, "First", uuid2, "Second"]);
	});
});

describe("Table.in()", () => {
	const uuid1 = "550e8400-e29b-41d4-a716-446655440001";
	const uuid2 = "550e8400-e29b-41d4-a716-446655440002";
	const uuid3 = "550e8400-e29b-41d4-a716-446655440003";

	test("generates IN clause with single value", () => {
		const {sql, params} = renderFragment(Posts.in("id", [uuid1]));
		expect(sql).toBe('"posts"."id" IN (?)');
		expect(params).toEqual([uuid1]);
	});

	test("generates IN clause with multiple values", () => {
		const {sql, params} = renderFragment(Posts.in("id", [uuid1, uuid2, uuid3]));
		expect(sql).toBe('"posts"."id" IN (?, ?, ?)');
		expect(params).toEqual([uuid1, uuid2, uuid3]);
	});

	test("handles empty array with always-false condition", () => {
		const {sql, params} = renderFragment(Posts.in("id", []));
		expect(sql).toBe("1 = 0");
		expect(params).toEqual([]);
	});

	test("works with different field types", () => {
		const {sql, params} = renderFragment(
			Posts.in("title", ["First", "Second"]),
		);
		expect(sql).toBe('"posts"."title" IN (?, ?)');
		expect(params).toEqual(["First", "Second"]);
	});

	test("throws on invalid field", () => {
		expect(() => Posts.in("nonexistent" as any, [uuid1])).toThrow(
			'Field "nonexistent" does not exist in table "posts"',
		);
	});

	test("works in WHERE template", () => {
		const ids = [uuid1, uuid2];
		const strings = [
			"WHERE ",
			" AND published = ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[Posts.in("id", ids), true],
			"sqlite",
		);

		expect(sql).toBe('WHERE "posts"."id" IN (?, ?) AND published = ?');
		expect(params).toEqual([uuid1, uuid2, true]);
	});

	test("postgresql placeholders", () => {
		const ids = [uuid1, uuid2];
		const strings = ["", ""] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[Posts.in("id", ids)],
			"postgresql",
		);

		expect(sql).toBe('"posts"."id" IN ($1, $2)');
		expect(params).toEqual([uuid1, uuid2]);
	});
});

describe("fragment interpolation in parseTemplate", () => {
	const uuid1 = "550e8400-e29b-41d4-a716-446655440001";
	const uuid2 = "550e8400-e29b-41d4-a716-446655440002";

	test("in() fragment in template", () => {
		const fragment = Posts.in("id", [uuid1]);
		const strings = ["WHERE ", ""] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(strings, [fragment], "sqlite");

		expect(sql).toBe('WHERE "posts"."id" IN (?)');
		expect(params).toEqual([uuid1]);
	});

	test("multiple fragments in template", () => {
		const inFragment = Posts.in("id", [uuid1, uuid2]);
		const setFragment = Posts.set({title: "Updated"});
		const strings = [
			"UPDATE posts SET ",
			" WHERE ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[setFragment, inFragment],
			"sqlite",
		);

		expect(sql).toBe(
			'UPDATE posts SET "title" = ? WHERE "posts"."id" IN (?, ?)',
		);
		expect(params).toEqual(["Updated", uuid1, uuid2]);
	});

	test("fragment with regular values", () => {
		const fragment = Posts.in("id", [uuid1]);
		const strings = [
			"SELECT * FROM posts WHERE ",
			" AND published = ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(strings, [fragment, true], "sqlite");

		expect(sql).toBe(
			'SELECT * FROM posts WHERE "posts"."id" IN (?) AND published = ?',
		);
		expect(params).toEqual([uuid1, true]);
	});

	test("postgresql placeholders", () => {
		const fragment = Posts.in("id", [uuid1, uuid2]);
		const strings = [
			"SELECT * FROM posts WHERE ",
			" AND published = ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[fragment, true],
			"postgresql",
		);

		expect(sql).toBe(
			'SELECT * FROM posts WHERE "posts"."id" IN ($1, $2) AND published = $3',
		);
		expect(params).toEqual([uuid1, uuid2, true]);
	});
});
