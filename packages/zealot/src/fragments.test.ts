import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, primary, references} from "./table.js";
import {parseTemplate} from "./query.js";

// Test tables (Uppercase plural convention)
const Users = table("users", {
	id: primary(z.string().uuid()),
	email: z.string().email(),
	name: z.string(),
	role: z.enum(["user", "admin"]).default("user"),
	createdAt: z.date(),
});

const Posts = table("posts", {
	id: primary(z.string().uuid()),
	authorId: references(z.string().uuid(), Users, {as: "author"}),
	title: z.string(),
	published: z.boolean().default(false),
	viewCount: z.number().int().default(0),
});

describe("Table.where()", () => {
	test("simple equality with qualified column", () => {
		const fragment = Posts.where({published: true});
		expect(fragment.sql).toBe('"posts"."published" = ?');
		expect(fragment.params).toEqual([true]);
	});

	test("multiple conditions (AND-joined)", () => {
		const fragment = Posts.where({published: true, title: "Hello"});
		expect(fragment.sql).toBe('"posts"."published" = ? AND "posts"."title" = ?');
		expect(fragment.params).toEqual([true, "Hello"]);
	});

	test("qualifies field names with table", () => {
		const fragment = Posts.where({viewCount: 100});
		expect(fragment.sql).toBe('"posts"."viewCount" = ?');
		expect(fragment.params).toEqual([100]);
	});

	test("$eq operator", () => {
		const fragment = Posts.where({published: {$eq: true}});
		expect(fragment.sql).toBe('"posts"."published" = ?');
		expect(fragment.params).toEqual([true]);
	});

	test("$neq operator", () => {
		const fragment = Users.where({role: {$neq: "admin"}});
		expect(fragment.sql).toBe('"users"."role" != ?');
		expect(fragment.params).toEqual(["admin"]);
	});

	test("$lt operator", () => {
		const fragment = Posts.where({viewCount: {$lt: 100}});
		expect(fragment.sql).toBe('"posts"."viewCount" < ?');
		expect(fragment.params).toEqual([100]);
	});

	test("$gt operator", () => {
		const fragment = Posts.where({viewCount: {$gt: 50}});
		expect(fragment.sql).toBe('"posts"."viewCount" > ?');
		expect(fragment.params).toEqual([50]);
	});

	test("$lte operator", () => {
		const fragment = Posts.where({viewCount: {$lte: 100}});
		expect(fragment.sql).toBe('"posts"."viewCount" <= ?');
		expect(fragment.params).toEqual([100]);
	});

	test("$gte operator", () => {
		const fragment = Posts.where({viewCount: {$gte: 50}});
		expect(fragment.sql).toBe('"posts"."viewCount" >= ?');
		expect(fragment.params).toEqual([50]);
	});

	test("$like operator", () => {
		const fragment = Posts.where({title: {$like: "%hello%"}});
		expect(fragment.sql).toBe('"posts"."title" LIKE ?');
		expect(fragment.params).toEqual(["%hello%"]);
	});

	test("$in operator", () => {
		const fragment = Users.where({role: {$in: ["user", "admin"]}});
		expect(fragment.sql).toBe('"users"."role" IN (?, ?)');
		expect(fragment.params).toEqual(["user", "admin"]);
	});

	test("$isNull operator (true)", () => {
		const fragment = Posts.where({title: {$isNull: true}});
		expect(fragment.sql).toBe('"posts"."title" IS NULL');
		expect(fragment.params).toEqual([]);
	});

	test("$isNull operator (false)", () => {
		const fragment = Posts.where({title: {$isNull: false}});
		expect(fragment.sql).toBe('"posts"."title" IS NOT NULL');
		expect(fragment.params).toEqual([]);
	});

	test("multiple operators on same field", () => {
		const fragment = Posts.where({viewCount: {$gte: 10, $lte: 100}});
		expect(fragment.sql).toBe('"posts"."viewCount" >= ? AND "posts"."viewCount" <= ?');
		expect(fragment.params).toEqual([10, 100]);
	});

	test("empty conditions returns 1 = 1", () => {
		const fragment = Posts.where({});
		expect(fragment.sql).toBe("1 = 1");
		expect(fragment.params).toEqual([]);
	});

	test("skips undefined values", () => {
		const fragment = Posts.where({published: true, title: undefined});
		expect(fragment.sql).toBe('"posts"."published" = ?');
		expect(fragment.params).toEqual([true]);
	});
});

describe("Table.set()", () => {
	test("single field with quoted name", () => {
		const fragment = Posts.set({title: "New Title"});
		expect(fragment.sql).toBe('"title" = ?');
		expect(fragment.params).toEqual(["New Title"]);
	});

	test("multiple fields", () => {
		const fragment = Posts.set({title: "New Title", published: true});
		expect(fragment.sql).toBe('"title" = ?, "published" = ?');
		expect(fragment.params).toEqual(["New Title", true]);
	});

	test("quotes field names", () => {
		const fragment = Posts.set({viewCount: 42});
		expect(fragment.sql).toBe('"viewCount" = ?');
		expect(fragment.params).toEqual([42]);
	});

	test("skips undefined values", () => {
		const fragment = Posts.set({title: "New", published: undefined});
		expect(fragment.sql).toBe('"title" = ?');
		expect(fragment.params).toEqual(["New"]);
	});

	test("throws on empty object", () => {
		expect(() => Posts.set({})).toThrow("set() requires at least one field");
	});

	test("throws when all values undefined", () => {
		expect(() => Posts.set({title: undefined})).toThrow(
			"set() requires at least one non-undefined field",
		);
	});
});

describe("Table.on()", () => {
	test("generates FK equality with qualified names", () => {
		const fragment = Posts.on("authorId");
		expect(fragment.sql).toBe('"users"."id" = "posts"."authorId"');
		expect(fragment.params).toEqual([]);
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

	test("single row with specified columns", () => {
		const fragment = Posts.values([{id: uuid1, title: "Hello"}], ["id", "title"]);
		expect(fragment.sql).toBe("(?, ?)");
		expect(fragment.params).toEqual([uuid1, "Hello"]);
	});

	test("multiple rows", () => {
		const rows = [
			{id: uuid1, title: "First"},
			{id: uuid2, title: "Second"},
			{id: uuid3, title: "Third"},
		];
		const fragment = Posts.values(rows, ["id", "title"]);
		expect(fragment.sql).toBe("(?, ?), (?, ?), (?, ?)");
		expect(fragment.params).toEqual([uuid1, "First", uuid2, "Second", uuid3, "Third"]);
	});

	test("respects column order", () => {
		const fragment = Posts.values([{id: uuid1, title: "Hello"}], ["title", "id"]);
		expect(fragment.sql).toBe("(?, ?)");
		expect(fragment.params).toEqual(["Hello", uuid1]);
	});

	test("validates rows against schema", () => {
		// viewCount must be an integer
		expect(() =>
			Posts.values([{id: uuid1, viewCount: "not a number"}] as any, ["id", "viewCount"]),
		).toThrow();
	});

	test("throws on empty rows", () => {
		expect(() => Posts.values([], ["id", "title"])).toThrow(
			"values() requires at least one row",
		);
	});

	test("throws on empty columns", () => {
		expect(() => Posts.values([{id: uuid1}], [])).toThrow(
			"values() requires at least one column",
		);
	});

	test("works in INSERT template", () => {
		const rows = [
			{id: uuid1, title: "First", published: true},
			{id: uuid2, title: "Second", published: false},
		];
		const strings = [
			"INSERT INTO posts (id, title, published) VALUES ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[Posts.values(rows, ["id", "title", "published"])],
			"sqlite",
		);

		expect(sql).toBe("INSERT INTO posts (id, title, published) VALUES (?, ?, ?), (?, ?, ?)");
		expect(params).toEqual([uuid1, "First", true, uuid2, "Second", false]);
	});

	test("postgresql placeholders", () => {
		const rows = [
			{id: uuid1, title: "First"},
			{id: uuid2, title: "Second"},
		];
		const strings = [
			"INSERT INTO posts (id, title) VALUES ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[Posts.values(rows, ["id", "title"])],
			"postgresql",
		);

		expect(sql).toBe("INSERT INTO posts (id, title) VALUES ($1, $2), ($3, $4)");
		expect(params).toEqual([uuid1, "First", uuid2, "Second"]);
	});
});

describe("fragment interpolation in parseTemplate", () => {
	test("where fragment in template", () => {
		const fragment = Posts.where({published: true});
		const strings = ["WHERE ", ""] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(strings, [fragment], "sqlite");

		expect(sql).toBe('WHERE "posts"."published" = ?');
		expect(params).toEqual([true]);
	});

	test("multiple fragments in template", () => {
		const whereFragment = Posts.where({published: true});
		const setFragment = Posts.set({title: "Updated"});
		const strings = [
			"UPDATE posts SET ",
			" WHERE ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[setFragment, whereFragment],
			"sqlite",
		);

		expect(sql).toBe('UPDATE posts SET "title" = ? WHERE "posts"."published" = ?');
		expect(params).toEqual(["Updated", true]);
	});

	test("fragment with regular values", () => {
		const fragment = Posts.where({published: true});
		const strings = [
			"SELECT * FROM posts WHERE ",
			" AND id = ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[fragment, "post-123"],
			"sqlite",
		);

		expect(sql).toBe('SELECT * FROM posts WHERE "posts"."published" = ? AND id = ?');
		expect(params).toEqual([true, "post-123"]);
	});

	test("postgresql placeholders", () => {
		const fragment = Posts.where({published: true, title: "Hello"});
		const strings = [
			"SELECT * FROM posts WHERE ",
			" AND id = ",
			"",
		] as unknown as TemplateStringsArray;
		const {sql, params} = parseTemplate(
			strings,
			[fragment, "post-123"],
			"postgresql",
		);

		expect(sql).toBe(
			'SELECT * FROM posts WHERE "posts"."published" = $1 AND "posts"."title" = $2 AND id = $3',
		);
		expect(params).toEqual([true, "Hello", "post-123"]);
	});
});
