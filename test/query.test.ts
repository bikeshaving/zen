import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, extendZod} from "../src/impl/table.js";
import {
	buildSelectColumns,
	parseTemplate,
	buildQuery,
	createQuery,
	rawQuery,
} from "../src/impl/query.js";
import {makeTemplate, createTemplate} from "../src/impl/template.js";

// Extend Zod once before tests
extendZod(z);

// Test tables
const users = table("users", {
	id: z.string().uuid().db.primary(),
	email: z.string().email().db.unique(),
	name: z.string(),
});

const posts = table("posts", {
	id: z.string().uuid().db.primary(),
	authorId: z.string().uuid().db.references(users, {as: "author"}),
	title: z.string(),
	body: z.string(),
	published: z.boolean(),
});

describe("buildSelectColumns", () => {
	test("single table", () => {
		const {sql, params} = buildSelectColumns([users], "sqlite");

		expect(sql).toContain('"users"."id" AS "users.id"');
		expect(sql).toContain('"users"."email" AS "users.email"');
		expect(sql).toContain('"users"."name" AS "users.name"');
		expect(params).toEqual([]);
	});

	test("multiple tables", () => {
		const {sql} = buildSelectColumns([posts, users], "sqlite");

		// Post columns
		expect(sql).toContain('"posts"."id" AS "posts.id"');
		expect(sql).toContain('"posts"."authorId" AS "posts.authorId"');
		expect(sql).toContain('"posts"."title" AS "posts.title"');

		// User columns
		expect(sql).toContain('"users"."id" AS "users.id"');
		expect(sql).toContain('"users"."name" AS "users.name"');
	});

	test("mysql dialect uses backticks", () => {
		const {sql} = buildSelectColumns([users], "mysql");

		expect(sql).toContain("`users`.`id` AS `users.id`");
		expect(sql).toContain("`users`.`email` AS `users.email`");
	});
});

describe("parseTemplate", () => {
	test("no parameters", () => {
		const strings = ["WHERE active = true"] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [], "sqlite");

		expect(result.sql).toBe("WHERE active = true");
		expect(result.params).toEqual([]);
	});

	test("single parameter - sqlite", () => {
		const strings = ["WHERE id = ", ""] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, ["user-123"], "sqlite");

		expect(result.sql).toBe("WHERE id = ?");
		expect(result.params).toEqual(["user-123"]);
	});

	test("multiple parameters - sqlite", () => {
		const strings = [
			"WHERE id = ",
			" AND active = ",
			"",
		] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, ["user-123", true], "sqlite");

		expect(result.sql).toBe("WHERE id = ? AND active = ?");
		expect(result.params).toEqual(["user-123", true]);
	});

	test("postgresql uses numbered placeholders", () => {
		const strings = [
			"WHERE id = ",
			" AND active = ",
			"",
		] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, ["user-123", true], "postgresql");

		expect(result.sql).toBe("WHERE id = $1 AND active = $2");
		expect(result.params).toEqual(["user-123", true]);
	});

	test("trims whitespace", () => {
		const strings = ["  WHERE id = ", "  "] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, ["user-123"], "sqlite");

		expect(result.sql).toBe("WHERE id = ?");
	});
});

describe("buildQuery", () => {
	test("single table with no clauses", () => {
		const {sql, params} = buildQuery([users], "", "sqlite");

		expect(sql).toContain("SELECT");
		expect(sql).toContain('"users"."id" AS "users.id"');
		expect(sql).toContain('FROM "users"');
		expect(params).toEqual([]);
	});

	test("single table with WHERE", () => {
		const {sql} = buildQuery([users], "WHERE active = ?", "sqlite");

		expect(sql).toContain('FROM "users" WHERE active = ?');
	});

	test("multiple tables with JOIN", () => {
		const {sql} = buildQuery(
			[posts, users],
			'JOIN "users" ON "users"."id" = "posts"."authorId" WHERE published = ?',
			"sqlite",
		);

		expect(sql).toContain('FROM "posts"');
		expect(sql).toContain(
			'JOIN "users" ON "users"."id" = "posts"."authorId" WHERE published = ?',
		);
		// Should have columns from both tables
		expect(sql).toContain('"posts"."id" AS "posts.id"');
		expect(sql).toContain('"users"."id" AS "users.id"');
	});

	test("throws on empty tables", () => {
		expect(() => buildQuery([], "", "sqlite")).toThrow(
			"At least one table is required",
		);
	});
});

describe("createQuery", () => {
	test("creates tagged template function", () => {
		const query = createQuery([posts, users], "sqlite");
		const {sql, params} = query`
      JOIN "users" ON "users"."id" = "posts"."authorId"
      WHERE published = ${true}
    `;

		expect(sql).toContain('SELECT "posts"."id" AS "posts.id"');
		expect(sql).toContain('FROM "posts"');
		expect(sql).toContain('JOIN "users"');
		expect(sql).toContain("WHERE published = ?");
		expect(params).toEqual([true]);
	});

	test("handles multiple parameters", () => {
		const query = createQuery([posts], "sqlite");
		const userId = "user-123";
		const limit = 10;
		const {sql, params} = query`
      WHERE "authorId" = ${userId}
      ORDER BY "createdAt" DESC
      LIMIT ${limit}
    `;

		expect(sql).toContain("WHERE");
		expect(sql).toContain("LIMIT ?");
		expect(params).toEqual([userId, limit]);
	});
});

describe("rawQuery", () => {
	test("parses raw SQL template", () => {
		const userId = "user-123";
		const {sql, params} =
			rawQuery`SELECT COUNT(*) FROM posts WHERE author_id = ${userId}`;

		expect(sql).toBe("SELECT COUNT(*) FROM posts WHERE author_id = ?");
		expect(params).toEqual(["user-123"]);
	});

	test("handles no parameters", () => {
		const {sql, params} = rawQuery`SELECT COUNT(*) FROM posts`;

		expect(sql).toBe("SELECT COUNT(*) FROM posts");
		expect(params).toEqual([]);
	});
});

describe("table interpolation", () => {
	test("interpolates table as quoted name - sqlite", () => {
		const strings = [
			"FROM ",
			" WHERE id = ",
			"",
		] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [posts, "123"], "sqlite");

		expect(result.sql).toBe('FROM "posts" WHERE id = ?');
		expect(result.params).toEqual(["123"]);
	});

	test("interpolates table as quoted name - postgresql", () => {
		const strings = [
			"FROM ",
			" WHERE id = ",
			"",
		] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [posts, "123"], "postgresql");

		expect(result.sql).toBe('FROM "posts" WHERE id = $1');
		expect(result.params).toEqual(["123"]);
	});

	test("interpolates table as quoted name - mysql", () => {
		const strings = [
			"FROM ",
			" WHERE id = ",
			"",
		] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [posts, "123"], "mysql");

		expect(result.sql).toBe("FROM `posts` WHERE id = ?");
		expect(result.params).toEqual(["123"]);
	});

	test("multiple tables in template", () => {
		const strings = [
			"FROM ",
			" JOIN ",
			" ON ",
			".id = ",
			".authorId",
		] as unknown as TemplateStringsArray;
		const result = parseTemplate(
			strings,
			[posts, users, users, posts],
			"sqlite",
		);

		expect(result.sql).toBe(
			'FROM "posts" JOIN "users" ON "users".id = "posts".authorId',
		);
		expect(result.params).toEqual([]);
	});

	test("picked table interpolates with same name", () => {
		const PostSummary = posts.pick("id", "title");
		const strings = ["FROM ", ""] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [PostSummary], "sqlite");

		expect(result.sql).toBe('FROM "posts"');
	});
});

describe("buildSelectColumns with partial tables", () => {
	test("picks only selected columns", () => {
		const UserSummary = users.pick("id", "name");
		const {sql} = buildSelectColumns([UserSummary], "sqlite");

		expect(sql).toContain('"users"."id" AS "users.id"');
		expect(sql).toContain('"users"."name" AS "users.name"');
		expect(sql).not.toContain("email");
	});

	test("multiple partial tables", () => {
		const PostSummary = posts.pick("id", "title", "authorId");
		const UserSummary = users.pick("id", "name");
		const {sql} = buildSelectColumns([PostSummary, UserSummary], "sqlite");

		// Post columns
		expect(sql).toContain('"posts"."id" AS "posts.id"');
		expect(sql).toContain('"posts"."title" AS "posts.title"');
		expect(sql).toContain('"posts"."authorId" AS "posts.authorId"');
		expect(sql).not.toContain('"posts"."body"');

		// User columns
		expect(sql).toContain('"users"."id" AS "users.id"');
		expect(sql).toContain('"users"."name" AS "users.name"');
		expect(sql).not.toContain('"users"."email"');
	});
});

describe("buildSelectColumns with derived tables", () => {
	test("skips derived fields in regular columns", () => {
		const PostsWithCount = posts.derive("likeCount", z.number())`COUNT(*)`;

		const {sql, params} = buildSelectColumns([PostsWithCount], "sqlite");

		// Regular columns should be present
		expect(sql).toContain('"posts"."id" AS "posts.id"');
		expect(sql).toContain('"posts"."title" AS "posts.title"');

		// Should NOT have derived field as a regular column
		expect(sql).not.toContain('"posts"."likeCount" AS "posts.likeCount"');
		expect(params).toEqual([]);
	});

	test("appends derived expressions with auto-generated aliases", () => {
		const PostsWithCount = posts.derive("likeCount", z.number())`COUNT(*)`;

		const {sql} = buildSelectColumns([PostsWithCount], "sqlite");

		// Should have the derived expression with auto-generated prefixed alias
		expect(sql).toContain('(COUNT(*)) AS "posts.likeCount"');
	});

	test("handles composition (multiple derive() calls)", () => {
		const WithLikes = posts.derive("likeCount", z.number())`COUNT(likes.id)`;
		const WithLikesAndComments = WithLikes.derive(
			"commentCount",
			z.number(),
		)`COUNT(comments.id)`;

		const {sql} = buildSelectColumns([WithLikesAndComments], "sqlite");

		// Both expressions should be present
		expect(sql).toContain('AS "posts.likeCount"');
		expect(sql).toContain('AS "posts.commentCount"');
	});

	test("works with MySQL dialect", () => {
		const PostsWithCount = posts.derive("likeCount", z.number())`COUNT(*)`;

		const {sql} = buildSelectColumns([PostsWithCount], "mysql");

		// MySQL uses backticks
		expect(sql).toContain("`posts`.`id` AS `posts.id`");
		expect(sql).toContain("AS `posts.likeCount`");
	});

	test("collects params from derived expressions", () => {
		const PostsWithThreshold = posts.derive(
			"hasMany",
			z.boolean(),
		)`CASE WHEN COUNT(*) > ${10} THEN 1 ELSE 0 END`;

		const {sql, params} = buildSelectColumns([PostsWithThreshold], "sqlite");

		expect(sql).toContain("CASE WHEN COUNT(*) > ? THEN 1 ELSE 0 END");
		expect(params).toEqual([10]);
	});
});

describe("SQL fragment placeholder handling", () => {
	test("preserves literal ? inside single-quoted strings", () => {
		// SQL with a literal '?' in a string literal - template format
		const fragment = createTemplate(
			makeTemplate([`"users"."name" = '?' AND "users"."email" = `, ""]),
			["test@example.com"],
		);

		const strings = ["WHERE ", ""] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [fragment], "postgresql");

		// The '?' inside the string literal is preserved, only the real placeholder becomes $1
		expect(result.sql).toBe(
			`WHERE "users"."name" = '?' AND "users"."email" = $1`,
		);
		expect(result.params).toEqual(["test@example.com"]);
	});

	test("fragment with multiple params replaces placeholders in order", () => {
		// Template format: strings around placeholders
		const fragment = createTemplate(
			makeTemplate(['"posts"."title" = ', ' AND "posts"."body" LIKE ', ""]),
			["Hello", "%test%"],
		);

		const strings = ["WHERE ", ""] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [fragment], "postgresql");

		// Should correctly replace BOTH placeholders with $1 and $2
		expect(result.sql).toBe(
			'WHERE "posts"."title" = $1 AND "posts"."body" LIKE $2',
		);
		expect(result.params).toEqual(["Hello", "%test%"]);
	});

	test("preserves literal ? inside double-quoted identifiers", () => {
		// Unusual but valid: column name containing ? - new template format
		const fragment = createTemplate(makeTemplate(['"weird?col" = ', ""]), [
			"value",
		]);

		const strings = ["WHERE ", ""] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [fragment], "postgresql");

		expect(result.sql).toBe('WHERE "weird?col" = $1');
		expect(result.params).toEqual(["value"]);
	});

	test("handles escaped single quotes", () => {
		// SQL with escaped single quote: WHERE name = 'O''Brien' AND id = ?
		// New template format: strings with value placeholder
		const fragment = createTemplate(
			makeTemplate([`"users"."name" = 'O''Brien' AND "users"."id" = `, ""]),
			["123"],
		);

		const strings = ["WHERE ", ""] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [fragment], "postgresql");

		expect(result.sql).toBe(
			`WHERE "users"."name" = 'O''Brien' AND "users"."id" = $1`,
		);
		expect(result.params).toEqual(["123"]);
	});

	test("handles escaped double quotes", () => {
		// Column with escaped double quote in name - new template format
		const fragment = createTemplate(makeTemplate([`"weird""col" = `, ""]), [
			"value",
		]);

		const strings = ["WHERE ", ""] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [fragment], "postgresql");

		expect(result.sql).toBe(`WHERE "weird""col" = $1`);
		expect(result.params).toEqual(["value"]);
	});

	test("handles multiple ? in string literals", () => {
		// Multiple literal ? characters in a string - new template format
		const fragment = createTemplate(
			makeTemplate([`"col" = '???' AND "other" = `, ""]),
			["value"],
		);

		const strings = ["WHERE ", ""] as unknown as TemplateStringsArray;
		const result = parseTemplate(strings, [fragment], "postgresql");

		expect(result.sql).toBe(`WHERE "col" = '???' AND "other" = $1`);
		expect(result.params).toEqual(["value"]);
	});
});

// =============================================================================
// Regression: Empty array in in() (Issue #3)
// =============================================================================

describe("Empty array in in() clause", () => {
	test("in() with empty array returns 1 = 0 (always false)", () => {
		const {renderFragment} = require("../src/impl/query.js");
		const {sql, params} = renderFragment(users.in("id", []));
		expect(sql).toBe("1 = 0");
		expect(params).toEqual([]);
	});
});

// =============================================================================
// Regression: findNextPlaceholder safety (Issue #11)
// =============================================================================

describe("findNextPlaceholder safety", () => {
	test("unterminated string should not cause infinite loop", () => {
		// Unterminated single quote - should handle gracefully, not hang
		const strings = ["WHERE name = '", ""] as unknown as TemplateStringsArray;

		// Should complete within reasonable time (not hang)
		const start = Date.now();
		try {
			parseTemplate(strings, ["test"], "postgresql");
		} catch {
			// Error is fine, hanging is not
		}
		const elapsed = Date.now() - start;

		expect(elapsed).toBeLessThan(1000); // Should complete in under 1 second
	});

	test("deeply nested quotes should not cause issues", () => {
		// Many nested quote patterns
		const strings = [
			`WHERE a = '"'"'"'"'"' AND b = `,
			"",
		] as unknown as TemplateStringsArray;

		const start = Date.now();
		const result = parseTemplate(strings, [123], "postgresql");
		const elapsed = Date.now() - start;

		expect(elapsed).toBeLessThan(100);
		expect(result.params).toEqual([123]);
	});
});
