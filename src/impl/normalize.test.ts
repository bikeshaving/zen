import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, primary, unique, references} from "./table.js";
import {
	extractEntityData,
	getPrimaryKeyValue,
	entityKey,
	buildEntityMap,
	resolveReferences,
	normalize,
	normalizeOne,
} from "./query.js";

// Test tables (using plain strings - normalization doesn't need UUID validation)
const users = table("users", {
	id: primary(z.string()),
	email: unique(z.string().email()),
	name: z.string(),
});

const posts = table("posts", {
	id: primary(z.string()),
	authorId: references(z.string(), users, {as: "author"}),
	title: z.string(),
	body: z.string(),
});

// Test data - simulating SQL JOIN result
const rawRows = [
	{
		"posts.id": "p1",
		"posts.authorId": "u1",
		"posts.title": "First Post",
		"posts.body": "Content 1",
		"users.id": "u1",
		"users.email": "alice@example.com",
		"users.name": "Alice",
	},
	{
		"posts.id": "p2",
		"posts.authorId": "u1",
		"posts.title": "Second Post",
		"posts.body": "Content 2",
		"users.id": "u1",
		"users.email": "alice@example.com",
		"users.name": "Alice",
	},
	{
		"posts.id": "p3",
		"posts.authorId": "u2",
		"posts.title": "Third Post",
		"posts.body": "Content 3",
		"users.id": "u2",
		"users.email": "bob@example.com",
		"users.name": "Bob",
	},
];

describe("extractEntityData", () => {
	test("extracts fields for table", () => {
		const row = rawRows[0];

		const postData = extractEntityData(row, "posts");
		expect(postData).toEqual({
			id: "p1",
			authorId: "u1",
			title: "First Post",
			body: "Content 1",
		});

		const userData = extractEntityData(row, "users");
		expect(userData).toEqual({
			id: "u1",
			email: "alice@example.com",
			name: "Alice",
		});
	});

	test("returns null for all-null data (LEFT JOIN no match)", () => {
		const row = {
			"posts.id": "p1",
			"posts.authorId": null,
			"posts.title": "Orphan Post",
			"posts.body": "Content",
			"users.id": null,
			"users.email": null,
			"users.name": null,
		};

		const postData = extractEntityData(row, "posts");
		expect(postData).not.toBeNull();
		expect(postData!.id).toBe("p1");

		const userData = extractEntityData(row, "users");
		expect(userData).toBeNull();
	});

	test("returns null for non-existent table", () => {
		const data = extractEntityData(rawRows[0], "nonexistent");
		expect(data).toBeNull();
	});
});

describe("getPrimaryKeyValue", () => {
	test("gets single primary key", () => {
		const entity = {id: "user-123", name: "Alice"};
		const pk = getPrimaryKeyValue(entity, users);
		expect(pk).toBe("user-123");
	});

	test("returns null for missing primary key", () => {
		const entity = {name: "Alice"};
		const pk = getPrimaryKeyValue(entity, users);
		expect(pk).toBeNull();
	});
});

describe("entityKey", () => {
	test("creates key from table and primary key", () => {
		expect(entityKey("users", "u1")).toBe("users:u1");
		expect(entityKey("posts", "p1")).toBe("posts:p1");
	});
});

describe("buildEntityMap", () => {
	test("builds map of all entities", () => {
		const entities = buildEntityMap(rawRows, [posts, users]);

		// Should have 3 posts + 2 users = 5 entities
		expect(entities.size).toBe(5);

		// Check posts
		expect(entities.has("posts:p1")).toBe(true);
		expect(entities.has("posts:p2")).toBe(true);
		expect(entities.has("posts:p3")).toBe(true);

		// Check users (deduplicated)
		expect(entities.has("users:u1")).toBe(true);
		expect(entities.has("users:u2")).toBe(true);
	});

	test("deduplicates entities with same primary key", () => {
		const entities = buildEntityMap(rawRows, [posts, users]);

		// Alice appears twice in raw rows but should only be stored once
		const aliceEntries = [...entities.entries()].filter(([k]) =>
			k.startsWith("users:u1"),
		);
		expect(aliceEntries.length).toBe(1);
	});
});

describe("resolveReferences", () => {
	test("resolves references to actual entities", () => {
		const entities = buildEntityMap(rawRows, [posts, users]);
		resolveReferences(entities, [posts, users]);

		const post1 = entities.get("posts:p1")!;
		const alice = entities.get("users:u1")!;

		// Should have "author" property pointing to Alice
		expect(post1.author).toBe(alice);
		expect((post1.author as any).name).toBe("Alice");
	});

	test("same referenced entity is same instance", () => {
		const entities = buildEntityMap(rawRows, [posts, users]);
		resolveReferences(entities, [posts, users]);

		const post1 = entities.get("posts:p1")!;
		const post2 = entities.get("posts:p2")!;

		// Both posts by Alice should reference the SAME object
		expect(post1.author).toBe(post2.author);
	});

	test("handles null references", () => {
		// Posts with nullable FK for testing orphan records
		const postsWithNullableFK = table("posts", {
			id: primary(z.string()),
			authorId: z.string().nullable(),
			title: z.string(),
			body: z.string(),
		});

		const rowsWithNull = [
			{
				"posts.id": "p1",
				"posts.authorId": null,
				"posts.title": "Orphan",
				"posts.body": "No author",
			},
		];

		const entities = buildEntityMap(rowsWithNull, [postsWithNullableFK]);
		resolveReferences(entities, [postsWithNullableFK, users]);

		const post = entities.get("posts:p1")!;
		// No reference defined in postsWithNullableFK, so no .author property
		expect(post.authorId).toBeNull();
	});
});

describe("normalize", () => {
	test("returns main table entities with references resolved", () => {
		const results = normalize<any>(rawRows, [posts, users]);

		expect(results.length).toBe(3);
		expect(results[0].id).toBe("p1");
		expect(results[0].title).toBe("First Post");
		expect(results[0].author.name).toBe("Alice");
	});

	test("maintains original row order", () => {
		const results = normalize<any>(rawRows, [posts, users]);

		expect(results[0].id).toBe("p1");
		expect(results[1].id).toBe("p2");
		expect(results[2].id).toBe("p3");
	});

	test("deduplicates referenced entities", () => {
		const results = normalize<any>(rawRows, [posts, users]);

		// Post 1 and Post 2 should have the same author instance
		expect(results[0].author).toBe(results[1].author);

		// Post 3 should have different author
		expect(results[2].author).not.toBe(results[0].author);
		expect(results[2].author.name).toBe("Bob");
	});

	test("returns empty array for empty rows", () => {
		const results = normalize<any>([], [posts, users]);
		expect(results).toEqual([]);
	});

	test("throws on empty tables", () => {
		expect(() => normalize(rawRows, [])).toThrow(
			"At least one table is required",
		);
	});

	test("handles duplicate rows (same entity multiple times)", () => {
		const duplicateRows = [
			...rawRows,
			rawRows[0], // Duplicate first row
		];

		const results = normalize<any>(duplicateRows, [posts, users]);

		// Should still only return 3 unique posts
		expect(results.length).toBe(3);
	});
});

describe("normalizeOne", () => {
	test("returns single entity", () => {
		const post = normalizeOne<any>(rawRows[0], [posts, users]);

		expect(post).not.toBeNull();
		expect(post!.id).toBe("p1");
		expect(post!.author.name).toBe("Alice");
	});

	test("returns null for null row", () => {
		const result = normalizeOne(null, [posts, users]);
		expect(result).toBeNull();
	});
});

describe("self-referencing tables", () => {
	test("handles self-referencing tables", () => {
		// Employee with manager (another employee)
		const employees = table("employees", {
			id: primary(z.string()),
			name: z.string(),
			managerId: z.string().nullable(),
		});

		const rows = [
			{
				"employees.id": "e1",
				"employees.name": "Alice",
				"employees.managerId": null,
			},
			{
				"employees.id": "e2",
				"employees.name": "Bob",
				"employees.managerId": "e1",
			},
		];

		const results = normalize<any>(rows, [employees]);

		expect(results.length).toBe(2);
		expect(results[0].name).toBe("Alice");
		expect(results[1].name).toBe("Bob");
	});
});

describe("circular references", () => {
	test("handles circular references without infinite loop", () => {
		// Users can have a featured post, posts have an author
		const circularUsers = table("users", {
			id: primary(z.string()),
			name: z.string(),
			featuredPostId: z.string().nullable(),
		});

		const circularPosts = table("posts", {
			id: primary(z.string()),
			title: z.string(),
			authorId: references(z.string(), circularUsers, {as: "author"}),
		});

		// Note: Can't actually create circular references() at definition time
		// because tables must be defined before being referenced.
		// But we can test that the normalization handles circular data.

		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
				"users.featuredPostId": "p1",
			},
		];

		// This should not infinite loop
		const results = normalize<any>(rows, [circularPosts, circularUsers]);

		expect(results.length).toBe(1);
		expect(results[0].id).toBe("p1");
		expect(results[0].author.id).toBe("u1");
		expect(results[0].author.name).toBe("Alice");
		// The user's featuredPostId is just a string, not resolved
		// (because we didn't define it as a reference)
		expect(results[0].author.featuredPostId).toBe("p1");
	});

	test("resolves mutual references when both are defined", () => {
		// Create tables where we manually test the resolution
		const usersTable = table("users", {
			id: primary(z.string()),
			name: z.string(),
		});

		const postsTable = table("posts", {
			id: primary(z.string()),
			title: z.string(),
			authorId: references(z.string(), usersTable, {as: "author"}),
		});

		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Post 1",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
			{
				"posts.id": "p2",
				"posts.title": "Post 2",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const results = normalize<any>(rows, [postsTable, usersTable]);

		// Both posts should reference the same user instance
		expect(results[0].author).toBe(results[1].author);
		expect(results[0].author.name).toBe("Alice");
	});
});

describe("deep nesting (3+ tables)", () => {
	test("resolves references across 3 tables", () => {
		const orgs = table("organizations", {
			id: primary(z.string()),
			name: z.string(),
		});

		const deepUsers = table("users", {
			id: primary(z.string()),
			name: z.string(),
			orgId: references(z.string(), orgs, {as: "organization"}),
		});

		const deepPosts = table("posts", {
			id: primary(z.string()),
			title: z.string(),
			authorId: references(z.string(), deepUsers, {as: "author"}),
		});

		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
				"users.orgId": "o1",
				"organizations.id": "o1",
				"organizations.name": "Acme Corp",
			},
			{
				"posts.id": "p2",
				"posts.title": "World",
				"posts.authorId": "u2",
				"users.id": "u2",
				"users.name": "Bob",
				"users.orgId": "o1",
				"organizations.id": "o1",
				"organizations.name": "Acme Corp",
			},
		];

		const results = normalize<any>(rows, [deepPosts, deepUsers, orgs]);

		expect(results.length).toBe(2);

		// Post -> Author -> Organization chain works
		expect(results[0].author.name).toBe("Alice");
		expect(results[0].author.organization.name).toBe("Acme Corp");

		expect(results[1].author.name).toBe("Bob");
		expect(results[1].author.organization.name).toBe("Acme Corp");

		// Same org instance for both users
		expect(results[0].author.organization).toBe(results[1].author.organization);
	});

	test("resolves 4-level deep nesting", () => {
		const countries = table("countries", {
			id: primary(z.string()),
			name: z.string(),
		});

		const cities = table("cities", {
			id: primary(z.string()),
			name: z.string(),
			countryId: references(z.string(), countries, {as: "country"}),
		});

		const offices = table("offices", {
			id: primary(z.string()),
			name: z.string(),
			cityId: references(z.string(), cities, {as: "city"}),
		});

		const employees = table("employees", {
			id: primary(z.string()),
			name: z.string(),
			officeId: references(z.string(), offices, {as: "office"}),
		});

		const rows = [
			{
				"employees.id": "e1",
				"employees.name": "Alice",
				"employees.officeId": "off1",
				"offices.id": "off1",
				"offices.name": "HQ",
				"offices.cityId": "c1",
				"cities.id": "c1",
				"cities.name": "San Francisco",
				"cities.countryId": "usa",
				"countries.id": "usa",
				"countries.name": "United States",
			},
		];

		const results = normalize<any>(rows, [
			employees,
			offices,
			cities,
			countries,
		]);

		expect(results[0].name).toBe("Alice");
		expect(results[0].office.name).toBe("HQ");
		expect(results[0].office.city.name).toBe("San Francisco");
		expect(results[0].office.city.country.name).toBe("United States");
	});
});

describe("unregistered table validation", () => {
	test("throws when joined table not passed to normalize", () => {
		// Query includes users data but we only pass posts table
		expect(() => normalize<any>(rawRows, [posts])).toThrow(
			'Query results contain columns for table(s) "users" not passed to all()/one()',
		);
	});

	test("error message includes all missing tables", () => {
		// Create rows with multiple unregistered tables
		const rowsWithMultipleTables = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"users.id": "u1",
				"users.name": "Alice",
				"comments.id": "c1",
				"comments.body": "Great post",
			},
		];

		expect(() => normalize<any>(rowsWithMultipleTables, [posts])).toThrow(
			/"users".*"comments"|"comments".*"users"/,
		);
	});

	test("does not throw when all tables are passed", () => {
		expect(() => normalize<any>(rawRows, [posts, users])).not.toThrow();
	});
});

describe("type coercion", () => {
	test("coerces date strings to Date objects", () => {
		const events = table("events", {
			id: primary(z.string()),
			name: z.string(),
			// z.coerce.date() converts string → Date
			createdAt: z.coerce.date(),
		});

		const rows = [
			{
				"events.id": "e1",
				"events.name": "Launch Party",
				"events.createdAt": "2024-01-15T10:30:00.000Z",
			},
		];

		const results = normalize<any>(rows, [events]);

		expect(results[0].createdAt).toBeInstanceOf(Date);
		expect(results[0].createdAt.toISOString()).toBe("2024-01-15T10:30:00.000Z");
	});

	test("coerces number strings to numbers", () => {
		const products = table("products", {
			id: primary(z.string()),
			name: z.string(),
			// z.coerce.number() converts string → number
			price: z.coerce.number(),
			quantity: z.coerce.number().int(),
		});

		const rows = [
			{
				"products.id": "p1",
				"products.name": "Widget",
				"products.price": "19.99",
				"products.quantity": "42",
			},
		];

		const results = normalize<any>(rows, [products]);

		expect(results[0].price).toBe(19.99);
		expect(typeof results[0].price).toBe("number");
		expect(results[0].quantity).toBe(42);
		expect(typeof results[0].quantity).toBe("number");
	});

	test("coerces boolean strings/numbers to booleans", () => {
		const flags = table("flags", {
			id: primary(z.string()),
			name: z.string(),
			// z.coerce.boolean() converts truthy/falsy → boolean
			enabled: z.coerce.boolean(),
		});

		const rows = [
			{
				"flags.id": "f1",
				"flags.name": "Feature A",
				"flags.enabled": 1, // SQLite stores booleans as 0/1
			},
			{
				"flags.id": "f2",
				"flags.name": "Feature B",
				"flags.enabled": 0,
			},
		];

		const results = normalize<any>(rows, [flags]);

		expect(results[0].enabled).toBe(true);
		expect(typeof results[0].enabled).toBe("boolean");
		expect(results[1].enabled).toBe(false);
	});

	test("coercion works with joins", () => {
		const authors = table("authors", {
			id: primary(z.string()),
			name: z.string(),
		});

		const articles = table("articles", {
			id: primary(z.string()),
			authorId: references(z.string(), authors, {as: "author"}),
			title: z.string(),
			publishedAt: z.coerce.date(),
			viewCount: z.coerce.number().int(),
		});

		const rows = [
			{
				"articles.id": "a1",
				"articles.authorId": "u1",
				"articles.title": "Hello World",
				"articles.publishedAt": "2024-06-01T00:00:00.000Z",
				"articles.viewCount": "1234",
				"authors.id": "u1",
				"authors.name": "Alice",
			},
		];

		const results = normalize<any>(rows, [articles, authors]);

		expect(results[0].publishedAt).toBeInstanceOf(Date);
		expect(results[0].viewCount).toBe(1234);
		expect(results[0].author.name).toBe("Alice");
	});
});
