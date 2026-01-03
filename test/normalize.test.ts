import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, extendZod} from "../src/impl/table.js";
import {
	extractEntityData,
	getPrimaryKeyValue,
	entityKey,
	buildEntityMap,
	resolveReferences,
	normalize,
	normalizeOne,
} from "../src/impl/query.js";

// Extend Zod once before tests
extendZod(z);

// Test tables (using plain strings - normalization doesn't need UUID validation)
const users = table("users", {
	id: z.string().db.primary(),
	email: z.string().email().db.unique(),
	name: z.string(),
});

const posts = table("posts", {
	id: z.string().db.primary(),
	authorId: z.string().db.references(users, "author"),
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
			id: z.string().db.primary(),
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
			id: z.string().db.primary(),
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
			id: z.string().db.primary(),
			name: z.string(),
			featuredPostId: z.string().nullable(),
		});

		const circularPosts = table("posts", {
			id: z.string().db.primary(),
			title: z.string(),
			authorId: z.string().db.references(circularUsers, "author"),
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
			id: z.string().db.primary(),
			name: z.string(),
		});

		const postsTable = table("posts", {
			id: z.string().db.primary(),
			title: z.string(),
			authorId: z.string().db.references(usersTable, "author"),
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
			id: z.string().db.primary(),
			name: z.string(),
		});

		const deepUsers = table("users", {
			id: z.string().db.primary(),
			name: z.string(),
			orgId: z.string().db.references(orgs, "organization"),
		});

		const deepPosts = table("posts", {
			id: z.string().db.primary(),
			title: z.string(),
			authorId: z.string().db.references(deepUsers, "author"),
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
			id: z.string().db.primary(),
			name: z.string(),
		});

		const cities = table("cities", {
			id: z.string().db.primary(),
			name: z.string(),
			countryId: z.string().db.references(countries, "country"),
		});

		const offices = table("offices", {
			id: z.string().db.primary(),
			name: z.string(),
			cityId: z.string().db.references(cities, "city"),
		});

		const employees = table("employees", {
			id: z.string().db.primary(),
			name: z.string(),
			officeId: z.string().db.references(offices, "office"),
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

describe("type decoding", () => {
	// Zen handles DB→JS type conversion via decodeData, not Zod coercion.
	// Use z.date(), z.boolean(), z.number() - not z.coerce.*
	// Zod transforms/coercion only run on writes, never on reads.

	test("decodes date strings to Date objects", () => {
		const events = table("events", {
			id: z.string().db.primary(),
			name: z.string(),
			// Use z.date() - Zen's decodeData handles string→Date conversion
			createdAt: z.date(),
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

	test("numbers from DB stay as numbers", () => {
		const products = table("products", {
			id: z.string().db.primary(),
			name: z.string(),
			// Use z.number() - DB drivers return numbers as numbers
			price: z.number(),
			quantity: z.number().int(),
		});

		// Real DB drivers return numbers, not strings
		const rows = [
			{
				"products.id": "p1",
				"products.name": "Widget",
				"products.price": 19.99,
				"products.quantity": 42,
			},
		];

		const results = normalize<any>(rows, [products]);

		expect(results[0].price).toBe(19.99);
		expect(typeof results[0].price).toBe("number");
		expect(results[0].quantity).toBe(42);
		expect(typeof results[0].quantity).toBe("number");
	});

	test("decodes boolean 0/1 to true/false", () => {
		const flags = table("flags", {
			id: z.string().db.primary(),
			name: z.string(),
			// Use z.boolean() - Zen's decodeData handles 0/1→boolean conversion
			enabled: z.boolean(),
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

	test("decoding works with joins", () => {
		const authors = table("authors", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const articles = table("articles", {
			id: z.string().db.primary(),
			authorId: z.string().db.references(authors, "author"),
			title: z.string(),
			publishedAt: z.date(),
			viewCount: z.number().int(),
		});

		const rows = [
			{
				"articles.id": "a1",
				"articles.authorId": "u1",
				"articles.title": "Hello World",
				"articles.publishedAt": "2024-06-01T00:00:00.000Z",
				"articles.viewCount": 1234,
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

describe("reverse relationships (has-many)", () => {
	test("populates reverse relationship array", () => {
		const authors = table("authors", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const books = table("books", {
			id: z.string().db.primary(),
			authorId: z.string().db.references(authors, "author", {
				reverseAs: "books",
			}),
			title: z.string(),
		});

		const rows = [
			{
				"books.id": "b1",
				"books.authorId": "a1",
				"books.title": "Book One",
				"authors.id": "a1",
				"authors.name": "Alice",
			},
			{
				"books.id": "b2",
				"books.authorId": "a1",
				"books.title": "Book Two",
				"authors.id": "a1",
				"authors.name": "Alice",
			},
		];

		const results = normalize<any>(rows, [books, authors]);

		// Forward reference works
		expect(results[0].author.name).toBe("Alice");
		expect(results[1].author.name).toBe("Alice");
		// Same instance
		expect(results[0].author).toBe(results[1].author);

		// Reverse reference populated
		const author = results[0].author;
		expect(author.books).toBeInstanceOf(Array);
		expect(author.books.length).toBe(2);
		expect(author.books[0].title).toBe("Book One");
		expect(author.books[1].title).toBe("Book Two");
	});

	test("empty array when no referencing entities", () => {
		const authors = table("authors", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const books = table("books", {
			id: z.string().db.primary(),
			authorId: z.string().db.references(authors, "author", {
				reverseAs: "books",
			}),
			title: z.string(),
		});

		const rows = [
			{
				"books.id": "b1",
				"books.authorId": "a1",
				"books.title": "Book One",
				"authors.id": "a1",
				"authors.name": "Alice",
			},
			{
				"authors.id": "a2",
				"authors.name": "Bob",
			},
		];

		const entities = buildEntityMap(rows, [books, authors]);
		resolveReferences(entities, [books, authors]);

		const alice = entities.get("authors:a1");
		const bob = entities.get("authors:a2");

		expect(alice).toBeDefined();
		expect(bob).toBeDefined();
		expect((alice as any).books.length).toBe(1);
		expect((bob as any).books.length).toBe(0); // Empty array
	});

	test("handles null foreign keys", () => {
		const authors = table("authors", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const books = table("books", {
			id: z.string().db.primary(),
			authorId: z.string().nullable().db.references(authors, "author", {
				reverseAs: "books",
			}),
			title: z.string(),
		});

		const rows = [
			{
				"books.id": "b1",
				"books.authorId": "a1",
				"books.title": "Book One",
				"authors.id": "a1",
				"authors.name": "Alice",
			},
			{
				"books.id": "b2",
				"books.authorId": null,
				"books.title": "Book Two (anonymous)",
			},
		];

		const results = normalize<any>(rows, [books, authors]);

		expect(results[0].author.name).toBe("Alice");
		expect(results[1].author).toBeNull();

		// Reverse: only b1 appears in alice.books
		const alice = results[0].author;
		expect(alice.books.length).toBe(1);
		expect(alice.books[0].title).toBe("Book One");
	});

	test("multiple reverse relationships on same table", () => {
		const users = table("users", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const posts = table("posts", {
			id: z.string().db.primary(),
			authorId: z.string().db.references(users, "author", {
				reverseAs: "authoredPosts",
			}),
			editorId: z.string().nullable().db.references(users, "editor", {
				reverseAs: "editedPosts",
			}),
			title: z.string(),
		});

		const rows = [
			{
				"posts.id": "p1",
				"posts.authorId": "u1",
				"posts.editorId": "u2",
				"posts.title": "Post One",
				"users.id": "u1",
				"users.name": "Alice",
			},
			{
				"posts.id": "p2",
				"posts.authorId": "u1",
				"posts.editorId": null,
				"posts.title": "Post Two",
			},
			{
				"users.id": "u2",
				"users.name": "Bob",
			},
		];

		const entities = buildEntityMap(rows, [posts, users]);
		resolveReferences(entities, [posts, users]);

		const alice = entities.get("users:u1");
		const bob = entities.get("users:u2");

		expect((alice as any).authoredPosts.length).toBe(2);
		expect((alice as any).editedPosts.length).toBe(0);

		expect((bob as any).authoredPosts.length).toBe(0);
		expect((bob as any).editedPosts.length).toBe(1);
		expect((bob as any).editedPosts[0].title).toBe("Post One");
	});

	test("works with many-to-many through join table", () => {
		const posts = table("posts", {
			id: z.string().db.primary(),
			title: z.string(),
		});

		const tags = table("tags", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const postTags = table("post_tags", {
			id: z.string().db.primary(), // Add primary key for join table
			postId: z.string().db.references(posts, "post", {reverseAs: "postTags"}),
			tagId: z.string().db.references(tags, "tag", {reverseAs: "postTags"}),
		});

		const rows = [
			{
				"post_tags.id": "pt1",
				"post_tags.postId": "p1",
				"post_tags.tagId": "t1",
				"posts.id": "p1",
				"posts.title": "My Post",
				"tags.id": "t1",
				"tags.name": "javascript",
			},
			{
				"post_tags.id": "pt2",
				"post_tags.postId": "p1",
				"post_tags.tagId": "t2",
				"posts.id": "p1",
				"posts.title": "My Post",
				"tags.id": "t2",
				"tags.name": "typescript",
			},
		];

		const results = normalize<any>(rows, [postTags, posts, tags]);

		// PostTags have forward refs
		expect(results[0].post.title).toBe("My Post");
		expect(results[0].tag.name).toBe("javascript");
		expect(results[1].tag.name).toBe("typescript");

		// Reverse: post.postTags and tag.postTags
		const post = results[0].post;
		const jsTag = results[0].tag;
		const tsTag = results[1].tag;

		expect(post.postTags.length).toBe(2);
		expect(jsTag.postTags.length).toBe(1);
		expect(tsTag.postTags.length).toBe(1);
	});
});

describe("many-to-many relationships (comprehensive)", () => {
	// Reusable table definitions
	const postsTable = table("posts", {
		id: z.string().db.primary(),
		title: z.string(),
		published: z.boolean(),
	});

	const tagsTable = table("tags", {
		id: z.string().db.primary(),
		name: z.string(),
	});

	const postTagsTable = table("post_tags", {
		id: z.string().db.primary(),
		postId: z.string().db.references(postsTable, "post", {
			reverseAs: "postTags",
		}),
		tagId: z.string().db.references(tagsTable, "tag", {reverseAs: "postTags"}),
	});

	test("query from posts side - get all tags for posts", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Intro to TypeScript",
				"posts.published": true,
				"post_tags.id": "pt1",
				"post_tags.postId": "p1",
				"post_tags.tagId": "t1",
				"tags.id": "t1",
				"tags.name": "typescript",
			},
			{
				"posts.id": "p1",
				"posts.title": "Intro to TypeScript",
				"posts.published": true,
				"post_tags.id": "pt2",
				"post_tags.postId": "p1",
				"post_tags.tagId": "t2",
				"tags.id": "t2",
				"tags.name": "javascript",
			},
			{
				"posts.id": "p2",
				"posts.title": "Advanced Patterns",
				"posts.published": false,
				"post_tags.id": "pt3",
				"post_tags.postId": "p2",
				"post_tags.tagId": "t1",
				"tags.id": "t1",
				"tags.name": "typescript",
			},
		];

		const results = normalize<any>(rows, [
			postsTable,
			postTagsTable,
			tagsTable,
		]);

		// Main result is posts
		expect(results.length).toBe(2);
		expect(results[0].title).toBe("Intro to TypeScript");
		expect(results[1].title).toBe("Advanced Patterns");

		// Post 1 has 2 tags via reverse relationship
		const post1 = results[0];
		expect(post1.postTags.length).toBe(2);
		expect(post1.postTags[0].tag.name).toBe("typescript");
		expect(post1.postTags[1].tag.name).toBe("javascript");

		// Post 2 has 1 tag
		const post2 = results[1];
		expect(post2.postTags.length).toBe(1);
		expect(post2.postTags[0].tag.name).toBe("typescript");

		// Tags have reverse relationships too
		const tsTag = post1.postTags[0].tag;
		expect(tsTag.postTags.length).toBe(2); // Used by p1 and p2
	});

	test("query from tags side - get all posts for a tag", () => {
		const rows = [
			{
				"tags.id": "t1",
				"tags.name": "typescript",
				"post_tags.id": "pt1",
				"post_tags.postId": "p1",
				"post_tags.tagId": "t1",
				"posts.id": "p1",
				"posts.title": "Intro to TypeScript",
				"posts.published": true,
			},
			{
				"tags.id": "t1",
				"tags.name": "typescript",
				"post_tags.id": "pt2",
				"post_tags.postId": "p2",
				"post_tags.tagId": "t1",
				"posts.id": "p2",
				"posts.title": "Advanced Patterns",
				"posts.published": false,
			},
		];

		const results = normalize<any>(rows, [
			tagsTable,
			postTagsTable,
			postsTable,
		]);

		// Main result is tags
		expect(results.length).toBe(1);
		const tag = results[0];
		expect(tag.name).toBe("typescript");

		// Tag has 2 posts via reverse relationship
		expect(tag.postTags.length).toBe(2);
		expect(tag.postTags[0].post.title).toBe("Intro to TypeScript");
		expect(tag.postTags[1].post.title).toBe("Advanced Patterns");
	});

	test("post with no tags (empty reverse relationship)", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Untagged Post",
				"posts.published": true,
			},
			{
				"posts.id": "p2",
				"posts.title": "Tagged Post",
				"posts.published": true,
				"post_tags.id": "pt1",
				"post_tags.postId": "p2",
				"post_tags.tagId": "t1",
				"tags.id": "t1",
				"tags.name": "typescript",
			},
		];

		const results = normalize<any>(rows, [
			postsTable,
			postTagsTable,
			tagsTable,
		]);

		expect(results.length).toBe(2);
		expect(results[0].postTags).toEqual([]); // Empty array
		expect(results[1].postTags.length).toBe(1);
	});

	test("tag with no posts (empty reverse relationship)", () => {
		const rows = [
			{
				"tags.id": "t1",
				"tags.name": "unused-tag",
			},
			{
				"tags.id": "t2",
				"tags.name": "popular-tag",
				"post_tags.id": "pt1",
				"post_tags.postId": "p1",
				"post_tags.tagId": "t2",
				"posts.id": "p1",
				"posts.title": "Some Post",
				"posts.published": true,
			},
		];

		const results = normalize<any>(rows, [
			tagsTable,
			postTagsTable,
			postsTable,
		]);

		expect(results.length).toBe(2);
		expect(results[0].postTags).toEqual([]); // Unused tag
		expect(results[1].postTags.length).toBe(1);
	});

	test("multiple many-to-many relationships on same entities", () => {
		const usersTable = table("users", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const projectsTable = table("projects", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		// Users can be members or admins of projects
		const projectMembersTable = table("project_members", {
			id: z.string().db.primary(),
			userId: z.string().db.references(usersTable, "user", {
				reverseAs: "memberships",
			}),
			projectId: z.string().db.references(projectsTable, "project", {
				reverseAs: "members",
			}),
		});

		const projectAdminsTable = table("project_admins", {
			id: z.string().db.primary(),
			userId: z.string().db.references(usersTable, "user", {
				reverseAs: "adminships",
			}),
			projectId: z.string().db.references(projectsTable, "project", {
				reverseAs: "admins",
			}),
		});

		const rows = [
			// Alice is member of p1
			{
				"users.id": "u1",
				"users.name": "Alice",
				"project_members.id": "pm1",
				"project_members.userId": "u1",
				"project_members.projectId": "p1",
				"projects.id": "p1",
				"projects.name": "Project Alpha",
			},
			// Alice is admin of p1
			{
				"project_admins.id": "pa1",
				"project_admins.userId": "u1",
				"project_admins.projectId": "p1",
			},
		];

		const entities = buildEntityMap(rows, [
			usersTable,
			projectsTable,
			projectMembersTable,
			projectAdminsTable,
		]);
		resolveReferences(entities, [
			usersTable,
			projectsTable,
			projectMembersTable,
			projectAdminsTable,
		]);

		const alice = entities.get("users:u1");
		const project = entities.get("projects:p1");

		expect(alice).toBeDefined();
		expect(project).toBeDefined();

		// Alice has separate memberships and adminships arrays
		expect((alice as any).memberships.length).toBe(1);
		expect((alice as any).adminships.length).toBe(1);

		// Project has separate members and admins arrays
		expect((project as any).members.length).toBe(1);
		expect((project as any).admins.length).toBe(1);
	});

	test("self-referential many-to-many (user followers)", () => {
		const usersTable = table("users", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const followsTable = table("follows", {
			id: z.string().db.primary(),
			followerId: z.string().db.references(usersTable, "follower", {
				reverseAs: "following",
			}),
			followeeId: z.string().db.references(usersTable, "followee", {
				reverseAs: "followers",
			}),
		});

		const rows = [
			// Alice follows Bob
			{
				"follows.id": "f1",
				"follows.followerId": "u1",
				"follows.followeeId": "u2",
				"users.id": "u1",
				"users.name": "Alice",
			},
			// Bob follows Alice
			{
				"follows.id": "f2",
				"follows.followerId": "u2",
				"follows.followeeId": "u1",
			},
			// User records
			{
				"users.id": "u2",
				"users.name": "Bob",
			},
		];

		const entities = buildEntityMap(rows, [followsTable, usersTable]);
		resolveReferences(entities, [followsTable, usersTable]);

		const alice = entities.get("users:u1");
		const bob = entities.get("users:u2");

		expect(alice).toBeDefined();
		expect(bob).toBeDefined();

		// Alice is following Bob (via followerId)
		expect((alice as any).following.length).toBe(1);
		expect((alice as any).following[0].followee.name).toBe("Bob");

		// Alice has Bob as a follower (via followeeId)
		expect((alice as any).followers.length).toBe(1);
		expect((alice as any).followers[0].follower.name).toBe("Bob");

		// Bob is following Alice
		expect((bob as any).following.length).toBe(1);
		expect((bob as any).following[0].followee.name).toBe("Alice");

		// Bob has Alice as a follower
		expect((bob as any).followers.length).toBe(1);
		expect((bob as any).followers[0].follower.name).toBe("Alice");
	});

	test("complex query with filtering - only published posts with tags", () => {
		const rows = [
			// Published post with tags
			{
				"posts.id": "p1",
				"posts.title": "Published Article",
				"posts.published": true,
				"post_tags.id": "pt1",
				"post_tags.postId": "p1",
				"post_tags.tagId": "t1",
				"tags.id": "t1",
				"tags.name": "typescript",
			},
			{
				"posts.id": "p1",
				"posts.title": "Published Article",
				"posts.published": true,
				"post_tags.id": "pt2",
				"post_tags.postId": "p1",
				"post_tags.tagId": "t2",
				"tags.id": "t2",
				"tags.name": "javascript",
			},
			// Another published post with same tag
			{
				"posts.id": "p2",
				"posts.title": "Another Article",
				"posts.published": true,
				"post_tags.id": "pt3",
				"post_tags.postId": "p2",
				"post_tags.tagId": "t1",
				"tags.id": "t1",
				"tags.name": "typescript",
			},
		];

		const results = normalize<any>(rows, [
			postsTable,
			postTagsTable,
			tagsTable,
		]);

		expect(results.length).toBe(2);

		// Verify deduplication - typescript tag appears in both posts but is same instance
		const tsTagFromPost1 = results[0].postTags.find(
			(pt: any) => pt.tag.name === "typescript",
		).tag;
		const tsTagFromPost2 = results[1].postTags[0].tag;
		expect(tsTagFromPost1).toBe(tsTagFromPost2); // Same object instance

		// Tag reverse relationship includes both posts
		expect(tsTagFromPost1.postTags.length).toBe(2);
	});

	test("join table with additional metadata", () => {
		const moviesTable = table("movies", {
			id: z.string().db.primary(),
			title: z.string(),
		});

		const actorsTable = table("actors", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const castTable = table("cast", {
			id: z.string().db.primary(),
			movieId: z.string().db.references(moviesTable, "movie", {
				reverseAs: "cast",
			}),
			actorId: z.string().db.references(actorsTable, "actor", {
				reverseAs: "filmography",
			}),
			role: z.string(), // Additional metadata on join table
			billingOrder: z.number(),
		});

		const rows = [
			{
				"movies.id": "m1",
				"movies.title": "The Matrix",
				"cast.id": "c1",
				"cast.movieId": "m1",
				"cast.actorId": "a1",
				"cast.role": "Neo",
				"cast.billingOrder": 1,
				"actors.id": "a1",
				"actors.name": "Keanu Reeves",
			},
			{
				"movies.id": "m1",
				"movies.title": "The Matrix",
				"cast.id": "c2",
				"cast.movieId": "m1",
				"cast.actorId": "a2",
				"cast.role": "Trinity",
				"cast.billingOrder": 2,
				"actors.id": "a2",
				"actors.name": "Carrie-Anne Moss",
			},
		];

		const results = normalize<any>(rows, [moviesTable, castTable, actorsTable]);

		expect(results.length).toBe(1);
		const movie = results[0];

		expect(movie.cast.length).toBe(2);
		expect(movie.cast[0].role).toBe("Neo");
		expect(movie.cast[0].billingOrder).toBe(1);
		expect(movie.cast[0].actor.name).toBe("Keanu Reeves");

		expect(movie.cast[1].role).toBe("Trinity");
		expect(movie.cast[1].billingOrder).toBe(2);
	});
});

describe("reverse relationship validation", () => {
	test("throws when reverseAs collides with target table field", () => {
		const users = table("users", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		expect(() =>
			table("posts", {
				id: z.string().db.primary(),
				authorId: z.string().db.references(users, "author", {
					reverseAs: "name", // Collides with users.name!
				}),
			}),
		).toThrow(/reverse reference property "name".*collides/i);
	});

	test("throws when as collides with source table field", () => {
		const users = table("users", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		expect(() =>
			table("posts", {
				id: z.string().db.primary(),
				title: z.string(),
				// "title" collides with posts.title!
				authorId: z.string().db.references(users, "title"),
			}),
		).toThrow(/reference property "title".*collides/i);
	});
});

// ============================================================================
// Enumerability and Serialization
// ============================================================================

describe("enumerability and serialization", () => {
	const users = table("users", {
		id: z.string().db.primary(),
		name: z.string(),
	});

	const posts = table("posts", {
		id: z.string().db.primary(),
		title: z.string(),
		authorId: z.string().db.references(users, "author", {
			reverseAs: "posts",
		}),
	});

	const postsWithDerived = table(
		"posts",
		{
			id: z.string().db.primary(),
			title: z.string(),
			authorId: z.string().db.references(users, "author", {
				reverseAs: "posts",
			}),
		},
		{
			derive: {
				titleUpper: (post: any) => post.title.toUpperCase(),
			},
		},
	);

	test("forward references are enumerable", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [posts, users]);
		const post = result[0];

		expect(post.author).toEqual({id: "u1", name: "Alice"});
		expect(Object.keys(post)).toContain("author");

		const descriptors = Object.getOwnPropertyDescriptor(post, "author");
		expect(descriptors?.enumerable).toBe(true);
	});

	test("forward references are immutable", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [posts, users]);
		const post = result[0];

		expect(() => {
			post.author = {id: "u2", name: "Bob"};
		}).toThrow();

		const descriptors = Object.getOwnPropertyDescriptor(post, "author");
		expect(descriptors?.writable).toBe(false);
	});

	test("reverse references are non-enumerable", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
			{
				"posts.id": "p2",
				"posts.title": "World",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [posts, users]);
		const post = result[0];
		const author = post.author;

		expect(author.posts).toHaveLength(2);
		expect(author.posts[0].id).toBe("p1");
		expect(author.posts[1].id).toBe("p2");
		expect(Object.keys(author)).not.toContain("posts");

		const descriptors = Object.getOwnPropertyDescriptor(author, "posts");
		expect(descriptors?.enumerable).toBe(false);
	});

	test("reverse references are immutable", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [posts, users]);
		const author = result[0].author;

		expect(() => {
			author.posts = [];
		}).toThrow();

		const descriptors = Object.getOwnPropertyDescriptor(author, "posts");
		expect(descriptors?.writable).toBe(false);
	});

	test("forward refs serialize with JSON.stringify", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [posts, users]);
		const post = result[0];

		const json = JSON.stringify(post);
		const parsed = JSON.parse(json);

		expect(parsed.author).toEqual({id: "u1", name: "Alice"});
	});

	test("reverse refs don't serialize with JSON.stringify", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
			{
				"posts.id": "p2",
				"posts.title": "World",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [posts, users]);
		const author = result[0].author;

		const json = JSON.stringify(author);
		const parsed = JSON.parse(json);

		expect(parsed.posts).toBeUndefined();
		expect(parsed).toEqual({id: "u1", name: "Alice"});
	});

	test("reverse refs prevent circular JSON when paired with forward refs", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [posts, users]);
		const post = result[0];

		// This would cause infinite recursion if reverse refs were enumerable
		// because post.author.posts[0].author.posts[0]... would be circular
		expect(() => JSON.stringify(post)).not.toThrow();

		const json = JSON.stringify(post);
		const parsed = JSON.parse(json);

		// Forward ref is serialized
		expect(parsed.author).toEqual({id: "u1", name: "Alice"});
		// But reverse ref is not, preventing the cycle
		expect(parsed.author.posts).toBeUndefined();
	});

	test("spread operator includes forward refs but not reverse refs", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [posts, users]);
		const post = result[0];
		const author = post.author;

		const spreadPost = {...post};
		const spreadAuthor = {...author};

		// Forward ref is included
		expect(spreadPost.author).toEqual({id: "u1", name: "Alice"});

		// Reverse ref is not included
		expect(spreadAuthor.posts).toBeUndefined();
	});

	test("derived properties are non-enumerable", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "hello",
				"posts.authorId": "u1",
			},
		];

		const result = normalize<any>(rows, [postsWithDerived]);
		const post = result[0];

		expect(post.titleUpper).toBe("HELLO");
		expect(Object.keys(post)).not.toContain("titleUpper");

		const descriptors = Object.getOwnPropertyDescriptor(post, "titleUpper");
		expect(descriptors?.enumerable).toBe(false);
	});

	test("derived properties are lazy getters", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "hello",
				"posts.authorId": "u1",
			},
		];

		const result = normalize<any>(rows, [postsWithDerived]);
		const post = result[0];

		const descriptors = Object.getOwnPropertyDescriptor(post, "titleUpper");
		expect(descriptors?.get).toBeInstanceOf(Function);
		expect(descriptors?.set).toBeUndefined();
	});

	test("derived properties don't serialize with JSON.stringify", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "hello",
				"posts.authorId": "u1",
			},
		];

		const result = normalize<any>(rows, [postsWithDerived]);
		const post = result[0];

		const json = JSON.stringify(post);
		const parsed = JSON.parse(json);

		expect(parsed.titleUpper).toBeUndefined();
		expect(parsed.title).toBe("hello");
	});

	test("derived properties can be explicitly included in objects", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "hello",
				"posts.authorId": "u1",
			},
		];

		const result = normalize<any>(rows, [postsWithDerived]);
		const post = result[0];

		const explicit = {...post, titleUpper: post.titleUpper};

		expect(explicit.titleUpper).toBe("HELLO");
		expect(JSON.stringify(explicit)).toContain("HELLO");
	});

	test("reverse refs can be explicitly included in objects", () => {
		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
			{
				"posts.id": "p2",
				"posts.title": "World",
				"posts.authorId": "u1",
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [posts, users]);
		const author = result[0].author;

		const explicit = {...author, posts: author.posts};

		expect(explicit.posts).toHaveLength(2);
		expect(JSON.stringify(explicit)).toContain("Hello");
	});

	test("null forward refs are enumerable", () => {
		const postsNullable = table("posts", {
			id: z.string().db.primary(),
			title: z.string(),
			authorId: z.string().nullable().db.references(users, "author", {
				reverseAs: "posts",
			}),
		});

		const rows = [
			{
				"posts.id": "p1",
				"posts.title": "Hello",
				"posts.authorId": null,
			},
		];

		const result = normalize<any>(rows, [postsNullable, users]);
		const post = result[0];

		expect(post.author).toBeNull();
		expect(Object.keys(post)).toContain("author");

		const descriptors = Object.getOwnPropertyDescriptor(post, "author");
		expect(descriptors?.enumerable).toBe(true);
	});

	test("empty reverse refs are non-enumerable", () => {
		const rows = [
			{
				"users.id": "u1",
				"users.name": "Alice",
			},
		];

		const result = normalize<any>(rows, [users, posts]);
		const user = result[0];

		expect(user.posts).toEqual([]);
		expect(Object.keys(user)).not.toContain("posts");

		const descriptors = Object.getOwnPropertyDescriptor(user, "posts");
		expect(descriptors?.enumerable).toBe(false);
	});
});
