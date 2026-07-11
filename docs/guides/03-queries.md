---
title: Queries
description: Write raw SQL with tagged templates and automatic parameterization. Type-safe fragment helpers handle the tedious parts without hiding your SQL.
---

ZenDB is **not a query builder**. Instead of fluent `.where().orderBy()` chains,
you write SQL directly with tagged templates. Values are automatically
parameterized, and helper fragments handle the tedious parts.

## Query methods

```typescript
const title = "Hello";
const userId = "u1";

// Single-table query, with normalization
const posts = await db.all(Posts)`WHERE published = ${true}`;

// Multi-table with joins — pass an array of tables
const joined = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
  WHERE ${Posts.cols.published} = ${true}
`;

// Get a single entity
const post = await db.get(Posts)`WHERE ${Posts.cols.title} = ${title}`;

// Get by primary key (convenience)
const byId = await db.get(Posts, "p1");

// Raw query (no normalization)
const counts = await db.query<{count: number}>`
  SELECT COUNT(*) as count FROM ${Posts} WHERE ${Posts.cols.authorId} = ${userId}
`;

// Execute a statement
await db.exec`CREATE INDEX idx_posts_author ON ${Posts}(${Posts.cols.authorId})`;

// Single scalar value
const total = await db.val<number>`SELECT COUNT(*) FROM ${Posts}`;
```

Every value interpolated into a template becomes a bound parameter — there is no
string concatenation, so injection isn't possible.

## Fragment helpers

Tables expose type-safe SQL fragments as methods. They compose inside templates:

```typescript
const rows = [
  {id: "p1", title: "Hello", published: true},
  {id: "p2", title: "World", published: false},
];

// UPDATE ... SET with set()
await db.exec`
  UPDATE ${Posts}
  SET ${Posts.set({title: "New Title", published: true})}
  WHERE ${Posts.cols.id} = ${"p1"}
`;
// -> UPDATE "posts" SET "title" = ?, "published" = ? WHERE "posts"."id" = ?

// JOIN ... ON with on()
const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
`;
// -> JOIN "users" ON "users"."id" = "posts"."authorId"

// Bulk INSERT with values()
await db.exec`INSERT INTO ${Posts} ${Posts.values(rows)}`;
// -> INSERT INTO "posts" ("id", "title", "published") VALUES (?, ?, ?), (?, ?, ?)

// Qualified column names with cols
const ordered = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
  ORDER BY ${Posts.cols.title} DESC
`;

// Safe IN clause with in()
const some = await db.all(Posts)`WHERE ${Posts.in("id", ["id1", "id2"])}`;
// -> WHERE "posts"."id" IN (?, ?)

// Empty arrays are handled correctly
const none = await db.all(Posts)`WHERE ${Posts.in("id", [])}`;
// -> WHERE 1 = 0
```

## Caching

Tagged template queries are cached by template-object identity — each call site
is compiled once. Interpolating different values reuses the same compiled SQL
with new parameters.

## Debugging

Inspect generated SQL and query plans without executing:

```typescript
const query = db.print`SELECT * FROM ${Posts} WHERE ${Posts.cols.published} = ${true}`;
query.sql;     // SELECT * FROM "posts" WHERE "posts"."published" = ?
query.params;  // [true]

// Fragments stringify too
Posts.set({title: "Updated"}).toString();
// SQLFragment { sql: "\"title\" = ?", params: ["Updated"] }

// Query execution plan
await db.explain`SELECT * FROM ${Posts}`;
```

See [Relationships](/guides/relationships/) for how `all()` and `get()`
normalize and resolve joined data.
