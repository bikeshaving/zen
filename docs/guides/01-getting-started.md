---
title: Getting Started
description: Install ZenDB, connect a driver, define your first tables, and run a typed query — all in a few minutes.
---

ZenDB is the missing link between SQL and typed data. You define tables with
[Zod](https://zod.dev) schema, write raw SQL with tagged templates, and get
typed, normalized objects back. It works with SQLite, PostgreSQL, and MySQL.

## Installation

```bash
npm install @b9g/zen zod

# In Node, install a driver (choose one):
npm install better-sqlite3  # for SQLite
npm install postgres        # for PostgreSQL
npm install mysql2          # for MySQL
```

Bun has a built-in driver which uses `Bun.SQL`:

```bash
bun install @b9g/zen zod
```

## Connecting a driver

Each driver implements the `Driver` interface and is a separate module in the
package:

```typescript
import {Database} from "@b9g/zen";
import BunDriver from "@b9g/zen/bun";
import SQLiteDriver from "@b9g/zen/sqlite";
import PostgresDriver from "@b9g/zen/postgres";
import MySQLDriver from "@b9g/zen/mysql";

const sqliteDriver = new SQLiteDriver("file:app.db");
const postgresDriver = new PostgresDriver("postgresql://localhost/mydb");
const mySQLDriver = new MySQLDriver("mysql://localhost/mydb");

// Bun auto-detects the dialect from the connection URL.
const bunDriver = new BunDriver("sqlite://app.db");

const db = new Database(sqliteDriver);
```

## Your first tables

Define tables with Zod schema. The `.db` namespace adds database modifiers like
primary keys, unique constraints, foreign keys, and auto-generated values:

```typescript
import {z, table, Database} from "@b9g/zen";
import SQLiteDriver from "@b9g/zen/sqlite";

let Users = table("users", {
  id: z.string().uuid().db.primary().db.auto(),
  email: z.string().email().db.unique(),
  name: z.string(),
});

const Posts = table("posts", {
  id: z.string().uuid().db.primary().db.auto(),
  authorId: z.string().uuid().db.references(Users, "author"),
  title: z.string(),
  published: z.boolean().db.inserted(() => false),
});
```

## Migrations

ZenDB uses IndexedDB-style, event-based migrations. Schema changes are explicit
and forward-only:

```typescript
const db = new Database(new SQLiteDriver("file:app.db"));

db.addEventListener("upgradeneeded", (e) => {
  e.waitUntil((async () => {
    if (e.oldVersion < 1) {
      await db.ensureTable(Users);
      await db.ensureTable(Posts);
    }
  })());
});

await db.open(1);
```

## Insert, query, done

Writes are validated against your Zod schema. Auto-generated fields like `id`
are filled in for you:

```typescript
// Insert with validation (id auto-generated)
const user = await db.insert(Users, {
  email: "alice@example.com",
  name: "Alice",
});

// Query with raw SQL — objects come back typed and normalized
const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
  WHERE ${Posts.cols.published} = ${true}
`;

const author = posts[0].author;
author?.name;                 // "Alice" — resolved from the JOIN
author === posts[1].author;   // true — same instance (deduplicated)

// Get by primary key
const post = await db.get(Posts, posts[0].id);

// Update
await db.update(Users, {name: "Alice Smith"}, user.id);
```

## Where to next

- [Defining Tables](/guides/defining-tables/) — the full `.db` namespace
- [Queries](/guides/queries/) — tagged templates and fragment helpers
- [Relationships](/guides/relationships/) — normalization and references
- [Migrations](/guides/migrations/) — safe, additive schema evolution
- [API Reference](/guides/api-reference/) — every export at a glance
