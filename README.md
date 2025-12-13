# @b9g/zealot

A schema-driven SQL client for TypeScript. Replaces ORMs (Prisma, Drizzle ORM), query builders (Kysely), and raw client wrappers with a single SQL-first library built on Zod schemas and event-driven migrations.

**Not an ORM** — a thin wrapper over SQL that uses Zod schemas to define storage, validation and metadata in one place.

## Design Principles

1. **ZREAM (Zod Rules Everything Around Me)** — One schema defines SQL types, validation rules, and form field metadata
2. **SQL is not hidden** — You write SQL with tagged templates; we handle parameterization and normalization
3. **Schema-driven normalization** — Relationships are resolved from table definitions, not query shape
4. **No codegen** — All behavior is runtime-driven; no schema files, migrations folders, CLI generators, or compile-time artifacts

## Two Modes

The library operates in two distinct modes that remain separate:

**Structural mode**: Table definitions, DDL generation, metadata extraction. Define your schema once and derive everything from it.

**Operational mode**: Queries, normalization, transactions. Write SQL directly with full control.

This separation is intentional — the system is not an ORM because these modes never blur together.

## Installation

```bash
bun add @b9g/zealot zod
# Install a driver (choose one):
bun add better-sqlite3  # for SQLite
bun add postgres        # for PostgreSQL
bun add mysql2          # for MySQL
```

## Drivers

Zealot provides drivers as subpath exports with optional peer dependencies:

```typescript
// Bun.SQL (built-in, works with SQLite/PostgreSQL/MySQL)
import { createDriver } from "@b9g/zealot/bun";
const { driver, close, dialect } = createDriver("sqlite://app.db");

// better-sqlite3 (Node.js SQLite)
import { createDriver, dialect } from "@b9g/zealot/sqlite";
const { driver, close } = createDriver("file:app.db");

// postgres.js
import { createDriver, dialect } from "@b9g/zealot/postgres";
const { driver, close } = createDriver("postgresql://localhost/mydb");

// mysql2
import { createDriver, dialect } from "@b9g/zealot/mysql";
const { driver, close } = createDriver("mysql://localhost/mydb");
```

Each driver exports `createDriver()` and `dialect` (except Bun which auto-detects).

## Quick Start

```typescript
import {z} from "zod";
import {table, primary, unique, references, generateDDL, Database} from "@b9g/zealot";
import {createDriver, dialect} from "@b9g/zealot/sqlite"; // or /postgres, /mysql, /bun
// Note: where(), set(), on(), values() are methods on Table objects, not imports

const {driver, close} = createDriver("file:app.db");

// 1. Define tables
const Users = table("users", {
  id: primary(z.string().uuid()),
  email: unique(z.string().email()),
  name: z.string(),
});

const Posts = table("posts", {
  id: primary(z.string().uuid()),
  authorId: references(z.string().uuid(), Users, {as: "author"}),
  title: z.string(),
  published: z.boolean().default(false),
});

// 2. Create database with migrations
const db = new Database(driver, {dialect});

db.addEventListener("upgradeneeded", (e) => {
  e.waitUntil((async () => {
    if (e.oldVersion < 1) {
      await db.exec`${generateDDL(Users)}`;
      await db.exec`${generateDDL(Posts)}`;
    }
    if (e.oldVersion < 2) {
      await db.exec`ALTER TABLE users ADD COLUMN avatar TEXT`;
    }
  })());
});

await db.open(2);

// 3. Insert with validation
const user = await db.insert(Users, {
  id: crypto.randomUUID(),
  email: "alice@example.com",
  name: "Alice",
});

// 4. Query with normalization
const posts = await db.all([Posts, Users])`
  JOIN users ON ${Posts.on("authorId")}
  WHERE ${Posts.where({published: true})}
`;

posts[0].author.name;              // "Alice" — resolved from JOIN
posts[0].author === posts[1].author; // true — same instance

// 5. Get by primary key
const post = await db.get(Posts, postId);

// 6. Update
await db.update(Users, user.id, {name: "Alice Smith"});
```

## Table Definitions

```typescript
import {z} from "zod";
import {table, primary, unique, index, references} from "@b9g/zealot";

const Users = table("users", {
  id: primary(z.string().uuid()),
  email: unique(z.string().email()),
  name: z.string().max(100),
  role: z.enum(["user", "admin"]).default("user"),
  createdAt: z.date().default(() => new Date()),
});

const Posts = table("posts", {
  id: primary(z.string().uuid()),
  title: z.string(),
  content: z.string().optional(),
  authorId: references(z.string().uuid(), Users, {as: "author", onDelete: "cascade"}),
  published: z.boolean().default(false),
});
```

**Field wrappers:**
- `primary(schema)` — Primary key
- `unique(schema)` — Unique constraint
- `index(schema)` — Create an index
- `references(schema, table, {as, field?, onDelete?})` — Foreign key with resolved property name

**Compound indexes** via table options:
```typescript
const Posts = table("posts", {...}, {
  indexes: [["authorId", "createdAt"]]
});
```

**Partial selects** with `pick()`:
```typescript
const UserSummary = Users.pick("id", "name");
const posts = await db.all([Posts, UserSummary])`
  JOIN users ON ${Posts.on("authorId")}
`;
// posts[0].author has only id and name
```

**Table identity**: A table definition is a singleton value. Importing it from multiple modules does not create duplicates — normalization and references rely on identity, not name.

## Queries

Tagged templates with automatic parameterization:

```typescript
// Single table query
const posts = await db.all(Posts)`WHERE published = ${true}`;

// Multi-table with joins — pass array
const posts = await db.all([Posts, Users])`
  JOIN users ON ${Posts.on("authorId")}
  WHERE ${Posts.where({published: true})}
`;

// Get single entity
const post = await db.get(Posts)`WHERE slug = ${slug}`;

// Get by primary key (convenience)
const post = await db.get(Posts, postId);

// Raw queries (no normalization)
const counts = await db.query<{count: number}>`
  SELECT COUNT(*) as count FROM posts WHERE author_id = ${userId}
`;

// Execute statements
await db.exec`CREATE INDEX idx_posts_author ON posts(author_id)`;

// Single value
const count = await db.val<number>`SELECT COUNT(*) FROM posts`;
```

## Fragment Helpers

Type-safe SQL fragments as methods on Table objects:

```typescript
// WHERE conditions with operator DSL
const posts = await db.all(Posts)`
  WHERE ${Posts.where({published: true, viewCount: {$gte: 100}})}
`;
// → WHERE "posts"."published" = ? AND "posts"."viewCount" >= ?

// UPDATE with set()
await db.exec`
  UPDATE posts
  SET ${Posts.set({title: "New Title", updatedAt: new Date()})}
  WHERE id = ${postId}
`;
// → UPDATE posts SET "title" = ?, "updatedAt" = ? WHERE id = ?

// JOIN with on()
const posts = await db.all([Posts, Users])`
  JOIN users ON ${Posts.on("authorId")}
  WHERE published = ${true}
`;
// → JOIN users ON "users"."id" = "posts"."authorId"

// Bulk INSERT with values()
await db.exec`
  INSERT INTO posts (id, title, published)
  VALUES ${Posts.values(rows, ["id", "title", "published"])}
`;
// → VALUES (?, ?, ?), (?, ?, ?)

// Qualified column names with cols
const posts = await db.all([Posts, Users])`
  JOIN users ON ${Posts.on("authorId")}
  ORDER BY ${Posts.cols.createdAt} DESC
`;
// → ORDER BY "posts"."createdAt" DESC
```

**Operators:** `$eq`, `$neq`, `$lt`, `$gt`, `$lte`, `$gte`, `$like`, `$in`, `$isNull`

Operators are intentionally limited to simple, single-column predicates. `OR`, subqueries, and cross-table logic belong in raw SQL.

## CRUD Helpers

```typescript
// Insert with Zod validation (uses RETURNING to get actual row)
const user = await db.insert(Users, {
  id: crypto.randomUUID(),
  email: "alice@example.com",
  name: "Alice",
});
// Returns actual row from DB, including DB-computed defaults

// Update by primary key (uses RETURNING)
const updated = await db.update(Users, userId, {name: "Bob"});

// Delete by primary key
await db.delete(Users, userId);
```

**RETURNING support:** `insert()` and `update()` use `RETURNING *` on SQLite and PostgreSQL to return the actual row from the database, including DB-computed defaults and triggers. MySQL falls back to a separate SELECT.

## Transactions

```typescript
await db.transaction(async (tx) => {
  const user = await tx.insert(Users, {...});
  await tx.insert(Posts, {authorId: user.id, ...});
  // Commits on success, rollbacks on error
});

// Returns values
const user = await db.transaction(async (tx) => {
  return await tx.insert(Users, {...});
});
```

## Migrations

IndexedDB-style event-based migrations:

```typescript
db.addEventListener("upgradeneeded", (e) => {
  e.waitUntil((async () => {
    if (e.oldVersion < 1) {
      await db.exec`${generateDDL(Users)}`;
      await db.exec`${generateDDL(Posts)}`;
    }
    if (e.oldVersion < 2) {
      await db.exec`ALTER TABLE users ADD COLUMN avatar TEXT`;
    }
  })());
});

await db.open(2); // Opens at version 2, fires upgradeneeded if needed
```

**Migration rules:**
- Migrations run sequentially from `oldVersion + 1` to `newVersion`
- If a migration crashes, the version does not bump
- You must keep migration code around indefinitely (forward-only, no down migrations)
- Multi-process safe via exclusive locking

**Why EventTarget?** Web standard pattern (like IndexedDB's `onupgradeneeded`). Third-party code can subscribe to lifecycle events without changing constructor signatures, enabling plugins for logging, tracing, and instrumentation.

## DDL Generation

Generate CREATE TABLE from Zod schemas:

```typescript
import {generateDDL} from "@b9g/zealot";

const ddl = generateDDL(Users, {dialect: "postgresql"});
// CREATE TABLE IF NOT EXISTS "users" (
//   "id" TEXT NOT NULL PRIMARY KEY,
//   "email" TEXT NOT NULL UNIQUE,
//   "name" VARCHAR(100) NOT NULL,
//   "role" TEXT DEFAULT 'user',
//   "created_at" TIMESTAMPTZ DEFAULT NOW()
// );
```

Foreign key constraints are generated automatically:

```typescript
const ddl = generateDDL(Posts);
// FOREIGN KEY ("author_id") REFERENCES "users"("id") ON DELETE CASCADE
```

**Dialect support:**

| Feature | SQLite | PostgreSQL | MySQL |
|---------|--------|------------|-------|
| Date type | TEXT | TIMESTAMPTZ | DATETIME |
| Date default | CURRENT_TIMESTAMP | NOW() | CURRENT_TIMESTAMP |
| Boolean | INTEGER | BOOLEAN | BOOLEAN |
| JSON | TEXT | JSONB | JSON |
| Quoting | "double" | "double" | \`backtick\` |

## Entity Normalization

Normalization is driven by table metadata, not query shape — SQL stays unrestricted.

The `all()`/`get()` methods:
1. Generate SELECT with prefixed column aliases (`posts.id AS "posts.id"`)
2. Parse rows into per-table entities
3. Deduplicate by primary key (same PK = same object instance)
4. Resolve `references()` to actual entity objects

```typescript
// Input rows from SQL:
[
  {"posts.id": "p1", "posts.authorId": "u1", "users.id": "u1", "users.name": "Alice"},
  {"posts.id": "p2", "posts.authorId": "u1", "users.id": "u1", "users.name": "Alice"},
]

// Output after normalization:
[
  {id: "p1", authorId: "u1", author: {id: "u1", name: "Alice"}},
  {id: "p2", authorId: "u1", author: /* same object as above */},
]
```

## Type Inference

```typescript
type User = Infer<typeof Users>;     // Full type (after read)
type NewUser = Insert<typeof Users>; // Insert type (respects defaults)
```

## Field Metadata

Tables expose metadata for form generation:

```typescript
const fields = Users.fields();
// {
//   email: { name: "email", type: "email", required: true, unique: true },
//   name: { name: "name", type: "text", required: true, maxLength: 100 },
//   role: { name: "role", type: "select", options: ["user", "admin"], default: "user" },
// }

const pk = Users.primaryKey();   // "id"
const refs = Posts.references(); // [{fieldName: "authorId", table: Users, as: "author"}]
```

## Performance

- Tagged template queries are cached by template object identity (compiled once per call site)
- Normalization cost is O(rows) with hash maps per table
- Reference resolution is zero-cost after deduplication

## Driver Interface

Adapters implement a simple interface:

```typescript
interface DatabaseDriver {
  all<T>(sql: string, params: unknown[]): Promise<T[]>;
  get<T>(sql: string, params: unknown[]): Promise<T | null>;
  run(sql: string, params: unknown[]): Promise<number>; // affected rows
  val<T>(sql: string, params: unknown[]): Promise<T>;   // single value
  escapeIdentifier(name: string): string;               // quote table/column names
  withMigrationLock?<T>(fn: () => Promise<T>): Promise<T>; // optional atomic migrations
}
```

**Migration locking**: If the driver provides `withMigrationLock()`, migrations run atomically (PostgreSQL uses advisory locks, MySQL uses `GET_LOCK`, SQLite uses exclusive transactions).

## Error Handling

All errors extend `ZealotError` with typed error codes:

```typescript
import {
  ZealotError,
  ValidationError,
  NotFoundError,
  isZealotError,
  hasErrorCode
} from "@b9g/zealot";

try {
  await db.insert(Users, invalidData);
} catch (e) {
  if (hasErrorCode(e, "VALIDATION_ERROR")) {
    console.log(e.fieldErrors); // {email: ["Invalid email"]}
  }
}
```

**Error types:**
- `ValidationError` — Zod validation failed
- `NotFoundError` — Entity not found
- `AlreadyExistsError` — Unique constraint violated
- `QueryError` — SQL execution failed
- `MigrationError` / `MigrationLockError` — Migration failures
- `ConstraintViolationError` — FK or check constraint violated
- `ConnectionError` / `TransactionError` — Connection issues

## What This Library Does Not Do

- **No model classes** — Tables are plain definitions, not class instances
- **No hidden JOINs** — You write all SQL explicitly
- **No implicit query building** — No `.where().orderBy().limit()` chains
- **No lazy loading** — Related data comes from your JOINs
- **No compile-time migrations** — Runtime event-based only
- **No ORM identity map** — Normalization is per-query, not session-wide
