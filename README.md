# @b9g/zealot

Zod-to-SQL database client and standard for TypeScript databases.

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
import BunDriver from "@b9g/zealot/bun";
const driver = new BunDriver("sqlite://app.db");

// better-sqlite3 (Node.js SQLite)
import SQLiteDriver from "@b9g/zealot/sqlite";
const driver = new SQLiteDriver("file:app.db");

// postgres.js
import PostgresDriver from "@b9g/zealot/postgres";
const driver = new PostgresDriver("postgresql://localhost/mydb");

// mysql2
import MySQLDriver from "@b9g/zealot/mysql";
const driver = new MySQLDriver("mysql://localhost/mydb");
```

Each driver is a class with a `dialect` property. Bun auto-detects dialect from the connection URL.

## Quick Start

```typescript
import {z} from "zod";
import {table, primary, unique, references, Database} from "@b9g/zealot";
import SQLiteDriver from "@b9g/zealot/sqlite"; // or PostgresDriver, MySQLDriver, BunDriver
// Note: where(), set(), on(), values(), in(), ddl() are methods on Table objects, not imports

const driver = new SQLiteDriver("file:app.db");

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
const db = new Database(driver);

db.addEventListener("upgradeneeded", (e) => {
  e.waitUntil((async () => {
    if (e.oldVersion < 1) {
      await db.exec`${Users.ddl()}`;
      await db.exec`${Posts.ddl()}`;
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
- `softDelete(schema)` — Soft delete timestamp field

**Soft delete:**
```typescript
const Users = table("users", {
  id: primary(z.string().uuid()),
  name: z.string(),
  deletedAt: softDelete(z.date().nullable().default(null)),
});

// Soft delete a record (sets deletedAt to current timestamp)
await db.softDelete(Users, userId);

// Hard delete (permanent removal)
await db.delete(Users, userId);

// Filter out soft-deleted records in queries
const activeUsers = await db.all(Users)`WHERE NOT ${Users.deleted()}`;
// → WHERE NOT "users"."deletedAt" IS NOT NULL
```

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
  INSERT INTO posts ${Posts.values(rows)}
`;
// → INSERT INTO posts ("id", "title", "published") VALUES (?, ?, ?), (?, ?, ?)

// Qualified column names with cols
const posts = await db.all([Posts, Users])`
  JOIN users ON ${Posts.on("authorId")}
  ORDER BY ${Posts.cols.createdAt} DESC
`;
// → ORDER BY "posts"."createdAt" DESC

// Safe IN clause with in()
const postIds = ["id1", "id2", "id3"];
const posts = await db.all(Posts)`WHERE ${Posts.in("id", postIds)}`;
// → WHERE "posts"."id" IN (?, ?, ?)

// Empty arrays handled correctly
const posts = await db.all(Posts)`WHERE ${Posts.in("id", [])}`;
// → WHERE 1 = 0
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

// Soft delete (sets deletedAt timestamp, requires softDelete() field)
await db.softDelete(Users, userId);
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
      await db.exec`${Users.ddl()}`;
      await db.exec`${Posts.ddl()}`;
    }
    if (e.oldVersion < 2) {
      await db.exec`${Posts.ensureColumn("views")}`;
    }
    if (e.oldVersion < 3) {
      await db.exec`${Posts.ensureIndex(["authorId", "createdAt"])}`;
    }
  })());
});

await db.open(3); // Opens at version 3, fires upgradeneeded if needed
```

**Migration rules:**
- Migrations run sequentially from `oldVersion + 1` to `newVersion`
- If a migration crashes, the version does not bump
- You must keep migration code around indefinitely (forward-only, no down migrations)
- Multi-process safe via exclusive locking

**Why EventTarget?** Web standard pattern (like IndexedDB's `onupgradeneeded`). Third-party code can subscribe to lifecycle events without changing constructor signatures, enabling plugins for logging, tracing, and instrumentation.

### Safe Migration Helpers

Zealot provides idempotent helpers that encourage safe, additive-only migrations:

```typescript
// Add a new column (reads from schema)
const Posts = table("posts", {
  id: primary(z.string()),
  title: z.string(),
  views: z.number().default(0), // NEW - add to schema
});

if (e.oldVersion < 2) {
  await db.exec`${Posts.ensureColumn("views")}`;
}
// → ALTER TABLE "posts" ADD COLUMN IF NOT EXISTS "views" REAL DEFAULT 0

// Add an index
if (e.oldVersion < 3) {
  await db.exec`${Posts.ensureIndex(["authorId", "createdAt"])}`;
}
// → CREATE INDEX IF NOT EXISTS "idx_posts_authorId_createdAt" ON "posts"("authorId", "createdAt")

// Safe column rename (additive, non-destructive)
const Users = table("users", {
  emailAddress: z.string().email(), // renamed from "email"
});

if (e.oldVersion < 4) {
  await db.exec`${Users.ensureColumn("emailAddress")}`;
  await db.exec`${Users.copyColumn("email", "emailAddress")}`;
  // Keep old "email" column for backwards compat
  // Drop it in a later migration if needed (manual SQL)
}
// → UPDATE "users" SET "emailAddress" = "email" WHERE "emailAddress" IS NULL
```

**Helper methods:**
- `table.ensureColumn(fieldName, options?)` - Idempotent ALTER TABLE ADD COLUMN
- `table.ensureIndex(fields, options?)` - Idempotent CREATE INDEX
- `table.copyColumn(from, to)` - Copy data between columns (for safe renames)

All helpers read from your table schema (single source of truth) and are safe to run multiple times (idempotent).

**Destructive operations** (DROP COLUMN, etc.) are not provided - write raw SQL if truly needed:
```typescript
// Manual destructive operation
if (e.oldVersion < 5) {
  await db.exec`ALTER TABLE users DROP COLUMN deprecated_field`;
}
```

## DDL Generation

Generate CREATE TABLE from Zod schemas:

```typescript
const ddl = Users.ddl({ dialect: "postgresql" });
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
const ddl = Posts.ddl();
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

const pkName = Users._meta.primary;   // "id" (field name)
const pkFragment = Users.primary;     // ColumnFragment: "users"."id"
const refs = Posts.references();      // [{fieldName: "authorId", table: Users, as: "author"}]
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
  ConstraintViolationError,
  NotFoundError,
  isZealotError,
  hasErrorCode
} from "@b9g/zealot";

// Validation errors (Zod/Standard Schema)
try {
  await db.insert(Users, { email: "not-an-email" });
} catch (e) {
  if (hasErrorCode(e, "VALIDATION_ERROR")) {
    console.log(e.fieldErrors); // {email: ["Invalid email"]}
  }
}

// Constraint violations (database-level)
try {
  await db.insert(Users, { id: "1", email: "duplicate@example.com" });
} catch (e) {
  if (e instanceof ConstraintViolationError) {
    console.log(e.kind);        // "unique"
    console.log(e.constraint);  // "users_email_unique"
    console.log(e.table);       // "users"
    console.log(e.column);      // "email"
  }
}

// Transaction errors (rolled back automatically)
await db.transaction(async (tx) => {
  await tx.insert(Users, newUser);
  await tx.insert(Posts, newPost); // Fails → transaction rolled back
});
```

**Error types:**
- `ValidationError` — Schema validation failed (fieldErrors, nested paths)
- `ConstraintViolationError` — Database constraint violated (kind, constraint, table, column)
- `NotFoundError` — Entity not found (tableName, id)
- `AlreadyExistsError` — Unique constraint violated (tableName, field, value)
- `QueryError` — SQL execution failed (sql)
- `MigrationError` / `MigrationLockError` — Migration failures (fromVersion, toVersion)
- `ConnectionError` / `TransactionError` — Connection/transaction issues

## Debugging

Inspect generated SQL and query plans:

```typescript
// Print SQL without executing
const query = db.print`SELECT * FROM ${Posts} WHERE ${Posts.where({ published: true })}`;
console.log(query.sql);     // SELECT * FROM "posts" WHERE "posts"."published" = $1
console.log(query.params);  // [true]

// Inspect DDL generation
const ddl = db.print`${Posts.ddl()}`;
console.log(ddl.sql);  // CREATE TABLE IF NOT EXISTS "posts" (...)

// Analyze query execution plan
const plan = await db.explain`
  SELECT * FROM ${Posts}
  WHERE ${Posts.where({ authorId: userId })}
`;
console.log(plan);
// SQLite: [{ detail: "SEARCH posts USING INDEX idx_posts_authorId (authorId=?)" }]
// PostgreSQL: [{ "QUERY PLAN": "Index Scan using idx_posts_authorId on posts" }]

// Debug fragments
console.log(Posts.where({ published: true }).toString());
// SQLFragment { sql: "\"posts\".\"published\" = ?", params: [true] }

console.log(Posts.ddl().toString());
// DDLFragment { type: "create-table", table: "posts" }
```

## Dialect Support

| Feature | SQLite | PostgreSQL | MySQL |
|---------|--------|------------|-------|
| **DDL Generation** | ✅ | ✅ | ✅ |
| **RETURNING** | ✅ | ✅ | ❌ (uses SELECT after) |
| **IF NOT EXISTS** (CREATE TABLE) | ✅ | ✅ | ✅ |
| **IF NOT EXISTS** (ADD COLUMN) | ✅ | ✅ | ❌ (may error if exists) |
| **Migration Locks** | BEGIN EXCLUSIVE | pg_advisory_lock | GET_LOCK |
| **EXPLAIN** | EXPLAIN QUERY PLAN | EXPLAIN | EXPLAIN |
| **JSON Type** | TEXT | JSONB | TEXT |
| **Boolean Type** | INTEGER (0/1) | BOOLEAN | BOOLEAN |
| **Date Type** | TEXT (ISO) | TIMESTAMPTZ | DATETIME |
| **Transactions** | ✅ | ✅ | ✅ |
| **Advisory Locks** | ❌ | ✅ | ✅ (named) |

## What This Library Does Not Do

**Query Generation:**
- **No model classes** — Tables are plain definitions, not class instances
- **No hidden JOINs** — You write all SQL explicitly
- **No implicit query building** — No `.where().orderBy().limit()` chains
- **No lazy loading** — Related data comes from your JOINs
- **No ORM identity map** — Normalization is per-query, not session-wide

**Migrations:**
- **No down migrations** — Forward-only, monotonic versioning (1 → 2 → 3)
- **No destructive helpers** — No `dropColumn()`, `dropTable()`, `renameColumn()` methods
- **No automatic migrations** — DDL must be written explicitly in upgrade events
- **No migration files** — Event handlers replace traditional migration folders
- **No branching versions** — Linear version history only

**Safety Philosophy:**
- Migrations are **additive and idempotent** by design
- Use `ensureColumn()`, `ensureIndex()`, `copyColumn()` for safe schema changes
- Breaking changes require multi-step migrations (add, migrate data, deprecate)
- Version numbers never decrease — rollbacks are new forward migrations
