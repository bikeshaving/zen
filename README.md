# ZenDB

Define Zod tables. Write raw SQL. Get typed objects.

[Website](https://zendb.org) · [GitHub](https://github.com/bikeshaving/zen) · [npm](https://www.npmjs.com/package/@b9g/zen)

## Installation

```bash
npm install @b9g/zen zod

# In Node, install a driver (choose one):
npm install better-sqlite3  # for SQLite
npm install postgres        # for PostgreSQL
npm install mysql2          # for MySQL

# Bun has a driver which uses Bun.SQL
bun install @b9g/zen zod
```

```typescript
import {Database} from "@b9g/zen";
import BunDriver from "@b9g/zen/bun";
import SQLiteDriver from "@b9g/zen/sqlite";
import PostgresDriver from "@b9g/zen/postgres";
import MySQLDriver from "@b9g/zen/mysql";

// Each driver implements the `Driver` interface
// and is a separate module in the package

const sqliteDriver = new SQLiteDriver("file:app.db");
const postgresDriver = new PostgresDriver("postgresql://localhost/mydb");
const mySQLDriver = new MySQLDriver("mysql://localhost/mydb");

// Bun auto-detects dialect from the connection URL.
const bunDriver = new BunDriver("sqlite://app.db");

const db = new Database(bunDriver);
```

## Quick Start

```typescript
import {z, table, Database} from "@b9g/zen";
import SQLiteDriver from "@b9g/zen/sqlite";

const driver = new SQLiteDriver("file:app.db");

// 1. Define tables
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

// 2. Create database with migrations
const db = new Database(driver);

db.addEventListener("upgradeneeded", (e) => {
  e.waitUntil((async () => {
    if (e.oldVersion < 1) {
      // Create tables (includes PK/unique/FK on new tables)
      await db.ensureTable(Users);
      await db.ensureTable(Posts);
      // For existing tables when adding FKs/uniques later, call ensureConstraints()
    }
    if (e.oldVersion < 2) {
      // Evolve schema: add avatar column (safe, additive)
      Users = table("users", {
        id: z.string().uuid().db.primary().db.auto(),
        email: z.string().email().db.unique(),
        name: z.string(),
        avatar: z.string().optional(),  // new field
      });
      await db.ensureTable(Users);          // adds missing columns/indexes
      await db.ensureConstraints(Users);    // applies new constraints on existing data
    }
  })());
});

await db.open(2);

// 3. Insert with validation (id auto-generated)
const user = await db.insert(Users, {
  email: "alice@example.com",
  name: "Alice",
});

// 4. Query with normalization
const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
  WHERE ${Posts.cols.published} = ${true}
`;

const author = posts[0].author;
author?.name; // "Alice" — resolved from JOIN
author === posts[1].author; // true — same instance

// 5. Get by primary key
const post = await db.get(Posts, posts[0].id);

// 6. Update
await db.update(Users, {name: "Alice Smith"}, user.id);
```

## Why Zen?

Zen is the missing link between SQL and typed data. By writing tables with Zod schema, you get idempotent migration helpers, typed CRUD, normalized object references, and many features other database clients cannot provide.

### What Zen is not:
- **Zen is not a query builder** — Rather than building SQL with fluent chains `.where().orderBy().limit()`, you write it directly with templates: `` db.all(Posts)`WHERE published = ${true} ORDER BY created_at DESC LIMIT 20` `` Helper functions help you write the tedious parts of SQL without hiding it or limiting your queries.
- **Zen is not an ORM** — Tables are not classes. They are Zod-powered singletons which provide schema-aware utilities. These tables can be used to validate writes, generate DDL, and deduplicate joined data.
- **Zen is not a startup** — Zen is an open-source library, not a venture-backed SaaS. There will never be a managed “ZenDB” instance or a “Zen Studio.” The library is a thin wrapper around Zod and JavaScript SQL drivers, with a focus on runtime abstractions rather than complicated tooling.

### Safety

- **No lazy loading** — Related data comes from your JOINs
- **No ORM identity map** — Normalization is per-query, not session-wide
- **No down migrations** — Forward-only versioning (1 → 2 → 3)
- **No destructive helpers** — No `dropColumn()`, `dropTable()`, `renameColumn()`
- **No automatic migrations** — Schema changes are explicit in upgrade events

## Table Definitions

```typescript
import {z, table} from "@b9g/zen";
import type {Row} from "@b9g/zen";

const Users = table("users", {
  id: z.string().uuid().db.primary().db.auto(),
  email: z.string().email().db.unique(),
  name: z.string().max(100),
  role: z.enum(["user", "admin"]).db.inserted(() => "user"),
  createdAt: z.date().db.auto(),
});

const Posts = table("posts", {
  id: z.string().uuid().db.primary().db.auto(),
  title: z.string(),
  content: z.string().optional(),
  authorId: z.string().uuid().db.references(Users, "author", {onDelete: "cascade"}),
  published: z.boolean().db.inserted(() => false),
});
```

**Zod to Database Behavior:**

| Zod Method | Effect |
|------------|--------|
| `.optional()` | Column allows `NULL`; field omittable on insert |
| `.nullable()` | Column allows `NULL`; must explicitly pass `null` or value |
| `.string().max(n)` | `VARCHAR(n)` in DDL (if n ≤ 255) |
| `.string().uuid()` | Used by `.db.auto()` to generate UUIDs |
| `.number().int()` | `INTEGER` column type |
| `.date()` | `TIMESTAMPTZ` / `DATETIME` / `TEXT` depending on dialect |
| `.object()` / `.array()` | Stored as JSON, auto-encoded/decoded |
| `.default()` | **Throws error** — use `.db.inserted()` instead |

**The `.db` namespace:**

The `.db` property is available on all Zod types imported from `@b9g/zen`. It provides database-specific modifiers:

- `.db.primary()` — Primary key
- `.db.unique()` — Unique constraint
- `.db.index()` — Create an index
- `.db.auto()` — Auto-generate value on insert (type-aware)
- `.db.references(table, as, {field?, reverseAs?, onDelete?})` — Foreign key with resolved property name
- `.db.softDelete()` — Soft delete timestamp field
- `.db.encode(fn)` — Custom encoding for database storage
- `.db.decode(fn)` — Custom decoding from database storage
- `.db.type(columnType)` — Explicit column type for DDL generation

**How does `.db` work?** When you import `z` from `@b9g/zen`, it's already extended with the `.db` namespace. The extension happens once when the module loads. If you need to extend a separate Zod instance, use `extendZod(z)`.

```typescript
import {z} from "zod";
import {extendZod} from "@b9g/zen";
extendZod(z);
// .db is available on all Zod types
```

**Auto-generated values with `.db.auto()`:**

The `.db.auto()` modifier auto-generates values on insert based on the field type:

| Type | Behavior |
|------|----------|
| `z.string().uuid()` | Generates UUID via `crypto.randomUUID()` |
| `z.number().int()` | Auto-increment (database-side) |
| `z.date()` | Current timestamp via `NOW` |

```typescript
const Users = table("users", {
  id: z.string().uuid().db.primary().db.auto(),  // UUID generated on insert
  name: z.string(),
  createdAt: z.date().db.auto(),                 // NOW on insert
});

// id and createdAt are optional - auto-generated if not provided
const user = await db.insert(Users, {name: "Alice"});
user.id;        // "550e8400-e29b-41d4-a716-446655440000"
user.createdAt; // 2024-01-15T10:30:00.000Z
```

**Default values with `.db.inserted()`, `.db.updated()`, `.db.upserted()`:**

These methods set default values for write operations. They accept JS functions or SQL builtins (`NOW`, `TODAY`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`):

```typescript
import {z, table, NOW} from "@b9g/zen";

const Posts = table("posts", {
  id: z.string().uuid().db.primary().db.auto(),
  title: z.string(),
  // JS function — runs client-side
  slug: z.string().db.inserted(() => generateSlug()),
  // SQL builtin — runs database-side
  createdAt: z.date().db.inserted(NOW),
  updatedAt: z.date().db.upserted(NOW),  // set on insert AND update
  viewCount: z.number().db.inserted(() => 0).db.updated(() => 0), // reset on update
});
```

| Method | When applied | Field becomes optional for |
|--------|--------------|---------------------------|
| `.db.inserted(value)` | INSERT only | insert |
| `.db.updated(value)` | UPDATE only | update |
| `.db.upserted(value)` | INSERT and UPDATE | insert and update |

**Why not Zod's `.default()`?** Zod's `.default()` applies at *parse time*, not *write time*. This means defaults would be applied when reading data, not when inserting. Zen throws an error if you use `.default()` — use `.db.inserted()` instead.

**Automatic JSON encoding/decoding:**

Objects (`z.object()`) and arrays (`z.array()`) are automatically serialized to JSON when stored and parsed back when read:

```typescript
const Settings = table("settings", {
  id: z.string().uuid().db.primary().db.auto(),
  config: z.object({theme: z.string(), fontSize: z.number()}),
  tags: z.array(z.string()),
});

// On insert: config and tags are JSON.stringify'd
const settings = await db.insert(Settings, {
  config: {theme: "dark", fontSize: 14},
  tags: ["admin", "premium"],
});
// Stored as: config='{"theme":"dark","fontSize":14}', tags='["admin","premium"]'

// On read: JSON strings are parsed back to objects/arrays
settings.config.theme; // "dark" (object, not string)
settings.tags[0];      // "admin" (array, not string)
```

**Custom encoding/decoding:**

Override automatic JSON encoding with custom transformations:

```typescript
const Custom = table("custom", {
  id: z.string().db.primary(),
  // Store array as CSV instead of JSON
  tags: z.array(z.string())
    .db.encode((arr) => arr.join(","))
    .db.decode((str: string) => str.split(","))
    .db.type("TEXT"), // Required: explicit column type for DDL
});

await db.insert(Custom, {id: "c1", tags: ["a", "b", "c"]});
// Stored as: tags='a,b,c' (not '["a","b","c"]')
```

**Explicit column types:**

When using custom encode/decode that transforms the storage type (e.g., array → CSV string), use `.db.type()` to specify the correct column type for DDL generation:

| Scenario | Column Type |
|----------|-------------|
| `z.object()` / `z.array()` (no codec) | JSON/JSONB (automatic) |
| `z.object()` / `z.array()` + encode only | JSON/JSONB (advanced use) |
| `z.object()` / `z.array()` + encode + decode | **Explicit `.db.type()` required** |

Without `.db.type()`, DDL generation would incorrectly use JSONB for a field that's actually stored as TEXT.

**Soft delete:**
```typescript
const Users = table("users", {
  id: z.string().uuid().db.primary().db.auto(),
  name: z.string(),
  deletedAt: z.date().nullable().db.softDelete(),
});

const userId = "u1";

// Soft delete a record (sets deletedAt to current timestamp)
await db.softDelete(Users, userId);

// Hard delete (permanent removal)
await db.delete(Users, userId);

// Filter out soft-deleted records in queries
const activeUsers = await db.all(Users)`
  WHERE NOT ${Users.deleted()}
`;

// Or use the .active view (auto-generated, read-only)
const activeUsers = await db.all(Users.active)``;

// JOINs with .active automatically filter deleted rows
const posts = await db.all([Posts, Users.active])`
  JOIN "users_active" ON ${Users.active.cols.id} = ${Posts.cols.authorId}
`;
```

**Compound indexes** via table options:
```typescript
const Posts = table("posts", {
  id: z.string().db.primary(),
  authorId: z.string(),
  createdAt: z.date(),
}, {
  indexes: [["authorId", "createdAt"]],
});
```

**Compound foreign keys** for composite primary keys:
```typescript
const OrderProducts = table("order_products", {
  orderId: z.string().uuid(),
  productId: z.string().uuid(),
  // ... compound primary key
});

const OrderItems = table("order_items", {
  id: z.string().uuid().db.primary(),
  orderId: z.string().uuid(),
  productId: z.string().uuid(),
  quantity: z.number(),
}, {
  references: [{
    fields: ["orderId", "productId"],
    table: OrderProducts,
    as: "orderProduct",
  }],
});
```

**Derived properties** for client-side transformations:
```typescript
const Posts = table("posts", {
  id: z.string().db.primary(),
  title: z.string(),
  authorId: z.string().db.references(Users, "author", {
    reverseAs: "posts"
  }),
}, {
  derive: {
    // Pure functions only (no I/O, no side effects)
    titleUpper: (post) => post.title.toUpperCase(),
    // Traverse relationships (requires JOIN in query)
    tags: (post) => post.postTags?.map(pt => pt.tag?.name) ?? [],
  }
});

type Post = Row<typeof Posts>;
// Post includes: id, title, authorId, titleUpper, tags

const posts = await db.all([Posts, Users, PostTags, Tags])`
  JOIN "users" ON ${Users.on(Posts)}
  LEFT JOIN "post_tags" ON ${PostTags.cols.postId} = ${Posts.cols.id}
  LEFT JOIN "tags" ON ${Tags.on(PostTags)}
`;

const post = posts[0];
post.titleUpper;  // "HELLO WORLD" — typed as string
post.tags;        // ["javascript", "typescript"] — traverses relationships
Object.keys(post);  // ["id", "title", "authorId", "author"] (no derived props)
JSON.stringify(post);  // Excludes derived properties (non-enumerable)
```

Derived properties:
- Are lazy getters (computed on access, not stored)
- Are non-enumerable (hidden from `Object.keys()` and `JSON.stringify()`)
- Must be pure functions (no I/O, no database queries)
- Can traverse resolved relationships from the same query
- Are fully typed via `Row<T>` inference

**Partial selects** with `pick()`:
```typescript
const UserSummary = Users.pick("id", "name");
const posts = await db.all([Posts, UserSummary])`
  JOIN "users" ON ${UserSummary.on(Posts)}
`;
// posts[0].author has only id and name
```

**Table identity**: A table definition is a singleton value which is passed to database methods for validation, normalization, schema management, and convenient CRUD operations. It is not a class.

## Views

Views are read-only projections of tables with predefined WHERE clauses:

```typescript
import {z, table, view} from "@b9g/zen";

const Users = table("users", {
  id: z.string().db.primary(),
  name: z.string(),
  role: z.enum(["user", "admin"]),
  deletedAt: z.date().nullable().db.softDelete(),
});

// Define views with explicit names
const ActiveUsers = view("active_users", Users)`
  WHERE ${Users.cols.deletedAt} IS NULL
`;

const AdminUsers = view("admin_users", Users)`
  WHERE ${Users.cols.role} = ${"admin"}
`;

// Query from views (same API as tables)
const admins = await db.all(AdminUsers)``;
const admin = await db.get(AdminUsers, "u1");

// Views are read-only — mutations throw errors
await db.insert(AdminUsers, {...});  // ✗ Error
await db.update(AdminUsers, {...});  // ✗ Error
await db.delete(AdminUsers, "u1");   // ✗ Error
```

**Auto-generated `.active` view:** Tables with a `.db.softDelete()` field automatically get an `.active` view:

```typescript
// Equivalent to: view("users_active", Users)`WHERE deletedAt IS NULL`
const activeUsers = await db.all(Users.active)``;
```

**Views preserve table relationships:** Views inherit references from their base table, so JOINs work identically:

```typescript
const posts = await db.all([Posts, AdminUsers])`
  JOIN "admin_users" ON ${AdminUsers.on(Posts)}
`;
posts[0].author?.role; // "admin"
```

## Queries

Tagged templates with automatic parameterization:

```typescript
const title = "Hello";
const postId = "p1";
const userId = "u1";

// Single table query
const posts = await db.all(Posts)`WHERE published = ${true}`;

// Multi-table with joins — pass array
const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
  WHERE ${Posts.cols.published} = ${true}
`;

// Get single entity
const post = await db.get(Posts)`WHERE ${Posts.cols.title} = ${title}`;

// Get by primary key (convenience)
const post = await db.get(Posts, postId);

// Raw queries (no normalization)
const counts = await db.query<{count: number}>`
  SELECT COUNT(*) as count FROM ${Posts} WHERE ${Posts.cols.authorId} = ${userId}
`;

// Execute statements
await db.exec`CREATE INDEX idx_posts_author ON ${Posts}(${Posts.cols.authorId})`;

// Single value
const count = await db.val<number>`SELECT COUNT(*) FROM ${Posts}`;
```

## CRUD Helpers
```typescript
// Insert with Zod validation (uses RETURNING to get actual row)
const user = await db.insert(Users, {
  email: "alice@example.com",
  name: "Alice",
});
// Returns actual row from DB, including auto-generated id and DB-computed defaults
const userId = user.id;

// Update by primary key (uses RETURNING)
const updated = await db.update(Users, {name: "Bob"}, userId);

// Delete by primary key
await db.delete(Users, userId);

// Soft delete (sets deletedAt timestamp, requires softDelete() field)
await db.softDelete(Users, userId);
```

**RETURNING support:** `insert()` and `update()` use `RETURNING *` on SQLite and PostgreSQL to return the actual row from the database, including DB-computed defaults and triggers. MySQL falls back to a separate SELECT.


## Fragment Helpers

Type-safe SQL fragments as methods on Table objects:

```typescript
const postId = "p1";
const rows = [
  {id: "p1", title: "Hello", published: true},
  {id: "p2", title: "World", published: false},
];

// UPDATE with set()
await db.exec`
  UPDATE ${Posts}
  SET ${Posts.set({title: "New Title", published: true})}
  WHERE ${Posts.cols.id} = ${postId}
`;
// → UPDATE "posts" SET "title" = ?, "published" = ? WHERE "posts"."id" = ?

// JOIN with on()
const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
  WHERE ${Posts.cols.published} = ${true}
`;
// → JOIN "users" ON "users"."id" = "posts"."authorId"

// Bulk INSERT with values()
await db.exec`
  INSERT INTO ${Posts} ${Posts.values(rows)}
`;
// → INSERT INTO "posts" ("id", "title", "published") VALUES (?, ?, ?), (?, ?, ?)

// Qualified column names with cols
const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
  ORDER BY ${Posts.cols.title} DESC
`;
// → ORDER BY "posts"."title" DESC

// Safe IN clause with in()
const postIds = ["id1", "id2", "id3"];
const posts = await db.all(Posts)`WHERE ${Posts.in("id", postIds)}`;
// → WHERE "posts"."id" IN (?, ?, ?)

// Empty arrays handled correctly
const posts = await db.all(Posts)`WHERE ${Posts.in("id", [])}`;
// → WHERE 1 = 0
```

## Transactions

```typescript
await db.transaction(async (tx) => {
  const user = await tx.insert(Users, {
    email: "alice@example.com",
    name: "Alice",
  });
  await tx.insert(Posts, {
    authorId: user.id,
    title: "Hello",
    published: true,
  });
  // Commits on success, rollbacks on error
});

// Returns values
const user = await db.transaction(async (tx) => {
  return await tx.insert(Users, {
    email: "bob@example.com",
    name: "Bob",
  });
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
      await db.exec`${Posts.ensureIndex(["title"])}`;
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

zen provides idempotent helpers that encourage safe, additive-only migrations:

```typescript
// Add a new column (reads from schema)
const Posts = table("posts", {
  id: z.string().db.primary(),
  title: z.string(),
  views: z.number().db.inserted(() => 0), // NEW - add to schema
});

if (e.oldVersion < 2) {
  await db.exec`${Posts.ensureColumn("views")}`;
}
// → ALTER TABLE "posts" ADD COLUMN IF NOT EXISTS "views" REAL DEFAULT 0

// Add an index
if (e.oldVersion < 3) {
  await db.exec`${Posts.ensureIndex(["title", "views"])}`;
}
// → CREATE INDEX IF NOT EXISTS "idx_posts_title_views" ON "posts"("title", "views")

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
  await db.exec`ALTER TABLE ${Users} DROP COLUMN deprecated_field`;
}
```

**Dialect support:**

### Default column types

| Feature | SQLite | PostgreSQL | MySQL |
|---------|--------|------------|-------|
| Date type | TEXT | TIMESTAMPTZ | DATETIME |
| Date default | CURRENT_TIMESTAMP | NOW() | CURRENT_TIMESTAMP |
| Boolean | INTEGER | BOOLEAN | BOOLEAN |
| JSON | TEXT | JSONB | TEXT |
| Quoting | "double" | "double" | \`backtick\` |

## Entity Normalization

Normalization is driven by table metadata, not query shape — SQL stays unrestricted.

The `all()`/`get()` methods:
1. Generate SELECT with prefixed column aliases (`posts.id AS "posts.id"`)
2. Parse rows into per-table entities
3. Deduplicate by primary key (same PK = same object instance)
4. Resolve `references()` to actual entity objects (forward and reverse)

**Typed relationships:** When you pass multiple tables to `db.all([Posts, Users])`, the return type includes optional relationship properties based on your `references()` declarations. They can be `null` when the foreign key is missing or the JOIN yields no row, so use optional chaining.

### Forward References (belongs-to)

```typescript
const Posts = table("posts", {
  id: z.string().db.primary(),
  authorId: z.string().db.references(Users, "author"),
  title: z.string(),
});

const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
`;
posts[0].author?.name; // typed as string | undefined
```

### Reverse References (has-many)

Use `reverseAs` to populate arrays of referencing entities:

```typescript
const Posts = table("posts", {
  id: z.string().db.primary(),
  authorId: z.string().db.references(Users, "author", {
    reverseAs: "posts" // Populate author.posts = Post[]
  }),
  title: z.string(),
});

const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
`;
posts[0].author?.posts; // [{id: "p1", ...}, {id: "p2", ...}]
```

**Note:** Reverse relationships are runtime-only materializations that reflect data in the current query result set. No automatic JOINs, lazy loading, or cascade fetching.

### Many-to-Many

```typescript
const Posts = table("posts", {
  id: z.string().db.primary(),
  title: z.string(),
});

const Tags = table("tags", {
  id: z.string().db.primary(),
  name: z.string(),
});

const PostTags = table("post_tags", {
  id: z.string().db.primary(),
  postId: z.string().db.references(Posts, "post", {reverseAs: "postTags"}),
  tagId: z.string().db.references(Tags, "tag", {reverseAs: "postTags"}),
});

const postId = "p1";

const results = await db.all([PostTags, Posts, Tags])`
  JOIN "posts" ON ${Posts.on(PostTags)}
  JOIN "tags" ON ${Tags.on(PostTags)}
  WHERE ${Posts.cols.id} = ${postId}
`;

// Access through join table:
const tags = results.map((pt) => pt.tag);

// Or access via reverse relationship:
const post = results[0].post;
post?.postTags?.forEach((pt) => console.log(pt.tag?.name));
```

### Serialization Rules

References and derived properties have specific serialization behavior to prevent circular JSON and distinguish stored vs computed data:

```typescript
const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
`;

const post = posts[0];

// Forward references (belongs-to): enumerable and immutable
Object.keys(post);    // ["id", "title", "authorId", "author"]
JSON.stringify(post); // Includes "author"

// Reverse references (has-many): non-enumerable and immutable
const author = post.author;
if (author) {
  Object.keys(author);    // ["id", "name"] (no "posts")
  JSON.stringify(author); // Excludes "posts" (prevents circular JSON)
  author.posts;           // Accessible (just hidden from enumeration)

// Circular references are safe:
JSON.stringify(post); // No error
// {
//   "id": "p1",
//   "title": "Hello",
//   "authorId": "u1",
//   "author": {"id": "u1", "name": "Alice"}  // No "posts" = no cycle
// }

  // Explicit inclusion when needed:
  const explicit = {...author, posts: author.posts};
  JSON.stringify(explicit);  // Now includes posts
}
```

**Why this design:**
- Forward refs are safe to serialize (no cycles by themselves)
- Reverse refs create cycles when paired with forward refs
- Non-enumerable reverse refs prevent accidental circular JSON errors
- Both are immutable to prevent confusion (these are query results, not mutable objects)
- Explicit spread syntax when you need reverse refs in output

## Type Inference

```typescript
type User = Row<typeof Users>;       // Full row type (after read)
type NewUser = Insert<typeof Users>; // Insert type (respects defaults/.db.auto())
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

const pkName = Users.primaryKey();    // "id" (field name)
const pkFragment = Users.primary;     // SQLTemplate: "users"."id"
const refs = Posts.references();      // [{fieldName: "authorId", table: Users, as: "author"}]
```

## Performance

- Tagged template queries are cached by template object identity (compiled once per call site)
- Normalization cost is O(rows) with hash maps per table
- Reference resolution is zero-cost after deduplication
- Zod validation happens on writes, never on reads.

## Driver Interface

Drivers implement a template-based interface where each method receives `(TemplateStringsArray, values[])` and builds SQL with native placeholders:

```typescript
interface Driver {
  // Query methods - build SQL with native placeholders (? or $1, $2, ...)
  all<T>(strings: TemplateStringsArray, values: unknown[]): Promise<T[]>;
  get<T>(strings: TemplateStringsArray, values: unknown[]): Promise<T | null>;
  run(strings: TemplateStringsArray, values: unknown[]): Promise<number>;
  val<T>(strings: TemplateStringsArray, values: unknown[]): Promise<T | null>;

  // Connection management
  close(): Promise<void>;
  transaction<T>(fn: (tx: Driver) => Promise<T>): Promise<T>;

  // Capabilities
  readonly supportsReturning: boolean;

  // Optional
  withMigrationLock?<T>(fn: () => Promise<T>): Promise<T>;
}
```

**Why templates?** Drivers receive raw template parts and build SQL with their native placeholder syntax (`?` for SQLite/MySQL, `$1, $2, ...` for PostgreSQL). No SQL parsing needed.

**`supportsReturning`**: Enables optimal paths for INSERT/UPDATE. SQLite and PostgreSQL use `RETURNING *`; MySQL falls back to a separate SELECT.

**Migration locking**: If the driver provides `withMigrationLock()`, migrations run atomically (PostgreSQL uses advisory locks, MySQL uses `GET_LOCK`, SQLite uses exclusive transactions).

**Connection pooling**: Handled by the underlying driver. `postgres.js` and `mysql2` pool automatically; `better-sqlite3` uses a single connection (SQLite is single-writer anyway).

## Error Handling

All errors extend `DatabaseError` with typed error codes:

```typescript
import {
  DatabaseError,
  ValidationError,
  ConstraintViolationError,
  NotFoundError,
  isDatabaseError,
  hasErrorCode
} from "@b9g/zen";

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
- `EnsureError` — Schema ensure operation failed (operation, table, step)
- `SchemaDriftError` — Existing schema doesn't match definition (table, drift)
- `ConnectionError` / `TransactionError` — Connection/transaction issues

## Debugging

Inspect generated SQL and query plans:

```typescript
const userId = "u1";

// Print SQL without executing
const query = db.print`SELECT * FROM ${Posts} WHERE ${Posts.cols.published} = ${true}`;
console.log(query.sql);     // SELECT * FROM "posts" WHERE "posts"."published" = ?
console.log(query.params);  // [true]

// Inspect DDL generation
const ddl = db.print`${Posts.ddl()}`;
console.log(ddl.sql);  // CREATE TABLE IF NOT EXISTS "posts" (...)

// Analyze query execution plan
const plan = await db.explain`
  SELECT * FROM ${Posts}
  WHERE ${Posts.cols.authorId} = ${userId}
`;
console.log(plan);
// SQLite: [{ detail: "SEARCH posts USING INDEX idx_posts_authorId (authorId=?)" }]
// PostgreSQL: [{ "QUERY PLAN": "Index Scan using idx_posts_authorId on posts" }]

// Debug fragments
console.log(Posts.set({ title: "Updated" }).toString());
// SQLFragment { sql: "\"title\" = ?", params: ["Updated"] }

console.log(Posts.ddl().toString());
// DDLFragment { type: "create-table", table: "posts" }
```

## Dialect Support

| Feature | SQLite | PostgreSQL | MySQL |
|---------|--------|------------|-------|
| RETURNING | ✅ | ✅ | ⚠️ fallback |
| IF NOT EXISTS (CREATE TABLE) | ✅ | ✅ | ✅ |
| IF NOT EXISTS (ADD COLUMN) | ✅ | ✅ | ⚠️ may error |
| Migration Locks | BEGIN EXCLUSIVE | pg_advisory_lock | GET_LOCK |
| Advisory Locks | — | ✅ | ✅ |

### Zod to SQL Type Mapping

| Zod Type | SQLite | PostgreSQL | MySQL |
|----------|--------|------------|-------|
| `z.string()` | TEXT | TEXT | TEXT |
| `z.string().max(n)` (n ≤ 255) | TEXT | VARCHAR(n) | VARCHAR(n) |
| `z.number()` | REAL | DOUBLE PRECISION | REAL |
| `z.number().int()` | INTEGER | INTEGER | INTEGER |
| `z.boolean()` | INTEGER | BOOLEAN | BOOLEAN |
| `z.date()` | TEXT | TIMESTAMPTZ | DATETIME |
| `z.enum([...])` | TEXT | TEXT | TEXT |
| `z.object({...})` | TEXT | JSONB | TEXT |
| `z.array(...)` | TEXT | JSONB | TEXT |

Override with `.db.type("CUSTOM")` when using custom encode/decode.

## Public API Reference

### Core Exports

```typescript
import {
  // Zod (extended with .db namespace)
  z,                  // Re-exported Zod with .db already available

  // Table and view definition
  table,              // Create a table definition from Zod schema
  view,               // Create a read-only view from a table
  isTable,            // Type guard for Table objects
  isView,             // Type guard for View objects
  extendZod,          // Extend a separate Zod instance (advanced)

  // Database
  Database,           // Main database class
  Transaction,        // Transaction context (passed to transaction callbacks)
  DatabaseUpgradeEvent, // Event object for "upgradeneeded" handler

  // SQL builtins (for .db.inserted() / .db.updated())
  NOW,                // CURRENT_TIMESTAMP alias
  TODAY,              // CURRENT_DATE alias
  CURRENT_TIMESTAMP,  // SQL CURRENT_TIMESTAMP
  CURRENT_DATE,       // SQL CURRENT_DATE
  CURRENT_TIME,       // SQL CURRENT_TIME

  // Errors
  DatabaseError,        // Base error class
  ValidationError,    // Schema validation failed
  TableDefinitionError, // Invalid table definition
  MigrationError,     // Migration failed
  MigrationLockError, // Failed to acquire migration lock
  QueryError,         // SQL execution failed
  NotFoundError,      // Entity not found
  AlreadyExistsError, // Unique constraint violated
  ConstraintViolationError, // Database constraint violated
  ConnectionError,    // Connection failed
  TransactionError,   // Transaction failed
  isDatabaseError,      // Type guard for DatabaseError
  hasErrorCode,       // Check error code
} from "@b9g/zen";
```

### Types

```typescript
import type {
  // Table types
  Table,              // Table definition object
  PartialTable,       // Table created via .pick()
  DerivedTable,       // Table with derived fields via .derive()
  TableOptions,       // Options for table()
  ReferenceInfo,      // Foreign key reference metadata
  CompoundReference,  // Compound foreign key reference

  // Field types
  FieldMeta,          // Field metadata for form generation
  FieldType,          // Field type enum
  FieldDBMeta,        // Database-specific field metadata

  // Type inference
  Row,                // Infer row type from Table (after read)
  Insert,             // Infer insert type from Table (respects defaults/.db.auto())
  Update,             // Infer update type from Table (all fields optional)

  // Fragment types
  SetValues,          // Values accepted by Table.set()
  SQLTemplate,        // SQL template object (return type of set(), on(), etc.)
  SQLDialect,         // "sqlite" | "postgresql" | "mysql"

  // Driver types
  Driver,             // Driver interface for adapters
  TaggedQuery,        // Tagged template query function

  // Error types
  DatabaseErrorCode,  // Error code string literals
} from "@b9g/zen";
```

### Table Methods

```typescript
import {z, table} from "@b9g/zen";

const Users = table("users", {
  id: z.string().db.primary(),
  email: z.string().email(),
  emailAddress: z.string().email().optional(),
  deletedAt: z.date().nullable().db.softDelete(),
});

const Posts = table("posts", {
  id: z.string().db.primary(),
  authorId: z.string().db.references(Users, "author"),
  title: z.string(),
});

const rows = [{id: "u1", email: "alice@example.com", deletedAt: null}];

// DDL Generation
Users.ddl();                     // DDLFragment for CREATE TABLE
Users.ensureColumn("emailAddress"); // DDLFragment for ALTER TABLE ADD COLUMN
Users.ensureIndex(["email"]);    // DDLFragment for CREATE INDEX
Users.copyColumn("email", "emailAddress"); // SQLFragment for UPDATE (copy data)

// Query Fragments
Users.set({email: "alice@example.com"}); // SQLFragment for SET clause
Users.values(rows);                      // SQLFragment for INSERT VALUES
Users.on(Posts);                         // SQLFragment for JOIN ON (foreign key)
Users.in("id", ["u1"]);                  // SQLFragment for IN clause
Users.deleted();                         // SQLFragment for soft delete check

// Column References
Users.cols.email;              // SQLTemplate for qualified column
Users.primary;                 // SQLTemplate for primary key column

// Metadata
Users.name;                    // Table name string
Users.schema;                  // Zod schema
Users.meta;                    // Table metadata (primary, indexes, etc.)
Users.primaryKey();            // Primary key field name or null
Users.fields();                // Field metadata for form generation
Users.references();            // Foreign key references

// Derived Tables
Users.pick("id", "email");     // PartialTable with subset of fields
Users.derive("hasEmail", z.boolean())`
  ${Users.cols.email} IS NOT NULL
`;

// Views
Users.active;                  // View excluding soft-deleted rows (read-only)
```

### Database Methods

```typescript
import {z, table, Database} from "@b9g/zen";
import SQLiteDriver from "@b9g/zen/sqlite";

const Users = table("users", {
  id: z.string().db.primary(),
  email: z.string().email(),
});

const db = new Database(new SQLiteDriver("file:app.db"));

// Lifecycle
await db.open(1);
db.addEventListener("upgradeneeded", () => {});

// Query Methods (with normalization)
await db.all(Users)`WHERE ${Users.cols.email} = ${"alice@example.com"}`;
await db.get(Users)`WHERE ${Users.cols.id} = ${"u1"}`;
await db.get(Users, "u1");

// Raw Query Methods (no normalization)
await db.query<{count: number}>`SELECT COUNT(*) as count FROM ${Users}`;
await db.exec`CREATE INDEX idx_users_email ON ${Users}(${Users.cols.email})`;
await db.val<number>`SELECT COUNT(*) FROM ${Users}`;

// CRUD Helpers
await db.insert(Users, {id: "u1", email: "alice@example.com"});
await db.update(Users, {email: "alice2@example.com"}, "u1");
await db.delete(Users, "u1");

// Transactions
await db.transaction(async (tx) => {
  await tx.exec`SELECT 1`;
});

// Debugging
db.print`SELECT 1`;
await db.explain`SELECT * FROM ${Users}`;
```

### Driver Exports

```typescript
// Bun (built-in, auto-detects dialect)
import BunDriver from "@b9g/zen/bun";

// Node.js SQLite (better-sqlite3)
import SQLiteDriver from "@b9g/zen/sqlite";

// PostgreSQL (postgres.js)
import PostgresDriver from "@b9g/zen/postgres";

// MySQL (mysql2)
import MySQLDriver from "@b9g/zen/mysql";
```
