---
title: Defining Tables
description: Tables are Zod schemas extended with a .db namespace for primary keys, constraints, foreign keys, auto-generated values, JSON encoding, soft deletes, and more.
---

A table definition is a **singleton value** created from a Zod schema. It is not
a class — it's passed to database methods for validation, normalization, schema
management, and convenient CRUD.

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
```

## The `.db` namespace

When you import `z` from `@b9g/zen`, it's already extended with the `.db`
namespace. The extension happens once when the module loads.

- `.db.primary()` — Primary key
- `.db.unique()` — Unique constraint
- `.db.index()` — Create an index
- `.db.auto()` — Auto-generate a value on insert (type-aware)
- `.db.references(table, as, {field?, reverseAs?, onDelete?})` — Foreign key with a resolved property name
- `.db.softDelete()` — Soft-delete timestamp field
- `.db.encode(fn)` / `.db.decode(fn)` — Custom storage codec
- `.db.type(columnType)` — Explicit column type for DDL generation

If you need to extend a separate Zod instance, use `extendZod(z)`:

```typescript
import {z} from "zod";
import {extendZod} from "@b9g/zen";
extendZod(z);
// .db is now available on all Zod types
```

## Zod to database behavior

| Zod Method | Effect |
|------------|--------|
| `.optional()` | Column allows `NULL`; field omittable on insert |
| `.nullable()` | Column allows `NULL`; must explicitly pass `null` or value |
| `.string().max(n)` | `VARCHAR(n)` in DDL (if n ≤ 255) |
| `.string().uuid()` | Used by `.db.auto()` to generate UUIDs |
| `.number().int()` | `INTEGER` column type |
| `.date()` | `TIMESTAMPTZ` / `DATETIME` / `TEXT` depending on dialect |
| `.object()` / `.array()` | Stored as JSON, auto-encoded/decoded |
| `.default()` | **Throws** — use `.db.inserted()` instead |

## Auto-generated values

The `.db.auto()` modifier generates values on insert based on the field type:

| Type | Behavior |
|------|----------|
| `z.string().uuid()` | Generates a UUID via `crypto.randomUUID()` |
| `z.number().int()` | Auto-increment (database-side) |
| `z.date()` | Current timestamp via `NOW` |

```typescript
const Users = table("users", {
  id: z.string().uuid().db.primary().db.auto(),  // UUID generated on insert
  name: z.string(),
  createdAt: z.date().db.auto(),                 // NOW on insert
});

// id and createdAt are optional — auto-generated if not provided
const user = await db.insert(Users, {name: "Alice"});
user.id;        // "550e8400-e29b-41d4-a716-446655440000"
user.createdAt; // 2024-01-15T10:30:00.000Z
```

## Write-time defaults

`.db.inserted()`, `.db.updated()`, and `.db.upserted()` set default values for
write operations. They accept JS functions (run client-side) or SQL builtins
(`NOW`, `TODAY`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, run
database-side):

```typescript
import {z, table, NOW} from "@b9g/zen";

const Posts = table("posts", {
  id: z.string().uuid().db.primary().db.auto(),
  title: z.string(),
  slug: z.string().db.inserted(() => generateSlug()),   // JS function
  createdAt: z.date().db.inserted(NOW),                 // SQL builtin
  updatedAt: z.date().db.upserted(NOW),                 // insert AND update
  viewCount: z.number().db.inserted(() => 0).db.updated(() => 0),
});
```

| Method | When applied | Field becomes optional for |
|--------|--------------|---------------------------|
| `.db.inserted(value)` | INSERT only | insert |
| `.db.updated(value)` | UPDATE only | update |
| `.db.upserted(value)` | INSERT and UPDATE | insert and update |

> **Why not Zod's `.default()`?** Zod's `.default()` applies at *parse time*,
> not *write time* — defaults would be applied when reading data, not inserting.
> ZenDB throws if you use `.default()`. Use `.db.inserted()` instead.

## JSON encoding

Objects and arrays are automatically serialized to JSON on write and parsed
back on read:

```typescript
const Settings = table("settings", {
  id: z.string().uuid().db.primary().db.auto(),
  config: z.object({theme: z.string(), fontSize: z.number()}),
  tags: z.array(z.string()),
});

const settings = await db.insert(Settings, {
  config: {theme: "dark", fontSize: 14},
  tags: ["admin", "premium"],
});

settings.config.theme; // "dark" (object, not string)
settings.tags[0];      // "admin" (array, not string)
```

Override the automatic codec with `.db.encode()` / `.db.decode()`. When your
codec changes the storage type, add `.db.type()` so DDL generation is correct:

```typescript
const Articles = table("articles", {
  id: z.string().db.primary(),
  tags: z.array(z.string())
    .db.encode((arr) => arr.join(","))
    .db.decode((str: string) => str.split(","))
    .db.type("TEXT"), // required: explicit column type
});
```

## Soft delete

```typescript
const Users = table("users", {
  id: z.string().uuid().db.primary().db.auto(),
  name: z.string(),
  deletedAt: z.date().nullable().db.softDelete(),
});

await db.softDelete(Users, "u1"); // sets deletedAt
await db.delete(Users, "u1");     // hard delete

// Filter out soft-deleted rows
const active = await db.all(Users)`WHERE NOT ${Users.deleted()}`;

// Or use the auto-generated .active view
const active2 = await db.all(Users.active)``;
```

## Table options

Compound indexes and unique constraints:

```typescript
const Posts = table("posts", {
  id: z.string().db.primary(),
  authorId: z.string(),
  slug: z.string(),
  createdAt: z.date(),
}, {
  indexes: [["authorId", "createdAt"]],
  unique: [["authorId", "slug"]],
});
```

Compound foreign keys for composite primary keys:

```typescript
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

## Derived properties

Pure, client-side transformations computed lazily on access:

```typescript
const Posts = table("posts", {
  id: z.string().db.primary(),
  title: z.string(),
  authorId: z.string().db.references(Users, "author", {reverseAs: "posts"}),
}, {
  derive: {
    titleUpper: (post) => post.title.toUpperCase(),
    tags: (post) => post.postTags?.map(pt => pt.tag?.name) ?? [],
  }
});

type Post = Row<typeof Posts>; // includes id, title, authorId, titleUpper, tags
```

Derived properties are lazy getters, non-enumerable (hidden from `Object.keys()`
and `JSON.stringify()`), must be pure, and are fully typed via `Row<T>`.

## Partial selects

`pick()` creates a projection with a subset of fields:

```typescript
const UserSummaries = Users.pick("id", "name");

const posts = await db.all([Posts, UserSummaries])`
  JOIN ${UserSummaries} ON ${UserSummaries.on(Posts)}
`;
// posts[0].author has only id and name
```

## Field metadata

Tables expose metadata for introspection and form generation:

```typescript
const fields = Users.fields();

fields.email.name;          // "email"
fields.email.schema;        // ZodString — use Zod APIs (isOptional(), etc.)
fields.email.db;            // FieldDBMeta object
fields.email.db.unique;     // true

Users.meta.primary;         // "id" (field name)
Users.primary;              // SQLTemplate: "users"."id"
Posts.meta.references;      // [{fieldName: "authorId", table: Users, as: "author"}]

// Relation navigation (forward and reverse)
Posts.relations().author.table;    // Users table
Users.relations().posts.table;     // Posts table (if reverseAs: "posts" defined)
```
