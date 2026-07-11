---
title: Migrations
description: IndexedDB-style, event-based, forward-only migrations with idempotent helpers that encourage safe, additive-only schema changes.
---

ZenDB uses IndexedDB-style migrations. Schema changes are explicit, sequential,
and **forward-only** — there are no down migrations.

```typescript
db.addEventListener("upgradeneeded", (e) => {
  e.waitUntil((async () => {
    if (e.oldVersion < 1) {
      await db.ensureTable(Users);
      await db.ensureTable(Posts);
    }
    if (e.oldVersion < 2) {
      // Add a new column — update the schema and call ensureTable again
      await db.ensureTable(Posts);
    }
    if (e.oldVersion < 3) {
      // Add constraints after data cleanup
      await db.ensureConstraints(Posts);
    }
  })());
});

await db.open(3); // opens at version 3, firing upgradeneeded if needed
```

## Migration rules

- Migrations run sequentially from `oldVersion + 1` to `newVersion`
- If a migration crashes, the version does not bump
- Keep migration code around indefinitely (forward-only, no down migrations)
- Multi-process safe via exclusive locking

> **Why EventTarget?** It's the web-standard pattern (like IndexedDB's
> `onupgradeneeded`). Third-party code can subscribe to lifecycle events without
> changing constructor signatures — enabling plugins for logging, tracing, and
> instrumentation.

## Safe migration helpers

ZenDB provides idempotent helpers that encourage additive-only changes. All of
them read from your table schema (the single source of truth) and are safe to
run multiple times.

```typescript
// Add a column — update the schema, then ensureTable
const Posts = table("posts", {
  id: z.string().db.primary(),
  title: z.string(),
  views: z.number().db.inserted(() => 0), // NEW
});

if (e.oldVersion < 2) {
  await db.ensureTable(Posts);
  // -> ALTER TABLE "posts" ADD COLUMN "views" REAL DEFAULT 0
}
```

```typescript
// Add an index — declare it in the schema, applied by ensureTable
const Posts = table("posts", {
  id: z.string().db.primary(),
  title: z.string().db.index(), // NEW
});

if (e.oldVersion < 3) {
  await db.ensureTable(Posts);
  // -> CREATE INDEX IF NOT EXISTS "idx_posts_title" ON "posts"("title")
}
```

```typescript
// Safe column rename (additive, non-destructive)
const Users = table("users", {
  id: z.string().db.primary(),
  email: z.string().email(),       // keep the old column
  emailAddress: z.string().email(), // NEW
});

if (e.oldVersion < 4) {
  await db.ensureTable(Users);                       // add emailAddress
  await db.copyColumn(Users, "email", "emailAddress"); // copy data across
  // Drop the old column later with raw SQL if you truly need to
}
```

### Helper methods

- `db.ensureTable(table)` — Idempotent CREATE TABLE / ADD COLUMN / CREATE INDEX
- `db.ensureView(view)` — Idempotent DROP + CREATE VIEW
- `db.ensureConstraints(table)` — Add unique / FK constraints (with preflight checks)
- `db.copyColumn(table, from, to)` — Copy data between columns (for safe renames)

## Destructive operations

Destructive helpers (`dropColumn`, `dropTable`, `renameColumn`) are **not
provided**. Write raw SQL if you truly need one:

```typescript
if (e.oldVersion < 5) {
  await db.exec`ALTER TABLE ${Users} DROP COLUMN deprecated_field`;
}
```

## Migration locking

If the driver provides `withMigrationLock()`, migrations run atomically:
PostgreSQL uses advisory locks, MySQL uses `GET_LOCK`, and SQLite uses exclusive
transactions. See [Drivers & Dialects](/guides/drivers-and-dialects/).
