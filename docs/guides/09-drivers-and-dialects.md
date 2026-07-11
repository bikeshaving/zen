---
title: Drivers & Dialects
description: A template-based driver interface across SQLite, PostgreSQL, and MySQL — with a per-dialect breakdown of types, RETURNING, and locking.
---

## The driver interface

Drivers implement a template-based interface. Each method receives
`(TemplateStringsArray, values[])` and builds SQL with the driver's native
placeholder syntax:

```typescript
interface Driver {
  // Query methods — build SQL with native placeholders (? or $1, $2, ...)
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

> **Why templates?** Drivers receive raw template parts and build SQL with their
> native placeholders (`?` for SQLite/MySQL, `$1, $2, …` for PostgreSQL). No SQL
> parsing is needed.

- **`supportsReturning`** enables optimal INSERT/UPDATE paths. SQLite and
  PostgreSQL use `RETURNING *`; MySQL falls back to a separate `SELECT`.
- **`withMigrationLock()`** makes migrations atomic. PostgreSQL uses advisory
  locks, MySQL uses `GET_LOCK`, SQLite uses exclusive transactions.
- **Connection pooling** is handled by the underlying driver. `postgres.js` and
  `mysql2` pool automatically; `better-sqlite3` uses a single connection (SQLite
  is single-writer anyway).

## Driver exports

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

## Dialect support

| Feature | SQLite | PostgreSQL | MySQL |
|---------|--------|------------|-------|
| RETURNING | ✅ | ✅ | ⚠️ fallback |
| IF NOT EXISTS (CREATE TABLE) | ✅ | ✅ | ✅ |
| IF NOT EXISTS (ADD COLUMN) | ✅ | ✅ | ⚠️ may error |
| Migration locks | BEGIN EXCLUSIVE | pg_advisory_lock | GET_LOCK |
| Advisory locks | — | ✅ | ✅ |

### Default column types

| Feature | SQLite | PostgreSQL | MySQL |
|---------|--------|------------|-------|
| Date type | TEXT | TIMESTAMPTZ | DATETIME |
| Date default | CURRENT_TIMESTAMP | NOW() | CURRENT_TIMESTAMP |
| Boolean | INTEGER | BOOLEAN | BOOLEAN |
| JSON | TEXT | JSONB | TEXT |
| Quoting | `"double"` | `"double"` | `` `backtick` `` |

### Zod to SQL type mapping

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

Override any of these with `.db.type("CUSTOM")` — see
[Defining Tables](/guides/defining-tables/).
