---
title: Errors & Debugging
description: Every error extends DatabaseError with typed error codes. Inspect generated SQL and query plans without executing.
---

## Error handling

All errors extend `DatabaseError` with typed error codes:

```typescript
import {
  DatabaseError,
  ValidationError,
  ConstraintViolationError,
  NotFoundError,
  isDatabaseError,
  hasErrorCode,
} from "@b9g/zen";

// Validation errors (Zod / Standard Schema)
try {
  await db.insert(Users, {email: "not-an-email"});
} catch (e) {
  if (hasErrorCode(e, "VALIDATION_ERROR")) {
    console.log(e.fieldErrors); // {email: ["Invalid email"]}
  }
}

// Constraint violations (database-level)
try {
  await db.insert(Users, {id: "1", email: "duplicate@example.com"});
} catch (e) {
  if (e instanceof ConstraintViolationError) {
    console.log(e.kind);       // "unique"
    console.log(e.constraint); // "users_email_unique"
    console.log(e.table);      // "users"
    console.log(e.column);     // "email"
  }
}

// Transaction errors are rolled back automatically
await db.transaction(async (tx) => {
  await tx.insert(Users, newUser);
  await tx.insert(Posts, newPost); // fails -> whole transaction rolled back
});
```

## Error types

- `ValidationError` — Schema validation failed (`fieldErrors`, nested paths)
- `ConstraintViolationError` — Database constraint violated (`kind`, `constraint`, `table`, `column`)
- `NotFoundError` — Entity not found (`tableName`, `id`)
- `AlreadyExistsError` — Unique constraint violated (`tableName`, `field`, `value`)
- `QueryError` — SQL execution failed (`sql`)
- `MigrationError` / `MigrationLockError` — Migration failures (`fromVersion`, `toVersion`)
- `EnsureError` — Schema ensure operation failed (`operation`, `table`, `step`)
- `SchemaDriftError` — Existing schema doesn't match definition (`table`, `drift`)
- `ConnectionError` / `TransactionError` — Connection / transaction issues

Use `isDatabaseError(e)` and `hasErrorCode(e, code)` as type guards.

## Debugging

Inspect generated SQL and query plans without executing:

```typescript
// Print SQL without executing
const query = db.print`SELECT * FROM ${Posts} WHERE ${Posts.cols.published} = ${true}`;
console.log(query.sql);    // SELECT * FROM "posts" WHERE "posts"."published" = ?
console.log(query.params); // [true]

// Debug fragments
console.log(Posts.set({title: "Updated"}).toString());
// SQLFragment { sql: "\"title\" = ?", params: ["Updated"] }

// Query execution plan
await db.explain`SELECT * FROM ${Posts}`;
```
