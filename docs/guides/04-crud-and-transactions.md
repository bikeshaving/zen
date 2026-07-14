---
title: CRUD & Transactions
description: Insert, update, and delete with Zod validation and RETURNING support, and group writes into atomic transactions that roll back on error.
---

## CRUD helpers

```typescript
// Insert with Zod validation (uses RETURNING to get the actual row)
const user = await db.insert(Users, {
  email: "alice@example.com",
  name: "Alice",
});
// Returns the actual row from the DB, including auto-generated id and
// DB-computed defaults
const userId = user.id;

// Update by primary key (uses RETURNING)
const updated = await db.update(Users, {name: "Bob"}, userId);

// Delete by primary key
await db.delete(Users, userId);

// Soft delete (sets deletedAt timestamp; requires a softDelete() field)
await db.softDelete(Users, userId);
```

### RETURNING support

`insert()` and `update()` use `RETURNING *` on SQLite and PostgreSQL to return
the actual row from the database — including DB-computed defaults and trigger
output. MySQL falls back to a separate `SELECT`.

## Transactions

Wrap writes in a transaction; it commits on success and rolls back on any thrown
error:

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
  // Commits on success, rolls back on error
});
```

Transactions return values:

```typescript
const user = await db.transaction(async (tx) => {
  return await tx.insert(Users, {email: "bob@example.com", name: "Bob"});
});
```

The `tx` object exposes the same query and CRUD API as `db`. If any statement
throws, the whole transaction is rolled back automatically.

## Type inference

```typescript
import type {Row, Insert, Update} from "@b9g/zen";

type User = Row<typeof Users>;       // full row type (after read)
type NewUser = Insert<typeof Users>; // insert type (respects defaults/.db.auto())
type UserPatch = Update<typeof Users>; // update type (all fields optional)
```

See [Errors & Debugging](/guides/errors-and-debugging/) for how validation and
constraint violations surface as typed errors.
