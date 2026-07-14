---
title: Views
description: Read-only projections of tables with predefined WHERE clauses. Views share the query API, inherit relationships, and reject mutations.
---

Views are read-only projections of tables with a predefined `WHERE` clause. They
share the same query API as tables, but reject mutations.

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
```

Mutations against a view throw:

```typescript
await db.insert(AdminUsers, {/* ... */}); // ✗ Error
await db.update(AdminUsers, {/* ... */}); // ✗ Error
await db.delete(AdminUsers, "u1");        // ✗ Error
```

## The auto-generated `.active` view

Tables with a `.db.softDelete()` field automatically get an `.active` view that
excludes soft-deleted rows:

```typescript
// Equivalent to: view("users_active", Users)`WHERE deletedAt IS NULL`
const activeUsers = await db.all(Users.active)``;
```

## Views preserve relationships

Views inherit references from their base table, so JOINs work identically:

```typescript
const posts = await db.all([Posts, AdminUsers])`
  JOIN "admin_users" ON ${AdminUsers.on(Posts)}
`;
posts[0].author?.role; // "admin"
```

Create or refresh a view's DDL with `db.ensureView(view)` — see
[Migrations](/guides/migrations/).
