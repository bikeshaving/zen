---
title: API Reference
description: Every ZenDB export at a glance — core functions, types, table methods, and database methods.
---

A flat reference of the public API. For explanations and examples, follow the
links into the guides.

## Core exports

```typescript
import {
  // Zod (extended with .db namespace)
  z,                        // Re-exported Zod with .db already available
  extendZod,                // Extend a separate Zod instance (advanced)

  // Table and view definition
  table,                    // Create a table definition from a Zod schema
  view,                     // Create a read-only view from a table
  isTable,                  // Type guard for Table objects
  isView,                   // Type guard for View objects

  // Database
  Database,                 // Main database class
  Transaction,              // Transaction context (passed to transaction callbacks)
  DatabaseUpgradeEvent,     // Event object for the "upgradeneeded" handler

  // SQL builtins (for .db.inserted() / .db.updated())
  NOW,                      // CURRENT_TIMESTAMP alias
  TODAY,                    // CURRENT_DATE alias
  CURRENT_TIMESTAMP,
  CURRENT_DATE,
  CURRENT_TIME,

  // Errors
  DatabaseError,            // Base error class
  ValidationError,          // Schema validation failed
  TableDefinitionError,     // Invalid table definition
  MigrationError,           // Migration failed
  MigrationLockError,       // Failed to acquire migration lock
  QueryError,               // SQL execution failed
  NotFoundError,            // Entity not found
  AlreadyExistsError,       // Unique constraint violated
  ConstraintViolationError, // Database constraint violated
  ConnectionError,          // Connection failed
  TransactionError,         // Transaction failed
  isDatabaseError,          // Type guard for DatabaseError
  hasErrorCode,             // Check an error code
} from "@b9g/zen";
```

## Types

```typescript
import type {
  // Table types
  Table,             // Table definition object
  PartialTable,      // Table created via .pick()
  DerivedTable,      // Table with derived fields via .derive()
  TableOptions,      // Options for table()
  ReferenceInfo,     // Foreign key reference metadata
  CompoundReference, // Compound foreign key reference

  // Field types
  FieldMeta,         // Field metadata for introspection
  FieldDBMeta,       // Database-specific field metadata
  Relation,          // Relation navigator from table.relations()

  // Type inference
  Row,               // Infer row type from a Table (after read)
  Insert,            // Infer insert type (respects defaults / .db.auto())
  Update,            // Infer update type (all fields optional)

  // Fragment types
  SetValues,         // Values accepted by Table.set()
  SQLTemplate,       // SQL template object (return type of set(), on(), etc.)
  SQLDialect,        // "sqlite" | "postgresql" | "mysql"

  // Driver types
  Driver,            // Driver interface for adapters
  TaggedQuery,       // Tagged template query function

  // Error types
  DatabaseErrorCode, // Error code string literals
} from "@b9g/zen";
```

## Table methods

```typescript
// Query fragments
Users.set({email: "alice@example.com"}); // SQLFragment for a SET clause
Users.values(rows);                      // SQLFragment for INSERT VALUES
Users.on(Posts);                         // SQLFragment for a JOIN ON (foreign key)
Users.in("id", ["u1"]);                  // SQLFragment for an IN clause
Users.deleted();                         // SQLFragment for the soft-delete check

// Column references
Users.cols.email;              // SQLTemplate for a qualified column
Users.primary;                 // SQLTemplate for the primary key column

// Metadata
Users.name;                    // Table name string
Users.schema;                  // Zod schema
Users.meta;                    // Table metadata (primary, references, indexes, ...)
Users.fields();                // Field metadata for form generation
Users.relations();             // Forward and reverse relation navigators

// Derived tables
Users.pick("id", "email");     // PartialTable with a subset of fields
Users.derive("hasEmail", z.boolean())`${Users.cols.email} IS NOT NULL`;

// Views
Users.active;                  // View excluding soft-deleted rows (read-only)
```

## Database methods

```typescript
// Lifecycle
await db.open(1);
db.addEventListener("upgradeneeded", () => {});

// Query methods (with normalization)
await db.all(Users)`WHERE ${Users.cols.email} = ${"alice@example.com"}`;
await db.get(Users)`WHERE ${Users.cols.id} = ${"u1"}`;
await db.get(Users, "u1");

// Raw query methods (no normalization)
await db.query<{count: number}>`SELECT COUNT(*) as count FROM ${Users}`;
await db.exec`CREATE INDEX idx_users_email ON ${Users}(${Users.cols.email})`;
await db.val<number>`SELECT COUNT(*) FROM ${Users}`;

// CRUD helpers
await db.insert(Users, {id: "u1", email: "alice@example.com"});
await db.update(Users, {email: "alice2@example.com"}, "u1");
await db.delete(Users, "u1");
await db.softDelete(Users, "u1");

// Transactions
await db.transaction(async (tx) => {
  await tx.exec`SELECT 1`;
});

// Schema management
await db.ensureTable(Users);              // CREATE TABLE / ADD COLUMN / CREATE INDEX
await db.ensureView(AdminUsers);          // DROP + CREATE VIEW
await db.ensureConstraints(Users);        // Add unique / FK constraints
await db.copyColumn(Users, "old", "new"); // Copy data between columns

// Debugging
db.print`SELECT 1`;                       // Returns { sql, params } without executing
await db.explain`SELECT * FROM ${Users}`; // Returns the query execution plan
```
