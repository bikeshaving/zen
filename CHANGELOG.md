# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-XX

### Added

**Core Features:**
- Schema-driven database client using Zod schemas
- Multi-database support: SQLite, PostgreSQL, MySQL via pluggable drivers
- Bun.SQL driver with automatic dialect detection
- Standard Schema v1.0 support for validation library interoperability
- IndexedDB-style event-driven migrations with forward-only versioning
- Entity normalization with Apollo-style reference resolution
- Type-safe query fragments with tagged templates

**Table Definitions:**
- `table()` - Define tables from Zod schemas
- Field wrappers: `primary()`, `unique()`, `index()`, `references()`, `softDelete()`
- Compound indexes via table options
- `.pick()` for partial table selects
- Foreign key references with automatic property resolution

**Query API:**
- `db.all()` - Query with entity normalization
- `db.get()` - Single entity or by primary key
- `db.query()` - Raw queries without normalization
- `db.exec()` - Execute statements (DDL, DML)
- `db.val()` - Single value queries
- `db.transaction()` - Transaction support with automatic rollback

**CRUD Helpers:**
- `db.insert()` - Insert with validation and RETURNING support
- `db.update()` - Update with validation and RETURNING support
- `db.delete()` - Hard delete
- `db.softDelete()` - Soft delete with timestamp

**Fragment Helpers:**
- `Table.where()` - Type-safe WHERE conditions with operator DSL
- `Table.set()` - UPDATE SET clauses
- `Table.on()` - JOIN conditions from foreign key references
- `Table.values()` - Bulk INSERT values
- `Table.in()` - IN clause fragments
- `Table.deleted()` - Soft delete filters

**DDL Generation:**
- `Table.ddl()` - Dialect-aware CREATE TABLE generation
- `Table.ensureColumn()` - Idempotent ADD COLUMN (with MySQL caveats)
- `Table.ensureIndex()` - Idempotent CREATE INDEX
- `Table.copyColumn()` - Safe data migration between columns
- DDL fragments automatically transform based on driver dialect

**Migration Helpers:**
- Safe, additive-only migration helpers
- Forward-only versioning (no down migrations)
- Migration locking (PostgreSQL advisory locks, MySQL GET_LOCK, SQLite exclusive)
- `DatabaseUpgradeEvent` with `waitUntil()` for async migrations

**Debugging:**
- `db.print()` - Inspect generated SQL without executing
- `db.explain()` - Analyze query execution plans (driver-level)
- `Fragment.toString()` - Debug fragment composition
- Comprehensive error messages with dialect/table/helper context

**Error Handling:**
- `ZealotError` base class with typed error codes
- `ValidationError` - Schema validation failures with nested field paths
- `ConstraintViolationError` - Normalized database constraint errors (kind, constraint, table, column)
- `NotFoundError`, `AlreadyExistsError`, `QueryError`, `MigrationError`
- `ConnectionError`, `TransactionError`, `MigrationLockError`
- Errors propagate through transactions unmodified

**Type Safety:**
- Full TypeScript type inference for queries and inserts
- `Infer<Table>` - Row type extraction
- `Insert<Table>` - Insert payload type (optional/default fields excluded)
- Nominal type branding for `SQLFragment` vs `DDLFragment`

**Metadata System:**
- `setDBMeta()` / `getDBMeta()` - Public API for custom field wrappers
- Declarative metadata (read once at table creation time)
- Namespaced metadata to avoid user collisions
- Last-write-wins precedence for multiple `setDBMeta()` calls

### Design Philosophy

- **Not an ORM** - Thin wrapper over SQL, no hidden behavior
- **SQL transparency** - You write SQL, we handle parameters and normalization
- **Schema-driven** - One Zod schema defines storage, validation, and metadata
- **Forward-only migrations** - Monotonic versioning, additive changes only
- **Late dialect binding** - Fragments created without dialect, resolved at execution
- **No codegen** - Everything runtime-driven

### Known Limitations

- MySQL doesn't support `IF NOT EXISTS` for `ALTER TABLE ADD COLUMN` (may error on re-run)
- Soft deletes require manual index management to exclude deleted records
- No down-migrations by design (use forward migrations to "undo")
- No query builder chains (write SQL directly)
- No automatic schema introspection or migration generation

### Breaking Changes

This is the first release (0.1.0), so there are no breaking changes yet.

Future breaking changes before 1.0.0 may occur without major version bumps.

### Migration from Pre-Release

If upgrading from unreleased versions:
- `generateDDL(table)` → `table.ddl()`
- `setDbMeta()` / `getDbMeta()` → `setDBMeta()` / `getDBMeta()` (ACROCase)
- DDL helpers now return `DDLFragment` instead of `string`
- No need to pass `dialect` parameter to DDL helpers (auto-detected)

[0.1.0]: https://github.com/b9g/zealot/releases/tag/v0.1.0
