# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2025-12-21

### Added

- Driver-level type encoding/decoding for dialect-specific handling
  - `encodeValue(value, fieldType)` and `decodeValue(value, fieldType)` methods on Driver interface
  - SQLite: Date→ISO string, boolean→1/0, JSON stringify/parse
  - MySQL: Date→"YYYY-MM-DD HH:MM:SS", boolean→1/0, JSON stringify/parse
  - PostgreSQL: Mostly passthrough (pg handles natively), JSON stringify
- `inferFieldType()` helper to infer field type from Zod schema
- Node.js tests for encode/decode functionality

### Changed

- **Breaking:** Removed deprecated `Infer<T>` type alias (use `Row<T>` instead)
- Renamed internal types for clarity:
  - `InferRefs` → `RowRefs`
  - `WithRefs` → `JoinedRow`

### Fixed

- Invalid datetime values now throw errors instead of returning Invalid Date

## [0.1.0] - 2025-12-20

Initial release of @b9g/zen - the simple database client.

### Features

- Table definitions with Zod schemas
- Explicit SQL queries with tagged templates
- Normalized object results from JOINs
- Multi-database support: SQLite, PostgreSQL, MySQL
- Bun.SQL driver with automatic dialect detection
- IndexedDB-style event-driven migrations
- Type-safe SQL fragment helpers (`set()`, `values()`, `on()`, `in()`)
- Forward and reverse relationship resolution
- Partial tables with `pick()` and SQL-computed fields with `derive()`
- DDL generation from schemas
- Form field metadata extraction
- Debugging tools (`db.print()`, `db.explain()`)
- Comprehensive error handling with typed errors

See README.md for complete documentation.
