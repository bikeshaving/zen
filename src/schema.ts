/**
 * @b9g/zen/schema - Table definitions without the database runtime.
 *
 * A table is just a Zod schema plus metadata, which makes it useful well away
 * from a database connection: form validation, API request/response contracts,
 * and types shared between client and server.
 *
 * This entrypoint exposes exactly that surface and deliberately does not reach
 * `impl/database.js`, so importing it cannot pull `Database`, migrations, query
 * building, normalization or DDL generation into a client bundle. (Note that
 * the main entrypoint re-exports the SQL builtins *from* `impl/database.js`, so
 * they are re-exported here from `impl/builtins.js` instead — same symbols,
 * none of the runtime.)
 *
 * Import from "@b9g/zen" instead when you need to actually talk to a database.
 */

import {z as zod} from "zod";
import {extendZod} from "./impl/table.js";

// Extend zod on module load, exactly as the main entrypoint does. extendZod is
// idempotent, so importing both entrypoints is safe.
extendZod(zod);

// Re-export extended zod
export {zod as z};

// ============================================================================
// Table Definition
// ============================================================================

export {
	// Functions
	table,
	view,
	extendZod,

	// Type guards
	isTable,
	isView,

	// View helpers
	getViewMeta,

	// Table types
	type Table,
	type PartialTable,
	type DerivedTable,
	type View,
	type Queryable,
	type TableOptions,

	// Row types
	type Row,
	type Insert,
	type Update,
	type SetValues,

	// Field types (for form generation)
	type FieldMeta,
	type FieldType,
	type FieldDBMeta,

	// Reference types
	type Relation,
	type ReferenceInfo,
	type CompoundReference,

	// View types
	type ViewMeta,
} from "./impl/table.js";

// ============================================================================
// SQL Builtins
//
// Plain `Symbol.for()` values used by .db.inserted() / .db.updated(). They
// carry no runtime behind them, so a table using NOW() still round-trips
// through this entrypoint.
// ============================================================================

export {
	NOW,
	TODAY,
	CURRENT_TIMESTAMP,
	CURRENT_DATE,
	CURRENT_TIME,
	isSQLBuiltin,
	type SQLBuiltin,
} from "./impl/builtins.js";

export {
	// SQL identifiers
	ident,
	isSQLIdentifier,

	// SQL templates
	type SQLTemplate,
	isSQLTemplate,
} from "./impl/template.js";

// ============================================================================
// Errors
//
// Only the errors schema-side code can actually raise. Query, migration and
// connection errors live behind the database runtime and are not re-exported.
// ============================================================================

export {
	// Base error
	DatabaseError,
	isDatabaseError,
	hasErrorCode,

	// Validation errors
	ValidationError,
	TableDefinitionError,

	// Error types
	type DatabaseErrorCode,
} from "./impl/errors.js";
