/**
 * @b9g/zen - The simple database client
 *
 * Define tables. Write SQL. Get objects.
 */

import {z as zod} from "zod";
import {extendZod} from "./impl/table.js";

// Extend zod on module load
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

	// Field types
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
// Database
// ============================================================================

export {
	// Classes
	Database,
	Transaction,
	DatabaseUpgradeEvent,

	// Types
	type Driver,
	type TaggedQuery,
	type EnsureResult,
} from "./impl/database.js";

// ============================================================================
// SQL Primitives
// ============================================================================

export {
	// SQL builtins (for .db.inserted() / .db.updated())
	NOW,
	TODAY,
	CURRENT_TIMESTAMP,
	CURRENT_DATE,
	CURRENT_TIME,
	isSQLBuiltin,
} from "./impl/database.js";

export {
	// SQL identifiers
	ident,
	isSQLIdentifier,

	// SQL templates
	type SQLTemplate,
	isSQLTemplate,
} from "./impl/template.js";

export {type SQLDialect} from "./impl/sql.js";

// ============================================================================
// Errors
// ============================================================================

export {
	// Base error
	DatabaseError,
	isDatabaseError,
	hasErrorCode,

	// Validation errors
	ValidationError,
	TableDefinitionError,

	// Query errors
	QueryError,
	NotFoundError,
	AlreadyExistsError,
	ConstraintViolationError,

	// Migration errors
	MigrationError,
	MigrationLockError,
	EnsureError,
	SchemaDriftError,
	ConstraintPreflightError,

	// Connection errors
	ConnectionError,
	TransactionError,

	// Error types
	type DatabaseErrorCode,
	type EnsureOperation,
} from "./impl/errors.js";
