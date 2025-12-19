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
	table,
	type Table,
	type TableOptions,
	type Row,
	type Insert,
	type Update,
	/** @deprecated Use `Row<T>` instead */
	type Infer,
	type FieldMeta,
	type FieldType,
} from "./impl/table.js";

// ============================================================================
// Database
// ============================================================================

export {
	Database,
	Transaction,
	DatabaseUpgradeEvent,
	type Driver,
} from "./impl/database.js";

// ============================================================================
// SQL Primitives (for drivers and custom queries)
// ============================================================================

export {
	// SQL builtins - SQL-native values with no JS equivalent
	CURRENT_TIMESTAMP,
	CURRENT_DATE,
	CURRENT_TIME,
	NOW,
	TODAY,
	isSQLBuiltin,
} from "./impl/database.js";

export {
	// SQL identifiers
	ident,
	isSQLIdentifier,
} from "./impl/template.js";

export {
	// SQL template (return type of table methods like deleted(), set(), etc.)
	type SQLTemplate,
	isSQLTemplate,
} from "./impl/template.js";

export {
	// Schema ensure result
	type EnsureResult,
} from "./impl/database.js";

// ============================================================================
// Errors
// ============================================================================

export {
	DatabaseError,
	ValidationError,
	TableDefinitionError,
	MigrationError,
	MigrationLockError,
	QueryError,
	NotFoundError,
	AlreadyExistsError,
	ConstraintViolationError,
	ConnectionError,
	TransactionError,
	EnsureError,
	SchemaDriftError,
	ConstraintPreflightError,
	isDatabaseError,
	hasErrorCode,
	type DatabaseErrorCode,
	type EnsureOperation,
} from "./impl/errors.js";
