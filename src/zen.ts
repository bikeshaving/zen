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
	type Infer,
	type Insert,
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
	// SQL symbols
	NOW,
	isSQLSymbol,
} from "./impl/database.js";

export {
	// SQL identifiers
	ident,
	isSQLIdentifier,
} from "./impl/template.js";

export {
	// SQL fragments (return types of table methods like deleted(), set(), etc.)
	// TODO: These should probably be TemplateTuple instead
	type SQLFragment,
	type DDLFragment,
} from "./impl/query.js";

// ============================================================================
// Errors
// ============================================================================

export {
	ZealotError,
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
	isZealotError,
	hasErrorCode,
	type ZealotErrorCode,
} from "./impl/errors.js";
