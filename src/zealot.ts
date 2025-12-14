/**
 * @b9g/zealot - Schema-driven database client
 *
 * Zod schemas define storage, validation, and form fields.
 * Not an ORM - a thin wrapper over SQL.
 */

export {
	// Table definition
	table,
	isTable,
	type Table,
	type PartialTable,
	type TableOptions,
	type ReferenceInfo,

	// Zod extension
	extendZod,

	// Field wrappers
	primary,
	unique,
	index,
	references,
	softDelete,

	// Custom field helpers
	setDBMeta,
	getDBMeta,

	// Field metadata
	type FieldMeta,
	type FieldType,
	type FieldDbMeta,

	// Type inference
	type Infer,
	type Insert,

	// Fragment method types
	type ConditionOperators,
	type ConditionValue,
	type WhereConditions,
	type SetValues,
} from "./impl/table.js";

export {
	// SQL dialect
	type SQLDialect,
	// SQL fragments
	type SQLFragment,
	// DDL fragments
	type DDLFragment,
} from "./impl/query.js";

export {
	// Database wrapper
	Database,
	Transaction,
	DatabaseUpgradeEvent,
	type Driver,
	type TaggedQuery,
} from "./impl/database.js";

export {
	// Errors
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
