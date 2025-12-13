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

	// Field wrappers
	primary,
	unique,
	index,
	references,

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
} from "./table.js";

export {
	// DDL generation
	generateDDL,
	ddl,
	type DDLOptions,
} from "./ddl.js";

export {
	// Query building
	buildSelectColumns,
	parseTemplate,
	buildQuery,
	createQuery,
	rawQuery,
	createRawQuery,
	type SQLDialect,
	type QueryOptions,
	type ParsedQuery,
	// SQL fragments
	isSQLFragment,
	createFragment,
	type SQLFragment,
} from "./query.js";

export {
	// Normalization
	normalize,
	normalizeOne,
	extractEntityData,
	buildEntityMap,
	resolveReferences,
	getPrimaryKeyValue,
	entityKey,
	type RawRow,
	type EntityMap,
	type TableMap,
} from "./normalize.js";

export {
	// Database wrapper
	Database,
	Transaction,
	DatabaseUpgradeEvent,
	createDatabase,
	type DatabaseAdapter,
	type DatabaseDriver,
	type TransactionDriver,
	type DatabaseOptions,
	type TaggedQuery,
} from "./database.js";

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
} from "./errors.js";
