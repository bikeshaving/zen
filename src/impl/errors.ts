/**
 * Structured error types for database operations.
 *
 * All database errors extend DatabaseError, which includes an error code
 * for programmatic error handling.
 */

// ============================================================================
// Error Codes
// ============================================================================

export type DatabaseErrorCode =
	| "VALIDATION_ERROR"
	| "TABLE_DEFINITION_ERROR"
	| "MIGRATION_ERROR"
	| "MIGRATION_LOCK_ERROR"
	| "QUERY_ERROR"
	| "NOT_FOUND"
	| "ALREADY_EXISTS"
	| "CONSTRAINT_VIOLATION"
	| "CONNECTION_ERROR"
	| "TRANSACTION_ERROR"
	| "ENSURE_ERROR"
	| "SCHEMA_DRIFT_ERROR"
	| "CONSTRAINT_PREFLIGHT_ERROR";

// ============================================================================
// Base Error
// ============================================================================

/**
 * Base error class for all database errors.
 *
 * Includes an error code for programmatic handling.
 */
export class DatabaseError extends Error {
	readonly code: DatabaseErrorCode;

	constructor(
		code: DatabaseErrorCode,
		message: string,
		options?: ErrorOptions,
	) {
		super(message, options);
		this.name = "DatabaseError";
		this.code = code;

		// Maintains proper stack trace in V8 environments
		if (Error.captureStackTrace) {
			Error.captureStackTrace(this, this.constructor);
		}
	}
}

// ============================================================================
// Specific Error Types
// ============================================================================

/**
 * Thrown when Zod validation fails during insert/update.
 */
export class ValidationError extends DatabaseError {
	readonly fieldErrors: Record<string, string[]>;

	constructor(
		message: string,
		fieldErrors: Record<string, string[]> = {},
		options?: ErrorOptions,
	) {
		super("VALIDATION_ERROR", message, options);
		this.name = "ValidationError";
		this.fieldErrors = fieldErrors;
	}
}

/**
 * Thrown when table definition is invalid (e.g., dots in names).
 */
export class TableDefinitionError extends DatabaseError {
	readonly tableName?: string;
	readonly fieldName?: string;

	constructor(
		message: string,
		tableName?: string,
		fieldName?: string,
		options?: ErrorOptions,
	) {
		super("TABLE_DEFINITION_ERROR", message, options);
		this.name = "TableDefinitionError";
		this.tableName = tableName;
		this.fieldName = fieldName;
	}
}

/**
 * Thrown when migration fails.
 */
export class MigrationError extends DatabaseError {
	readonly fromVersion: number;
	readonly toVersion: number;

	constructor(
		message: string,
		fromVersion: number,
		toVersion: number,
		options?: ErrorOptions,
	) {
		super("MIGRATION_ERROR", message, options);
		this.name = "MigrationError";
		this.fromVersion = fromVersion;
		this.toVersion = toVersion;
	}
}

/**
 * Thrown when migration lock cannot be acquired.
 */
export class MigrationLockError extends DatabaseError {
	constructor(message: string, options?: ErrorOptions) {
		super("MIGRATION_LOCK_ERROR", message, options);
		this.name = "MigrationLockError";
	}
}

/**
 * Thrown when a query fails.
 */
export class QueryError extends DatabaseError {
	readonly sql?: string;

	constructor(message: string, sql?: string, options?: ErrorOptions) {
		super("QUERY_ERROR", message, options);
		this.name = "QueryError";
		this.sql = sql;
	}
}

/**
 * Thrown when an expected entity is not found.
 */
export class NotFoundError extends DatabaseError {
	readonly tableName: string;
	readonly id?: unknown;

	constructor(tableName: string, id?: unknown, options?: ErrorOptions) {
		const message = id
			? `${tableName} with id "${id}" not found`
			: `${tableName} not found`;
		super("NOT_FOUND", message, options);
		this.name = "NotFoundError";
		this.tableName = tableName;
		this.id = id;
	}
}

/**
 * Thrown when trying to create an entity that already exists.
 */
export class AlreadyExistsError extends DatabaseError {
	readonly tableName: string;
	readonly field?: string;
	readonly value?: unknown;

	constructor(
		tableName: string,
		field?: string,
		value?: unknown,
		options?: ErrorOptions,
	) {
		const message = field
			? `${tableName} with ${field}="${value}" already exists`
			: `${tableName} already exists`;
		super("ALREADY_EXISTS", message, options);
		this.name = "AlreadyExistsError";
		this.tableName = tableName;
		this.field = field;
		this.value = value;
	}
}

/**
 * Thrown when a database constraint is violated.
 *
 * Constraint violations are detected at the database level and converted
 * from driver-specific errors into this normalized format.
 *
 * **Stable fields**: All fields are always present with defined types.
 * Best-effort extraction from driver errors means some may be undefined.
 *
 * **Transaction behavior**: This error is thrown immediately and does NOT
 * auto-rollback. The caller or driver transaction wrapper handles rollback.
 */
export class ConstraintViolationError extends DatabaseError {
	/**
	 * Type of constraint that was violated.
	 * "unknown" if the specific type couldn't be determined from the error.
	 */
	readonly kind: "unique" | "foreign_key" | "check" | "not_null" | "unknown";

	/**
	 * Name of the constraint (e.g., "users_email_unique", "users.email").
	 * May be undefined if the database error didn't include it.
	 */
	readonly constraint?: string;

	/**
	 * Table name where the violation occurred.
	 * May be undefined if not extractable from the error.
	 */
	readonly table?: string;

	/**
	 * Column name involved in the violation.
	 * May be undefined if not extractable from the error.
	 */
	readonly column?: string;

	constructor(
		message: string,
		details: {
			kind: "unique" | "foreign_key" | "check" | "not_null" | "unknown";
			constraint?: string;
			table?: string;
			column?: string;
		},
		options?: ErrorOptions,
	) {
		super("CONSTRAINT_VIOLATION", message, options);
		this.name = "ConstraintViolationError";
		this.kind = details.kind;
		this.constraint = details.constraint;
		this.table = details.table;
		this.column = details.column;
	}
}

/**
 * Thrown when database connection fails.
 */
export class ConnectionError extends DatabaseError {
	constructor(message: string, options?: ErrorOptions) {
		super("CONNECTION_ERROR", message, options);
		this.name = "ConnectionError";
	}
}

/**
 * Thrown when a transaction fails.
 */
export class TransactionError extends DatabaseError {
	constructor(message: string, options?: ErrorOptions) {
		super("TRANSACTION_ERROR", message, options);
		this.name = "TransactionError";
	}
}

// ============================================================================
// Schema Ensure Errors
// ============================================================================

/**
 * Operation type for ensure operations.
 */
export type EnsureOperation =
	| "ensureTable"
	| "ensureConstraints"
	| "copyColumn";

/**
 * Thrown when an ensure operation fails.
 *
 * Includes step information for diagnosing partial failures,
 * since DDL is not reliably transactional on all databases (especially MySQL).
 */
export class EnsureError extends DatabaseError {
	/** The operation that failed */
	readonly operation: EnsureOperation;
	/** The table being operated on */
	readonly table: string;
	/** The step index where failure occurred (0-based) */
	readonly step: number;

	constructor(
		message: string,
		details: {
			operation: EnsureOperation;
			table: string;
			step: number;
		},
		options?: ErrorOptions,
	) {
		super("ENSURE_ERROR", message, options);
		this.name = "EnsureError";
		this.operation = details.operation;
		this.table = details.table;
		this.step = details.step;
	}
}

/**
 * Thrown when schema drift is detected.
 *
 * Schema drift occurs when an existing database object doesn't match
 * the expected schema definition. For example, a column exists but
 * has a different type, or an index covers different columns.
 */
export class SchemaDriftError extends DatabaseError {
	/** The table where drift was detected */
	readonly table: string;
	/** Description of what drifted */
	readonly drift: string;
	/** Suggested action to resolve */
	readonly suggestion?: string;

	constructor(
		message: string,
		details: {
			table: string;
			drift: string;
			suggestion?: string;
		},
		options?: ErrorOptions,
	) {
		super("SCHEMA_DRIFT_ERROR", message, options);
		this.name = "SchemaDriftError";
		this.table = details.table;
		this.drift = details.drift;
		this.suggestion = details.suggestion;
	}
}

/**
 * Thrown when a constraint preflight check finds violations.
 *
 * Before adding a UNIQUE constraint or foreign key to existing data,
 * a preflight check verifies data integrity. This error is thrown
 * when violations are found, including the query used to detect them.
 */
export class ConstraintPreflightError extends DatabaseError {
	/** The table being constrained */
	readonly table: string;
	/** The constraint being added (e.g., "unique:email" or "fk:authorId") */
	readonly constraint: string;
	/** Number of violating rows */
	readonly violationCount: number;
	/** The SQL query that found the violations - run it to see details */
	readonly query: string;

	constructor(
		message: string,
		details: {
			table: string;
			constraint: string;
			violationCount: number;
			query: string;
		},
		options?: ErrorOptions,
	) {
		super("CONSTRAINT_PREFLIGHT_ERROR", message, options);
		this.name = "ConstraintPreflightError";
		this.table = details.table;
		this.constraint = details.constraint;
		this.violationCount = details.violationCount;
		this.query = details.query;
	}
}

// ============================================================================
// Type Guards
// ============================================================================

/**
 * Check if an error is a DatabaseError.
 */
export function isDatabaseError(error: unknown): error is DatabaseError {
	return error instanceof DatabaseError;
}

/**
 * Check if an error has a specific error code.
 */
export function hasErrorCode(
	error: unknown,
	code: DatabaseErrorCode,
): error is DatabaseError {
	return isDatabaseError(error) && error.code === code;
}
