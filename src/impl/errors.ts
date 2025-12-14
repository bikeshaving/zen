/**
 * Structured error types for Zealot.
 *
 * All Zealot errors extend ZealotError, which includes an error code
 * for programmatic error handling.
 */

// ============================================================================
// Error Codes
// ============================================================================

export type ZealotErrorCode =
	| "VALIDATION_ERROR"
	| "TABLE_DEFINITION_ERROR"
	| "MIGRATION_ERROR"
	| "MIGRATION_LOCK_ERROR"
	| "QUERY_ERROR"
	| "NOT_FOUND"
	| "ALREADY_EXISTS"
	| "CONSTRAINT_VIOLATION"
	| "CONNECTION_ERROR"
	| "TRANSACTION_ERROR";

// ============================================================================
// Base Error
// ============================================================================

/**
 * Base error class for all Zealot errors.
 *
 * Includes an error code for programmatic handling.
 */
export class ZealotError extends Error {
	readonly code: ZealotErrorCode;

	constructor(code: ZealotErrorCode, message: string, options?: ErrorOptions) {
		super(message, options);
		this.name = "ZealotError";
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
export class ValidationError extends ZealotError {
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
export class TableDefinitionError extends ZealotError {
	readonly tableName: string;
	readonly fieldName?: string;

	constructor(
		message: string,
		tableName: string,
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
export class MigrationError extends ZealotError {
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
export class MigrationLockError extends ZealotError {
	constructor(message: string, options?: ErrorOptions) {
		super("MIGRATION_LOCK_ERROR", message, options);
		this.name = "MigrationLockError";
	}
}

/**
 * Thrown when a query fails.
 */
export class QueryError extends ZealotError {
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
export class NotFoundError extends ZealotError {
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
export class AlreadyExistsError extends ZealotError {
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
export class ConstraintViolationError extends ZealotError {
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
export class ConnectionError extends ZealotError {
	constructor(message: string, options?: ErrorOptions) {
		super("CONNECTION_ERROR", message, options);
		this.name = "ConnectionError";
	}
}

/**
 * Thrown when a transaction fails.
 */
export class TransactionError extends ZealotError {
	constructor(message: string, options?: ErrorOptions) {
		super("TRANSACTION_ERROR", message, options);
		this.name = "TransactionError";
	}
}

// ============================================================================
// Type Guards
// ============================================================================

/**
 * Check if an error is a ZealotError.
 */
export function isZealotError(error: unknown): error is ZealotError {
	return error instanceof ZealotError;
}

/**
 * Check if an error has a specific error code.
 */
export function hasErrorCode(
	error: unknown,
	code: ZealotErrorCode,
): error is ZealotError {
	return isZealotError(error) && error.code === code;
}
