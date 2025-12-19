/**
 * SQL Builtins - SQL-native values with no JavaScript equivalent.
 *
 * These symbols represent database functions that are evaluated at query time,
 * not in JavaScript. They're used for default values and expressions.
 */

// ============================================================================
// SQL Builtins
// ============================================================================

/**
 * Current timestamp - resolves to CURRENT_TIMESTAMP (standard SQL).
 *
 * @example
 * createdAt: z.date().db.inserted(CURRENT_TIMESTAMP)
 * updatedAt: z.date().db.updated(NOW)
 */
export const CURRENT_TIMESTAMP = Symbol.for("@b9g/zen:CURRENT_TIMESTAMP");

/**
 * Current date - resolves to CURRENT_DATE (standard SQL).
 *
 * @example
 * dateOnly: z.date().db.inserted(CURRENT_DATE)
 */
export const CURRENT_DATE = Symbol.for("@b9g/zen:CURRENT_DATE");

/**
 * Current time - resolves to CURRENT_TIME (standard SQL).
 *
 * @example
 * timeOnly: z.string().db.inserted(CURRENT_TIME)
 */
export const CURRENT_TIME = Symbol.for("@b9g/zen:CURRENT_TIME");

/**
 * Ergonomic alias for CURRENT_TIMESTAMP.
 *
 * @example
 * createdAt: z.date().db.inserted(NOW)
 */
export const NOW: typeof CURRENT_TIMESTAMP = CURRENT_TIMESTAMP;

/**
 * Ergonomic alias for CURRENT_DATE.
 *
 * @example
 * dateOnly: z.date().db.inserted(TODAY)
 */
export const TODAY: typeof CURRENT_DATE = CURRENT_DATE;

/** SQL builtins - SQL-native values with no JavaScript representation */
export type SQLBuiltin =
	| typeof CURRENT_TIMESTAMP
	| typeof CURRENT_DATE
	| typeof CURRENT_TIME;

/**
 * Check if a value is a known SQL builtin.
 */
export function isSQLBuiltin(value: unknown): value is SQLBuiltin {
	if (typeof value !== "symbol") return false;
	const key = Symbol.keyFor(value);
	return (
		key === "@b9g/zen:CURRENT_TIMESTAMP" ||
		key === "@b9g/zen:CURRENT_DATE" ||
		key === "@b9g/zen:CURRENT_TIME"
	);
}

/**
 * Resolve a SQL builtin symbol to its SQL representation.
 */
export function resolveSQLBuiltin(sym: symbol): string {
	const key = Symbol.keyFor(sym);
	if (!key?.startsWith("@b9g/zen:")) {
		throw new Error(`Unknown SQL builtin: ${String(sym)}`);
	}
	return key.slice("@b9g/zen:".length);
}
