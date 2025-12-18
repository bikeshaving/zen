/**
 * Database wrapper - the main API for schema-driven SQL.
 *
 * Provides typed queries with entity normalization and reference resolution.
 * Extends EventTarget for IndexedDB-style migration events.
 */

import type {Table, Infer, Insert, FullTableOnly} from "./table.js";
import {validateWithStandardSchema} from "./table.js";
import {z} from "zod";
import {normalize, normalizeOne} from "./query.js";
import {ident, isSQLTemplate, makeTemplate} from "./template.js";
import {EnsureError} from "./errors.js";

// ============================================================================
// DB Expressions - Runtime values evaluated by the database
// ============================================================================

const DB_EXPR = Symbol.for("@b9g/zen:db-expr");

/**
 * Internal type for resolved DB expressions (template to inject).
 * Uses TemplateStringsArray + values instead of SQL with ? placeholders.
 */
interface DBExpression {
	readonly [DB_EXPR]: true;
	readonly strings: TemplateStringsArray;
	readonly values: readonly unknown[];
}

/**
 * Check if a value is a resolved DB expression.
 */
function isDBExpression(value: unknown): value is DBExpression {
	return (
		value !== null &&
		typeof value === "object" &&
		DB_EXPR in value &&
		(value as any)[DB_EXPR] === true
	);
}

/**
 * Create a DB expression from template strings and values.
 */
function createDBExpr(
	strings: TemplateStringsArray,
	values: readonly unknown[] = [],
): DBExpression {
	return {[DB_EXPR]: true, strings, values};
}

// ============================================================================
// SQL Symbols - Named expressions resolved at query time
// ============================================================================

/**
 * Current timestamp. Resolves to CURRENT_TIMESTAMP (standard SQL).
 *
 * @example
 * createdAt: z.date().db.inserted(NOW)
 * updatedAt: z.date().db.updated(NOW)
 */
export const NOW = Symbol.for("@b9g/zen:now");

/** Known SQL symbols that can be used in inserted()/updated() */
export type SQLSymbol = typeof NOW;

/**
 * Check if a value is a known SQL symbol.
 */
export function isSQLSymbol(value: unknown): value is SQLSymbol {
	return value === NOW;
}

/**
 * Resolve a SQL symbol to SQL.
 */
function resolveSQLSymbol(sym: symbol): string {
	if (sym === NOW) {
		// CURRENT_TIMESTAMP is standard SQL, works across all databases
		return "CURRENT_TIMESTAMP";
	}
	throw new Error(`Unknown SQL symbol: ${String(sym)}`);
}

/**
 * Separate DB expressions and SQL symbols from regular data values.
 * Both skip validation and encoding - they're raw SQL resolved at query time.
 *
 * @param data - User-provided data
 * @param table - Optional table to validate expressions aren't used with encoded fields
 */
function extractDBExpressions(
	data: Record<string, unknown>,
	table?: Table<any>,
): {
	regularData: Record<string, unknown>;
	expressions: Record<string, DBExpression>;
	symbols: Record<string, SQLSymbol>;
} {
	const regularData: Record<string, unknown> = {};
	const expressions: Record<string, DBExpression> = {};
	const symbols: Record<string, SQLSymbol> = {};

	for (const [key, value] of Object.entries(data)) {
		if (isDBExpression(value)) {
			// Validate: DB expressions cannot be used with encoded/decoded fields
			if (table) {
				const fieldMeta = table.meta.fields[key];
				if (fieldMeta?.encode || fieldMeta?.decode) {
					throw new Error(
						`Cannot use DB expression for field "${key}" which has encode/decode. ` +
							`DB expressions bypass encoding and are sent directly to the database.`,
					);
				}
			}
			expressions[key] = value;
		} else if (isSQLSymbol(value)) {
			// Validate: SQL symbols cannot be used with encoded/decoded fields
			if (table) {
				const fieldMeta = table.meta.fields[key];
				if (fieldMeta?.encode || fieldMeta?.decode) {
					throw new Error(
						`Cannot use SQL symbol for field "${key}" which has encode/decode. ` +
							`SQL symbols bypass encoding and are sent directly to the database.`,
					);
				}
			}
			symbols[key] = value;
		} else {
			regularData[key] = value;
		}
	}

	return {regularData, expressions, symbols};
}

/**
 * Inject schema-defined values for insert/update operations.
 * Checks field metadata for .db.inserted(), .db.updated(), and .db.upserted() markers.
 *
 * @param table - Table definition with schema metadata
 * @param data - User-provided data
 * @param operation - "insert" or "update"
 * @returns Data with schema defaults injected (user values take precedence)
 */
function injectSchemaExpressions<T extends Table<any>>(
	table: T,
	data: Record<string, unknown>,
	operation: "insert" | "update",
): Record<string, unknown> {
	const result = {...data};

	for (const [fieldName, fieldMeta] of Object.entries(table.meta.fields)) {
		// Skip if user already provided a value
		if (fieldName in data) continue;

		// Skip auto-increment fields (database handles)
		if (fieldMeta.autoIncrement) continue;

		// Determine which metadata to use
		// - inserted(): INSERT only
		// - updated(): UPDATE only
		// - upserted(): both INSERT and UPDATE (fallback)
		const meta =
			operation === "insert"
				? (fieldMeta.inserted ?? fieldMeta.upserted)
				: (fieldMeta.updated ?? fieldMeta.upserted);

		if (!meta) continue;

		// Resolve the value based on type
		if (meta.type === "sql") {
			result[fieldName] = createDBExpr(meta.template![0], [
				...meta.template![1],
			]);
		} else if (meta.type === "symbol") {
			// Pass symbol through - resolved at query build time
			result[fieldName] = meta.symbol;
		} else if (meta.type === "function") {
			result[fieldName] = meta.fn!();
		}
	}

	return result;
}

// ============================================================================
// Encoding/Decoding
// ============================================================================

/**
 * Encode data for database insert/update operations.
 * Converts app values → DB values using .db.encode() functions.
 * Automatically encodes objects/arrays as JSON unless custom encoding is specified.
 */
export function encodeData<T extends Table<any>>(
	table: T,
	data: Record<string, unknown>,
): Record<string, unknown> {
	const encoded: Record<string, unknown> = {};
	const shape = table.schema.shape;

	for (const [key, value] of Object.entries(data)) {
		const fieldMeta = table.meta.fields[key];
		const fieldSchema = shape?.[key];

		if (fieldMeta?.encode && typeof fieldMeta.encode === "function") {
			// Custom encoding specified - use it
			encoded[key] = fieldMeta.encode(value);
		} else if (fieldSchema) {
			// Check if field is an object or array type - auto-encode as JSON
			let core = fieldSchema;
			while (typeof (core as any).unwrap === "function") {
				// Stop unwrapping if we hit an array or object (they have unwrap() but it returns the element/shape)
				if (core instanceof z.ZodArray || core instanceof z.ZodObject) {
					break;
				}
				core = (core as any).unwrap();
			}

			if (
				(core instanceof z.ZodObject || core instanceof z.ZodArray) &&
				value !== null &&
				value !== undefined
			) {
				// Automatic JSON encoding for objects and arrays
				encoded[key] = JSON.stringify(value);
			} else {
				encoded[key] = value;
			}
		} else {
			encoded[key] = value;
		}
	}

	return encoded;
}

/**
 * Decode data from database read operations.
 * Converts DB values → app values using .db.decode() functions.
 * Automatically decodes JSON strings to objects/arrays unless custom decoding is specified.
 */
export function decodeData<T extends Table<any>>(
	table: T,
	data: Record<string, unknown> | null,
): Record<string, unknown> | null {
	if (!data) return data;

	const decoded: Record<string, unknown> = {};
	const shape = table.schema.shape;

	for (const [key, value] of Object.entries(data)) {
		const fieldMeta = table.meta.fields[key];
		const fieldSchema = shape?.[key];

		if (fieldMeta?.decode && typeof fieldMeta.decode === "function") {
			// Custom decoding specified - use it
			decoded[key] = fieldMeta.decode(value);
		} else if (fieldSchema) {
			// Check if field is an object or array type - auto-decode from JSON
			let core = fieldSchema;
			while (typeof (core as any).unwrap === "function") {
				// Stop unwrapping if we hit an array or object (they have unwrap() but it returns the element/shape)
				if (core instanceof z.ZodArray || core instanceof z.ZodObject) {
					break;
				}
				core = (core as any).unwrap();
			}

			if (core instanceof z.ZodObject || core instanceof z.ZodArray) {
				// Automatic JSON decoding for objects and arrays
				if (typeof value === "string") {
					try {
						decoded[key] = JSON.parse(value);
					} catch (e) {
						// Throw with helpful error message mentioning JSON and field
						throw new Error(
							`JSON parse error for field "${key}": ${e instanceof Error ? e.message : String(e)}. ` +
								`Value was: ${value.slice(0, 100)}${value.length > 100 ? "..." : ""}`,
						);
					}
				} else {
					// Already an object (e.g., from PostgreSQL JSONB)
					decoded[key] = value;
				}
			} else if (core instanceof z.ZodDate) {
				// Automatic date decoding from ISO string
				if (typeof value === "string") {
					const date = new Date(value);
					if (isNaN(date.getTime())) {
						throw new Error(
							`Invalid date value for field "${key}": "${value}" cannot be parsed as a valid date`,
						);
					}
					decoded[key] = date;
				} else if (value instanceof Date) {
					if (isNaN(value.getTime())) {
						throw new Error(
							`Invalid Date object for field "${key}": received an Invalid Date`,
						);
					}
					decoded[key] = value;
				} else {
					decoded[key] = value;
				}
			} else {
				decoded[key] = value;
			}
		} else {
			decoded[key] = value;
		}
	}

	return decoded;
}

// ============================================================================
// Driver Interface
// ============================================================================

/**
 * Database driver interface.
 *
 * Drivers own all SQL generation and dialect-specific behavior. They receive
 * template parts (strings + values) and build SQL with native placeholders.
 *
 * This keeps the Database class dialect-agnostic - it only interacts with
 * drivers through this interface and the `supportsReturning` capability flag.
 */
export interface Driver {
	// ==========================================================================
	// Query execution (drivers build SQL with native placeholders)
	// ==========================================================================

	/**
	 * Execute a query and return all rows.
	 * Driver joins strings with native placeholders (? or $1, $2, ...).
	 */
	all<T = Record<string, unknown>>(
		strings: TemplateStringsArray,
		values: unknown[],
	): Promise<T[]>;

	/**
	 * Execute a query and return the first row.
	 */
	get<T = Record<string, unknown>>(
		strings: TemplateStringsArray,
		values: unknown[],
	): Promise<T | null>;

	/**
	 * Execute a statement and return the number of affected rows.
	 */
	run(strings: TemplateStringsArray, values: unknown[]): Promise<number>;

	/**
	 * Execute a query and return a single value, or null if no rows.
	 */
	val<T = unknown>(
		strings: TemplateStringsArray,
		values: unknown[],
	): Promise<T | null>;

	// ==========================================================================
	// Connection management
	// ==========================================================================

	/**
	 * Close the database connection.
	 */
	close(): Promise<void>;

	/**
	 * Execute a function within a database transaction.
	 */
	transaction<T>(fn: (txDriver: Driver) => Promise<T>): Promise<T>;

	// ==========================================================================
	// Capabilities
	// ==========================================================================

	/**
	 * Whether this driver supports RETURNING clause for INSERT/UPDATE.
	 * - SQLite: true
	 * - PostgreSQL: true
	 * - MySQL: false
	 * - MariaDB 10.5+: true
	 */
	readonly supportsReturning: boolean;

	// ==========================================================================
	// Optional capabilities
	// ==========================================================================

	/**
	 * Execute a function while holding an exclusive migration lock.
	 */
	withMigrationLock?<T>(fn: () => Promise<T>): Promise<T>;

	// ==========================================================================
	// Schema ensure operations (optional - dialect-specific implementations)
	// ==========================================================================

	/**
	 * Ensure a table exists with its columns and indexes.
	 *
	 * **For new tables**: Creates the table with full structure including
	 * primary key, unique constraints, foreign keys, and indexes.
	 *
	 * **For existing tables**: Only performs safe, additive operations:
	 * - Adds missing columns
	 * - Adds missing non-unique indexes
	 *
	 * Throws SchemaDriftError if existing table has missing constraints
	 * (directs user to run ensureConstraints).
	 */
	ensureTable?<T extends Table<any>>(table: T): Promise<EnsureResult>;

	/**
	 * Ensure constraints (unique, foreign key) are applied to an existing table.
	 *
	 * Performs preflight checks to detect data violations before applying
	 * constraints. Throws ConstraintPreflightError if violations found.
	 */
	ensureConstraints?<T extends Table<any>>(table: T): Promise<EnsureResult>;

	/**
	 * Copy column data for safe rename migrations.
	 *
	 * Executes: UPDATE <table> SET <toField> = <fromField> WHERE <toField> IS NULL
	 *
	 * @param table The table to update
	 * @param fromField Source column (may be legacy/not in schema)
	 * @param toField Destination column (must exist in schema)
	 * @returns Number of rows updated
	 */
	copyColumn?<T extends Table<any>>(
		table: T,
		fromField: string,
		toField: string,
	): Promise<number>;

	/** Optional introspection: list columns for a table (name, type, nullability). */
	getColumns?(
		tableName: string,
	): Promise<{name: string; type?: string; notnull?: boolean}[]>;
}

// ============================================================================
// Schema Ensure Types
// ============================================================================

/**
 * Result from ensure operations.
 */
export interface EnsureResult {
	/** Whether any DDL was executed (false = no-op) */
	applied: boolean;
}

// ============================================================================
// Template Building Helpers
// ============================================================================

/**
 * Merge an expression's template into a base template.
 * Operates on mutable string[] and values[], maintains invariant:
 *   strings.length === values.length + 1
 *
 * @example
 * // Merging expression ["COALESCE(", ", ", ")"] with values [a, b]
 * // into base ["INSERT ... VALUES ("] with values []
 * // Result: strings = ["INSERT ... VALUES (COALESCE(", ", ", ")"]
 * //         values = [a, b]
 */
function mergeExpression(
	baseStrings: string[],
	baseValues: unknown[],
	expr: DBExpression,
): void {
	// Append expr.strings[0] to last baseString
	baseStrings[baseStrings.length - 1] += expr.strings[0];
	// Push remaining expr strings
	for (let i = 1; i < expr.strings.length; i++) {
		baseStrings.push(expr.strings[i]);
	}
	// Push expr values
	baseValues.push(...expr.values);
}

/**
 * Build INSERT template: INSERT INTO <table> (<col1>, <col2>) VALUES (<val1>, <val2>)
 * Returns template parts and values array.
 * Identifiers are passed as SQLIdentifier values - quoted by drivers per dialect.
 * Expressions are merged using mergeExpression to maintain template invariant.
 */
function buildInsertParts(
	tableName: string,
	data: Record<string, unknown>,
	expressions: Record<string, DBExpression>,
	symbols: Record<string, SQLSymbol> = {},
): {strings: TemplateStringsArray; values: unknown[]} {
	const regularCols = Object.keys(data);
	const exprCols = Object.keys(expressions);
	const symbolCols = Object.keys(symbols);
	const allCols = [...regularCols, ...symbolCols, ...exprCols];

	if (allCols.length === 0) {
		throw new Error("Insert requires at least one column");
	}

	const strings: string[] = ["INSERT INTO "];
	const values: unknown[] = [];

	// Table name (identifier)
	values.push(ident(tableName));
	strings.push(" (");

	// Column names (identifiers)
	for (let i = 0; i < allCols.length; i++) {
		values.push(ident(allCols[i]));
		strings.push(i < allCols.length - 1 ? ", " : ") VALUES (");
	}

	// Value placeholders: regular data values, then symbol values
	const valueCols = [...regularCols, ...symbolCols];
	for (let i = 0; i < valueCols.length; i++) {
		const col = valueCols[i];
		values.push(col in data ? data[col] : symbols[col]);
		strings.push(i < valueCols.length - 1 ? ", " : "");
	}

	// Expression values using mergeExpression
	for (let i = 0; i < exprCols.length; i++) {
		if (valueCols.length > 0 || i > 0) {
			strings[strings.length - 1] += ", ";
		}
		mergeExpression(strings, values, expressions[exprCols[i]]);
	}

	strings[strings.length - 1] += ")";

	return {
		strings: makeTemplate(strings),
		values,
	};
}

/**
 * Build UPDATE template: UPDATE <table> SET <col1> = <val1>, <col2> = <val2> WHERE <pk> = <id>
 * Returns template parts and values array.
 * Identifiers are passed as SQLIdentifier values - quoted by drivers per dialect.
 * Expressions are merged using mergeExpression to maintain template invariant.
 */
function buildUpdateByIdParts(
	tableName: string,
	pk: string,
	data: Record<string, unknown>,
	expressions: Record<string, DBExpression>,
	id: unknown,
	symbols: Record<string, SQLSymbol> = {},
): {strings: TemplateStringsArray; values: unknown[]} {
	const regularCols = Object.keys(data);
	const exprCols = Object.keys(expressions);
	const symbolCols = Object.keys(symbols);
	const valueCols = [...regularCols, ...symbolCols];

	const strings: string[] = ["UPDATE "];
	const values: unknown[] = [];

	// Table name
	values.push(ident(tableName));
	strings.push(" SET ");

	// SET assignments for regular and symbol values
	for (let i = 0; i < valueCols.length; i++) {
		const col = valueCols[i];
		values.push(ident(col));
		strings.push(" = ");
		values.push(col in data ? data[col] : symbols[col]);
		strings.push(i < valueCols.length - 1 ? ", " : "");
	}

	// Expression assignments using mergeExpression
	for (let i = 0; i < exprCols.length; i++) {
		if (valueCols.length > 0 || i > 0) {
			strings[strings.length - 1] += ", ";
		}
		values.push(ident(exprCols[i]));
		strings.push(" = ");
		mergeExpression(strings, values, expressions[exprCols[i]]);
	}

	// WHERE clause
	strings[strings.length - 1] += " WHERE ";
	values.push(ident(pk));
	strings.push(" = ");
	values.push(id);
	strings.push("");

	return {
		strings: makeTemplate(strings),
		values,
	};
}

/**
 * Build UPDATE template for multiple IDs: UPDATE <table> SET ... WHERE <pk> IN (<id1>, <id2>, ...)
 * Identifiers are passed as SQLIdentifier values - quoted by drivers per dialect.
 * Expressions are merged using mergeExpression to maintain template invariant.
 */
function buildUpdateByIdsParts(
	tableName: string,
	pk: string,
	data: Record<string, unknown>,
	expressions: Record<string, DBExpression>,
	ids: unknown[],
	symbols: Record<string, SQLSymbol> = {},
): {strings: TemplateStringsArray; values: unknown[]} {
	const regularCols = Object.keys(data);
	const exprCols = Object.keys(expressions);
	const symbolCols = Object.keys(symbols);
	const valueCols = [...regularCols, ...symbolCols];

	const strings: string[] = ["UPDATE "];
	const values: unknown[] = [];

	// Table name
	values.push(ident(tableName));
	strings.push(" SET ");

	// SET assignments for regular and symbol values
	for (let i = 0; i < valueCols.length; i++) {
		const col = valueCols[i];
		values.push(ident(col));
		strings.push(" = ");
		values.push(col in data ? data[col] : symbols[col]);
		strings.push(i < valueCols.length - 1 ? ", " : "");
	}

	// Expression assignments using mergeExpression
	for (let i = 0; i < exprCols.length; i++) {
		if (valueCols.length > 0 || i > 0) {
			strings[strings.length - 1] += ", ";
		}
		values.push(ident(exprCols[i]));
		strings.push(" = ");
		mergeExpression(strings, values, expressions[exprCols[i]]);
	}

	// WHERE clause with IN
	strings[strings.length - 1] += " WHERE ";
	values.push(ident(pk));
	strings.push(" IN (");

	// ID values
	for (let i = 0; i < ids.length; i++) {
		values.push(ids[i]);
		strings.push(i < ids.length - 1 ? ", " : ")");
	}

	return {
		strings: makeTemplate(strings),
		values,
	};
}

/**
 * Build SELECT column list: <table>.<col1> AS <alias1>, <table>.<col2> AS <alias2>, ...
 * Returns template parts with identifiers as SQLIdentifier values.
 * Handles derived expressions too.
 */
function buildSelectCols(tables: Table<any>[]): {
	strings: string[];
	values: unknown[];
} {
	const strings: string[] = [""];
	const values: unknown[] = [];
	let needsComma = false;

	for (const table of tables) {
		const tableName = table.name;
		const shape = table.schema.shape;

		// Get derived fields set (for skipping in regular column output)
		const derivedFields = new Set<string>(
			(table.meta as any).derivedFields ?? [],
		);

		// Add regular columns (skip derived fields - they come from expressions)
		for (const fieldName of Object.keys(shape)) {
			if (derivedFields.has(fieldName)) continue;

			if (needsComma) {
				strings[strings.length - 1] += ", ";
			}
			// table.column AS alias
			values.push(ident(tableName));
			strings.push(".");
			values.push(ident(fieldName));
			strings.push(" AS ");
			values.push(ident(`${tableName}.${fieldName}`));
			strings.push("");
			needsComma = true;
		}

		// Append derived expressions with auto-generated aliases
		const derivedExprs = (table.meta as any).derivedExprs ?? [];
		for (const expr of derivedExprs) {
			if (needsComma) {
				strings[strings.length - 1] += ", ";
			}
			const alias = `${tableName}.${expr.fieldName}`;
			// For derived expressions, merge their template and wrap in parentheses
			strings[strings.length - 1] += "(";
			// Merge the expression template
			strings[strings.length - 1] += expr.template[0][0];
			for (let i = 1; i < expr.template[0].length; i++) {
				strings.push(expr.template[0][i]);
			}
			values.push(...expr.template[1]);
			// Add closing paren and AS alias
			strings[strings.length - 1] += ") AS ";
			values.push(ident(alias));
			strings.push("");
			needsComma = true;
		}
	}

	return {strings, values};
}

/**
 * Build SELECT by primary key template: SELECT * FROM <table> WHERE <pk> = <id>
 */
function buildSelectByPkParts(
	tableName: string,
	pk: string,
	id: unknown,
): {strings: TemplateStringsArray; values: unknown[]} {
	return {
		strings: makeTemplate(["SELECT * FROM ", " WHERE ", " = ", ""]),
		values: [ident(tableName), ident(pk), id],
	};
}

/**
 * Build SELECT by multiple IDs: SELECT * FROM <table> WHERE <pk> IN (<id1>, <id2>, ...)
 */
function buildSelectByPksParts(
	tableName: string,
	pk: string,
	ids: unknown[],
): {strings: TemplateStringsArray; values: unknown[]} {
	const strings: string[] = ["SELECT * FROM ", " WHERE ", " IN ("];
	const values: unknown[] = [ident(tableName), ident(pk)];

	for (let i = 0; i < ids.length; i++) {
		values.push(ids[i]);
		strings.push(i < ids.length - 1 ? ", " : ")");
	}

	return {
		strings: makeTemplate(strings),
		values,
	};
}

/**
 * Build DELETE by primary key template: DELETE FROM <table> WHERE <pk> = <id>
 */
function buildDeleteByPkParts(
	tableName: string,
	pk: string,
	id: unknown,
): {strings: TemplateStringsArray; values: unknown[]} {
	return {
		strings: makeTemplate(["DELETE FROM ", " WHERE ", " = ", ""]),
		values: [ident(tableName), ident(pk), id],
	};
}

/**
 * Build DELETE by multiple IDs: DELETE FROM <table> WHERE <pk> IN (<id1>, <id2>, ...)
 */
function buildDeleteByPksParts(
	tableName: string,
	pk: string,
	ids: unknown[],
): {strings: TemplateStringsArray; values: unknown[]} {
	const strings: string[] = ["DELETE FROM ", " WHERE ", " IN ("];
	const values: unknown[] = [ident(tableName), ident(pk)];

	for (let i = 0; i < ids.length; i++) {
		values.push(ids[i]);
		strings.push(i < ids.length - 1 ? ", " : ")");
	}

	return {
		strings: makeTemplate(strings),
		values,
	};
}

/**
 * Append RETURNING * to a template.
 */
function appendReturning(parts: {
	strings: TemplateStringsArray;
	values: unknown[];
}): {strings: TemplateStringsArray; values: unknown[]} {
	const strings = [...parts.strings];
	strings[strings.length - 1] += " RETURNING *";
	return {
		strings: makeTemplate(strings),
		values: parts.values,
	};
}

/**
 * Expand SQLFragment objects within template values.
 * Returns flattened strings and values arrays.
 */
function expandFragments(
	strings: TemplateStringsArray,
	values: unknown[],
): {strings: TemplateStringsArray; values: unknown[]} {
	const newStrings: string[] = [strings[0]];
	const newValues: unknown[] = [];

	for (let i = 0; i < values.length; i++) {
		const value = values[i];
		if (isSQLTemplate(value)) {
			// Expand template: merge its parts directly (tuple format: [strings, values])
			// Append template[0][0] to last newString
			newStrings[newStrings.length - 1] += value[0][0];

			// Push remaining template strings and all template values
			for (let j = 1; j < value[0].length; j++) {
				newStrings.push(value[0][j]);
			}
			newValues.push(...value[1]);

			// Append the next template string part
			newStrings[newStrings.length - 1] += strings[i + 1];
		} else {
			// Regular value: add placeholder position
			newStrings.push(strings[i + 1]);
			newValues.push(value);
		}
	}

	return {
		strings: makeTemplate(newStrings),
		values: newValues,
	};
}

// ============================================================================
// Database Upgrade Event
// ============================================================================

/**
 * Event fired when database version increases during open().
 *
 * Similar to IndexedDB's IDBVersionChangeEvent combined with
 * ServiceWorker's ExtendableEvent (for waitUntil support).
 *
 * **Migration model**: Zealot uses monotonic, forward-only versioning:
 * - Versions are integers that only increase: 1 → 2 → 3 → ...
 * - Downgrading (e.g., 3 → 2) is NOT supported
 * - Branching version histories are NOT supported
 * - Each version should be deployed once and never modified
 *
 * **Best practices**:
 * - Use conditional checks: `if (e.oldVersion < 2) { ... }`
 * - Prefer additive changes (new columns, indexes) over destructive ones
 * - Never modify past migrations - add new versions instead
 * - Keep migrations idempotent when possible (use db.ensureTable())
 */
export class DatabaseUpgradeEvent extends Event {
	readonly oldVersion: number;
	readonly newVersion: number;
	#promises: Promise<void>[] = [];

	constructor(type: string, init: {oldVersion: number; newVersion: number}) {
		super(type);
		this.oldVersion = init.oldVersion;
		this.newVersion = init.newVersion;
	}

	/**
	 * Extend the event lifetime until the promise settles.
	 * Like ExtendableEvent.waitUntil() from ServiceWorker.
	 */
	waitUntil(promise: Promise<void>): void {
		this.#promises.push(promise);
	}

	/**
	 * @internal Wait for all waitUntil promises to settle.
	 */
	async _settle(): Promise<void> {
		await Promise.all(this.#promises);
	}
}

// ============================================================================
// Transaction
// ============================================================================

/**
 * Tagged template query function that returns normalized entities.
 */
export type TaggedQuery<T> = (
	strings: TemplateStringsArray,
	...values: unknown[]
) => Promise<T>;

/**
 * Transaction context with query methods.
 *
 * Provides the same query interface as Database, but bound to a single
 * connection for the duration of the transaction.
 */
export class Transaction {
	#driver: Driver;

	constructor(driver: Driver) {
		this.#driver = driver;
	}

	// ==========================================================================
	// Queries - Return Normalized Entities
	// ==========================================================================

	all<T extends Table<any>>(tables: T | T[]): TaggedQuery<Infer<T>[]> {
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const {strings: colStrings, values: colValues} =
				buildSelectCols(tableArray);
			const {strings: expandedStrings, values: expandedValues} =
				expandFragments(strings, values);

			// Build: SELECT <cols> FROM <table> <where>
			const queryStrings: string[] = ["SELECT "];
			const queryValues: unknown[] = [];

			// Merge column template
			queryStrings[0] += colStrings[0];
			for (let i = 1; i < colStrings.length; i++) {
				queryStrings.push(colStrings[i]);
			}
			queryValues.push(...colValues);

			// Add FROM <table>
			queryStrings[queryStrings.length - 1] += " FROM ";
			queryValues.push(ident(tableArray[0].name));
			queryStrings.push(" ");

			// Merge WHERE template
			queryStrings[queryStrings.length - 1] += expandedStrings[0];
			for (let i = 1; i < expandedStrings.length; i++) {
				queryStrings.push(expandedStrings[i]);
			}
			queryValues.push(...expandedValues);

			const rows = await this.#driver.all<Record<string, unknown>>(
				makeTemplate(queryStrings),
				queryValues,
			);
			return normalize<Infer<T>>(rows, tableArray as Table<any>[]);
		};
	}

	get<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<Infer<T> | null>;
	get<T extends Table<any>>(tables: T | T[]): TaggedQuery<Infer<T> | null>;
	get<T extends Table<any>>(
		tables: T | T[],
		id?: string | number,
	): Promise<Infer<T> | null> | TaggedQuery<Infer<T> | null> {
		// Convenience overload: get by primary key
		if (id !== undefined) {
			const table = tables as T;
			const pk = table.meta.primary;
			if (!pk) {
				return Promise.reject(
					new Error(`Table ${table.name} has no primary key defined`),
				);
			}
			const {strings, values} = buildSelectByPkParts(table.name, pk, id);
			return this.#driver
				.get<Record<string, unknown>>(strings, values)
				.then((row) => {
					if (!row) return null;
					return decodeData(table, row) as Infer<T>;
				});
		}

		// Tagged template query
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const {strings: colStrings, values: colValues} =
				buildSelectCols(tableArray);
			const {strings: expandedStrings, values: expandedValues} =
				expandFragments(strings, values);

			// Build: SELECT <cols> FROM <table> <where>
			const queryStrings: string[] = ["SELECT "];
			const queryValues: unknown[] = [];

			// Merge column template
			queryStrings[0] += colStrings[0];
			for (let i = 1; i < colStrings.length; i++) {
				queryStrings.push(colStrings[i]);
			}
			queryValues.push(...colValues);

			// Add FROM <table>
			queryStrings[queryStrings.length - 1] += " FROM ";
			queryValues.push(ident(tableArray[0].name));
			queryStrings.push(" ");

			// Merge WHERE template
			queryStrings[queryStrings.length - 1] += expandedStrings[0];
			for (let i = 1; i < expandedStrings.length; i++) {
				queryStrings.push(expandedStrings[i]);
			}
			queryValues.push(...expandedValues);

			const row = await this.#driver.get<Record<string, unknown>>(
				makeTemplate(queryStrings),
				queryValues,
			);
			return normalizeOne<Infer<T>>(row, tableArray as Table<any>[]);
		};
	}

	// ==========================================================================
	// Mutations - Validate Through Zod
	// ==========================================================================

	async insert<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Insert<T>,
	): Promise<Infer<T>>;
	async insert<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Insert<T>[],
	): Promise<Infer<T>[]>;
	async insert<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Insert<T> | Insert<T>[],
	): Promise<Infer<T> | Infer<T>[]> {
		if (Array.isArray(data)) {
			if (data.length === 0) {
				return [];
			}
			const results: Infer<T>[] = [];
			for (const row of data) {
				results.push(await this.#insertOne(table, row));
			}
			return results;
		}

		return this.#insertOne(table, data);
	}

	async #insertOne<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Insert<T>,
	): Promise<Infer<T>> {
		if (table.meta.isPartial) {
			throw new Error(
				`Cannot insert into partial table "${table.name}". Use the full table definition instead.`,
			);
		}

		if ((table.meta as any).isDerived) {
			throw new Error(
				`Cannot insert into derived table "${table.name}". Derived tables are SELECT-only.`,
			);
		}

		const dataWithSchemaExprs = injectSchemaExpressions(
			table,
			data as Record<string, unknown>,
			"insert",
		);

		const {regularData, expressions, symbols} = extractDBExpressions(
			dataWithSchemaExprs,
			table,
		);

		let schema = table.schema;
		const skipFields = {...expressions, ...symbols};
		if (Object.keys(skipFields).length > 0) {
			const skipFieldSchemas = Object.keys(skipFields).reduce(
				(acc, key) => {
					acc[key] = (table.schema.shape as any)[key].optional();
					return acc;
				},
				{} as Record<string, z.ZodTypeAny>,
			);
			schema = table.schema.extend(skipFieldSchemas);
		}
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			schema,
			regularData,
		);
		const encoded = encodeData(table, validated);

		const insertParts = buildInsertParts(
			table.name,
			encoded,
			expressions,
			symbols,
		);

		if (this.#driver.supportsReturning) {
			const {strings, values} = appendReturning(insertParts);
			const row = await this.#driver.get<Record<string, unknown>>(
				strings,
				values,
			);
			return decodeData(table, row) as Infer<T>;
		}

		// Fallback: INSERT then SELECT
		await this.#driver.run(insertParts.strings, insertParts.values);

		const pk = table.meta.primary;
		const pkValue = pk
			? (encoded[pk] ?? (expressions[pk] ? undefined : null))
			: null;
		if (pk && pkValue !== undefined && pkValue !== null) {
			const {strings, values} = buildSelectByPkParts(table.name, pk, pkValue);
			const row = await this.#driver.get<Record<string, unknown>>(
				strings,
				values,
			);
			if (row) {
				return decodeData(table, row) as Infer<T>;
			}
		}

		return validated as Infer<T>;
	}

	update<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		id: string | number,
	): Promise<Infer<T> | null>;
	update<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		ids: (string | number)[],
	): Promise<(Infer<T> | null)[]>;
	update<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
	): TaggedQuery<Infer<T>[]>;
	update<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		idOrIds?: string | number | (string | number)[],
	):
		| Promise<Infer<T> | null>
		| Promise<(Infer<T> | null)[]>
		| TaggedQuery<Infer<T>[]> {
		if (idOrIds === undefined) {
			return async (strings: TemplateStringsArray, ...values: unknown[]) => {
				return this.#updateWithWhere(table, data, strings, values);
			};
		}

		if (Array.isArray(idOrIds)) {
			return this.#updateByIds(table, data, idOrIds);
		}

		return this.#updateById(table, data, idOrIds);
	}

	async #updateById<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		id: string | number,
	): Promise<Infer<T> | null> {
		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		if ((table.meta as any).isDerived) {
			throw new Error(
				`Cannot update derived table "${table.name}". Derived tables are SELECT-only.`,
			);
		}

		const dataWithSchemaExprs = injectSchemaExpressions(
			table,
			data as Record<string, unknown>,
			"update",
		);

		const {regularData, expressions, symbols} = extractDBExpressions(
			dataWithSchemaExprs,
			table,
		);

		const partialSchema = table.schema.partial();
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			partialSchema,
			regularData,
		);

		const allColumns = [
			...Object.keys(validated),
			...Object.keys(expressions),
			...Object.keys(symbols),
		];
		if (allColumns.length === 0) {
			throw new Error("No fields to update");
		}

		const encoded = encodeData(table, validated);
		const updateParts = buildUpdateByIdParts(
			table.name,
			pk,
			encoded,
			expressions,
			id,
			symbols,
		);

		if (this.#driver.supportsReturning) {
			const {strings, values} = appendReturning(updateParts);
			const row = await this.#driver.get<Record<string, unknown>>(
				strings,
				values,
			);
			if (!row) return null;
			return decodeData(table, row) as Infer<T>;
		}

		// Fallback: UPDATE then SELECT
		await this.#driver.run(updateParts.strings, updateParts.values);

		const {strings: selectStrings, values: selectValues} = buildSelectByPkParts(
			table.name,
			pk,
			id,
		);
		const row = await this.#driver.get<Record<string, unknown>>(
			selectStrings,
			selectValues,
		);
		if (!row) return null;
		return decodeData(table, row) as Infer<T>;
	}

	async #updateByIds<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		ids: (string | number)[],
	): Promise<(Infer<T> | null)[]> {
		if (ids.length === 0) {
			return [];
		}

		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		if ((table.meta as any).isDerived) {
			throw new Error(
				`Cannot update derived table "${table.name}". Derived tables are SELECT-only.`,
			);
		}

		const dataWithSchemaExprs = injectSchemaExpressions(
			table,
			data as Record<string, unknown>,
			"update",
		);

		const {regularData, expressions, symbols} = extractDBExpressions(
			dataWithSchemaExprs,
			table,
		);

		const partialSchema = table.schema.partial();
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			partialSchema,
			regularData,
		);

		const allColumns = [
			...Object.keys(validated),
			...Object.keys(expressions),
			...Object.keys(symbols),
		];
		if (allColumns.length === 0) {
			throw new Error("No fields to update");
		}

		const encoded = encodeData(table, validated);
		const updateParts = buildUpdateByIdsParts(
			table.name,
			pk,
			encoded,
			expressions,
			ids,
			symbols,
		);

		if (this.#driver.supportsReturning) {
			const {strings, values} = appendReturning(updateParts);
			const rows = await this.#driver.all<Record<string, unknown>>(
				strings,
				values,
			);

			const resultMap = new Map<string | number, Infer<T>>();
			for (const row of rows) {
				const entity = decodeData(table, row) as Infer<T>;
				resultMap.set(row[pk] as string | number, entity);
			}

			return ids.map((id) => resultMap.get(id) ?? null);
		}

		// Fallback: UPDATE then SELECT
		await this.#driver.run(updateParts.strings, updateParts.values);

		const {strings: selectStrings, values: selectValues} =
			buildSelectByPksParts(table.name, pk, ids);
		const rows = await this.#driver.all<Record<string, unknown>>(
			selectStrings,
			selectValues,
		);

		const resultMap = new Map<string | number, Infer<T>>();
		for (const row of rows) {
			const entity = decodeData(table, row) as Infer<T>;
			resultMap.set(row[pk] as string | number, entity);
		}

		return ids.map((id) => resultMap.get(id) ?? null);
	}

	async #updateWithWhere<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		strings: TemplateStringsArray,
		templateValues: unknown[],
	): Promise<Infer<T>[]> {
		if ((table.meta as any).isDerived) {
			throw new Error(
				`Cannot update derived table "${table.name}". Derived tables are SELECT-only.`,
			);
		}

		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const dataWithSchemaExprs = injectSchemaExpressions(
			table,
			data as Record<string, unknown>,
			"update",
		);

		const {regularData, expressions} = extractDBExpressions(
			dataWithSchemaExprs,
			table,
		);

		const partialSchema = table.schema.partial();
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			partialSchema,
			regularData,
		);

		const allColumns = [...Object.keys(validated), ...Object.keys(expressions)];
		if (allColumns.length === 0) {
			throw new Error("No fields to update");
		}

		const encoded = encodeData(table, validated);

		// Build UPDATE template: UPDATE <table> SET <col1> = <val1>, ... WHERE ...
		const {strings: whereStrings, values: whereValues} = expandFragments(
			strings,
			templateValues,
		);

		const setCols = Object.keys(encoded);
		const exprCols = Object.keys(expressions);

		const queryStrings: string[] = ["UPDATE "];
		const queryValues: unknown[] = [];

		// Table name
		queryValues.push(ident(table.name));
		queryStrings.push(" SET ");

		// SET assignments for regular values
		for (let i = 0; i < setCols.length; i++) {
			const col = setCols[i];
			queryValues.push(ident(col));
			queryStrings.push(" = ");
			queryValues.push(encoded[col]);
			queryStrings.push(i < setCols.length - 1 ? ", " : "");
		}

		// Expression assignments using mergeExpression
		for (let i = 0; i < exprCols.length; i++) {
			if (setCols.length > 0 || i > 0) {
				queryStrings[queryStrings.length - 1] += ", ";
			}
			queryValues.push(ident(exprCols[i]));
			queryStrings.push(" = ");
			mergeExpression(queryStrings, queryValues, expressions[exprCols[i]]);
		}

		// Merge WHERE template
		queryStrings[queryStrings.length - 1] += " " + whereStrings[0];
		for (let i = 1; i < whereStrings.length; i++) {
			queryStrings.push(whereStrings[i]);
		}
		queryValues.push(...whereValues);

		if (this.#driver.supportsReturning) {
			queryStrings[queryStrings.length - 1] += " RETURNING *";
			const rows = await this.#driver.all<Record<string, unknown>>(
				makeTemplate(queryStrings),
				queryValues,
			);
			return rows.map((row) => decodeData(table, row) as Infer<T>);
		}

		// Fallback: Get IDs first, then UPDATE, then SELECT
		// Build SELECT to get IDs first
		const selectIdStrings: string[] = ["SELECT "];
		const selectIdValues: unknown[] = [];
		selectIdValues.push(ident(pk));
		selectIdStrings.push(" FROM ");
		selectIdValues.push(ident(table.name));
		selectIdStrings.push(" " + whereStrings[0]);
		for (let i = 1; i < whereStrings.length; i++) {
			selectIdStrings.push(whereStrings[i]);
		}
		selectIdValues.push(...whereValues);

		const idRows = await this.#driver.all<Record<string, unknown>>(
			makeTemplate(selectIdStrings),
			selectIdValues,
		);
		const ids = idRows.map((r) => r[pk] as string | number);

		if (ids.length === 0) {
			return [];
		}

		// Run UPDATE
		await this.#driver.run(makeTemplate(queryStrings), queryValues);

		// SELECT by IDs
		const {strings: selectStrings, values: selectVals} = buildSelectByPksParts(
			table.name,
			pk,
			ids,
		);
		const rows = await this.#driver.all<Record<string, unknown>>(
			selectStrings,
			selectVals,
		);

		return rows.map((row) => decodeData(table, row) as Infer<T>);
	}

	delete<T extends Table<any>>(table: T, id: string | number): Promise<number>;
	delete<T extends Table<any>>(
		table: T,
		ids: (string | number)[],
	): Promise<number>;
	delete<T extends Table<any>>(table: T): TaggedQuery<number>;
	delete<T extends Table<any>>(
		table: T,
		idOrIds?: string | number | (string | number)[],
	): Promise<number> | TaggedQuery<number> {
		if (idOrIds === undefined) {
			return async (strings: TemplateStringsArray, ...values: unknown[]) => {
				const {strings: expandedStrings, values: expandedValues} =
					expandFragments(strings, values);
				// Build: DELETE FROM <table> <where>
				const deleteStrings: string[] = ["DELETE FROM "];
				const deleteValues: unknown[] = [ident(table.name)];
				deleteStrings.push(" " + expandedStrings[0]);
				for (let i = 1; i < expandedStrings.length; i++) {
					deleteStrings.push(expandedStrings[i]);
				}
				deleteValues.push(...expandedValues);
				return this.#driver.run(makeTemplate(deleteStrings), deleteValues);
			};
		}

		if (Array.isArray(idOrIds)) {
			return this.#deleteByIds(table, idOrIds);
		}

		return this.#deleteById(table, idOrIds);
	}

	async #deleteById<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<number> {
		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const {strings, values} = buildDeleteByPkParts(table.name, pk, id);
		return this.#driver.run(strings, values);
	}

	async #deleteByIds<T extends Table<any>>(
		table: T,
		ids: (string | number)[],
	): Promise<number> {
		if (ids.length === 0) {
			return 0;
		}

		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const {strings, values} = buildDeleteByPksParts(table.name, pk, ids);
		return this.#driver.run(strings, values);
	}

	softDelete<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<number>;
	softDelete<T extends Table<any>>(
		table: T,
		ids: (string | number)[],
	): Promise<number>;
	softDelete<T extends Table<any>>(table: T): TaggedQuery<number>;
	softDelete<T extends Table<any>>(
		table: T,
		idOrIds?: string | number | (string | number)[],
	): Promise<number> | TaggedQuery<number> {
		const softDeleteField = table.meta.softDeleteField;
		if (!softDeleteField) {
			throw new Error(
				`Table ${table.name} does not have a soft delete field. Use softDelete() wrapper to mark a field.`,
			);
		}

		if (idOrIds === undefined) {
			return async (strings: TemplateStringsArray, ...values: unknown[]) => {
				return this.#softDeleteWithWhere(table, strings, values);
			};
		}

		if (Array.isArray(idOrIds)) {
			return this.#softDeleteByIds(table, idOrIds);
		}

		return this.#softDeleteById(table, idOrIds);
	}

	async #softDeleteById<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<number> {
		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const softDeleteField = table.meta.softDeleteField!;

		const schemaExprs = injectSchemaExpressions(table, {}, "update");
		const {expressions, symbols} = extractDBExpressions(schemaExprs);

		// Build: UPDATE <table> SET <softDeleteField> = CURRENT_TIMESTAMP, ... WHERE <pk> = <id>
		const queryStrings: string[] = ["UPDATE "];
		const queryValues: unknown[] = [];

		queryValues.push(ident(table.name));
		queryStrings.push(" SET ");

		// First assignment: softDeleteField = CURRENT_TIMESTAMP
		queryValues.push(ident(softDeleteField));
		queryStrings.push(" = CURRENT_TIMESTAMP");

		// Expression assignments
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(" = ");
				mergeExpression(queryStrings, queryValues, expr);
			}
		}

		// Symbol assignments
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(` = ${resolveSQLSymbol(sym)}`);
			}
		}

		// WHERE clause
		queryStrings[queryStrings.length - 1] += " WHERE ";
		queryValues.push(ident(pk));
		queryStrings.push(" = ");
		queryValues.push(id);
		queryStrings.push("");

		return this.#driver.run(makeTemplate(queryStrings), queryValues);
	}

	async #softDeleteByIds<T extends Table<any>>(
		table: T,
		ids: (string | number)[],
	): Promise<number> {
		if (ids.length === 0) {
			return 0;
		}

		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const softDeleteField = table.meta.softDeleteField!;

		const schemaExprs = injectSchemaExpressions(table, {}, "update");
		const {expressions, symbols} = extractDBExpressions(schemaExprs);

		// Build: UPDATE <table> SET <softDeleteField> = CURRENT_TIMESTAMP, ... WHERE <pk> IN (...)
		const queryStrings: string[] = ["UPDATE "];
		const queryValues: unknown[] = [];

		queryValues.push(ident(table.name));
		queryStrings.push(" SET ");

		// First assignment: softDeleteField = CURRENT_TIMESTAMP
		queryValues.push(ident(softDeleteField));
		queryStrings.push(" = CURRENT_TIMESTAMP");

		// Expression assignments
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(" = ");
				mergeExpression(queryStrings, queryValues, expr);
			}
		}

		// Symbol assignments
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(` = ${resolveSQLSymbol(sym)}`);
			}
		}

		// WHERE clause with IN
		queryStrings[queryStrings.length - 1] += " WHERE ";
		queryValues.push(ident(pk));
		queryStrings.push(" IN (");

		for (let i = 0; i < ids.length; i++) {
			queryValues.push(ids[i]);
			queryStrings.push(i < ids.length - 1 ? ", " : ")");
		}

		return this.#driver.run(makeTemplate(queryStrings), queryValues);
	}

	async #softDeleteWithWhere<T extends Table<any>>(
		table: T,
		strings: TemplateStringsArray,
		templateValues: unknown[],
	): Promise<number> {
		const softDeleteField = table.meta.softDeleteField!;

		const schemaExprs = injectSchemaExpressions(table, {}, "update");
		const {expressions, symbols} = extractDBExpressions(schemaExprs);

		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			templateValues,
		);

		// Build: UPDATE <table> SET <softDeleteField> = CURRENT_TIMESTAMP, ... <where>
		const queryStrings: string[] = ["UPDATE "];
		const queryValues: unknown[] = [];

		queryValues.push(ident(table.name));
		queryStrings.push(" SET ");

		// First assignment: softDeleteField = CURRENT_TIMESTAMP
		queryValues.push(ident(softDeleteField));
		queryStrings.push(" = CURRENT_TIMESTAMP");

		// Expression assignments
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(" = ");
				mergeExpression(queryStrings, queryValues, expr);
			}
		}

		// Symbol assignments
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(` = ${resolveSQLSymbol(sym)}`);
			}
		}

		// Merge WHERE template
		queryStrings[queryStrings.length - 1] += " " + expandedStrings[0];
		for (let i = 1; i < expandedStrings.length; i++) {
			queryStrings.push(expandedStrings[i]);
		}
		queryValues.push(...expandedValues);

		return this.#driver.run(makeTemplate(queryStrings), queryValues);
	}

	// ==========================================================================
	// Raw - No Normalization
	// ==========================================================================

	async query<T = Record<string, unknown>>(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<T[]> {
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			values,
		);
		return this.#driver.all<T>(expandedStrings, expandedValues);
	}

	async exec(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<number> {
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			values,
		);
		return this.#driver.run(expandedStrings, expandedValues);
	}

	async val<T>(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<T | null> {
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			values,
		);
		return this.#driver.val<T>(expandedStrings, expandedValues);
	}

	// ==========================================================================
	// Debugging
	// ==========================================================================

	/**
	 * Print the generated SQL and parameters without executing.
	 * Useful for debugging query composition and fragment expansion.
	 * Note: SQL shown uses ? placeholders; actual query may use $1, $2 etc.
	 */
	print(
		strings: TemplateStringsArray,
		...values: unknown[]
	): {sql: string; params: unknown[]} {
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			values,
		);
		// Join with ? placeholders for display
		let sql = expandedStrings[0];
		for (let i = 1; i < expandedStrings.length; i++) {
			sql += "?" + expandedStrings[i];
		}
		return {sql, params: expandedValues};
	}
}

// ============================================================================
// Database
// ============================================================================

/**
 * Database wrapper with typed queries and entity normalization.
 * Extends EventTarget for IndexedDB-style "upgradeneeded" events.
 *
 * @example
 * const db = new Database(driver);
 *
 * db.addEventListener("upgradeneeded", (e) => {
 *   e.waitUntil(runMigrations(e));
 * });
 *
 * await db.open(2);
 */
export class Database extends EventTarget {
	#driver: Driver;
	#version: number = 0;
	#opened: boolean = false;

	constructor(driver: Driver) {
		super();
		this.#driver = driver;
	}

	/**
	 * Current database schema version.
	 * Returns 0 if database has never been opened.
	 */
	get version(): number {
		return this.#version;
	}

	/**
	 * Open the database at a specific version.
	 *
	 * If the requested version is higher than the current version,
	 * fires an "upgradeneeded" event and waits for all waitUntil()
	 * promises before completing.
	 *
	 * Migration safety: Uses exclusive locking to prevent race conditions
	 * when multiple processes attempt migrations simultaneously.
	 *
	 * @example
	 * db.addEventListener("upgradeneeded", (e) => {
	 *   e.waitUntil(runMigrations(e));
	 * });
	 * await db.open(2);
	 */
	async open(version: number): Promise<void> {
		if (this.#opened) {
			throw new Error("Database already opened");
		}

		// Run migration logic inside lock
		const runMigration = async (): Promise<void> => {
			// Create table inside lock to prevent race conditions when
			// multiple processes start simultaneously
			await this.#ensureMigrationsTable();

			const currentVersion = await this.#getCurrentVersionLocked();

			if (version > currentVersion) {
				const event = new DatabaseUpgradeEvent("upgradeneeded", {
					oldVersion: currentVersion,
					newVersion: version,
				});
				this.dispatchEvent(event);
				await event._settle();

				await this.#setVersion(version);
			}
		};

		// Use driver's migration lock if available, otherwise use transaction
		if (this.#driver.withMigrationLock) {
			await this.#driver.withMigrationLock(runMigration);
		} else {
			// Fallback: Use driver's transaction for locking
			await this.#driver.transaction(runMigration);
		}

		this.#version = version;
		this.#opened = true;
	}

	// ==========================================================================
	// Migration Table Helpers
	// ==========================================================================

	async #ensureMigrationsTable(): Promise<void> {
		// Simple migrations table - just tracks versions
		// No applied_at default (MySQL doesn't support DEFAULT CURRENT_TIMESTAMP on TEXT)
		const createTable = makeTemplate([
			`CREATE TABLE IF NOT EXISTS _migrations (
				version INTEGER PRIMARY KEY
			)`,
		]);
		await this.#driver.run(createTable, []);
	}

	async #getCurrentVersionLocked(): Promise<number> {
		// Locking is handled by withMigrationLock() or transaction wrapper
		const selectVersion = makeTemplate([
			`SELECT MAX(version) as version FROM _migrations`,
		]);
		const row = await this.#driver.get<{version: number}>(selectVersion, []);
		return row?.version ?? 0;
	}

	async #setVersion(version: number): Promise<void> {
		const insertVersion = makeTemplate([
			`INSERT INTO _migrations (version) VALUES (`,
			`)`,
		]);
		await this.#driver.run(insertVersion, [version]);
	}

	// ==========================================================================
	// Queries - Return Normalized Entities
	// ==========================================================================

	/**
	 * Query multiple entities with joins and reference resolution.
	 *
	 * @example
	 * // Single table
	 * const posts = await db.all(Posts)`WHERE published = ${true}`;
	 *
	 * // Multi-table with joins
	 * const posts = await db.all([Posts, Users])`
	 *   JOIN users ON users.id = posts.author_id
	 *   WHERE published = ${true}
	 * `;
	 * posts[0].author.name  // "Alice"
	 */
	all<T extends Table<any>>(tables: T | T[]): TaggedQuery<Infer<T>[]> {
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const {strings: colStrings, values: colValues} =
				buildSelectCols(tableArray);
			const {strings: expandedStrings, values: expandedValues} =
				expandFragments(strings, values);

			// Build: SELECT <cols> FROM <table> <where>
			const queryStrings: string[] = ["SELECT "];
			const queryValues: unknown[] = [];

			// Merge column template
			queryStrings[0] += colStrings[0];
			for (let i = 1; i < colStrings.length; i++) {
				queryStrings.push(colStrings[i]);
			}
			queryValues.push(...colValues);

			// Add FROM <table>
			queryStrings[queryStrings.length - 1] += " FROM ";
			queryValues.push(ident(tableArray[0].name));
			queryStrings.push(" ");

			// Merge WHERE template
			queryStrings[queryStrings.length - 1] += expandedStrings[0];
			for (let i = 1; i < expandedStrings.length; i++) {
				queryStrings.push(expandedStrings[i]);
			}
			queryValues.push(...expandedValues);

			const rows = await this.#driver.all<Record<string, unknown>>(
				makeTemplate(queryStrings),
				queryValues,
			);
			return normalize<Infer<T>>(rows, tableArray as Table<any>[]);
		};
	}

	/**
	 * Query a single entity.
	 *
	 * @example
	 * // By primary key
	 * const post = await db.get(Posts, postId);
	 *
	 * // With query
	 * const post = await db.get(Posts)`WHERE slug = ${slug}`;
	 *
	 * // Multi-table
	 * const post = await db.get([Posts, Users])`
	 *   JOIN users ON users.id = posts.author_id
	 *   WHERE posts.id = ${postId}
	 * `;
	 */
	get<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<Infer<T> | null>;
	get<T extends Table<any>>(tables: T | T[]): TaggedQuery<Infer<T> | null>;
	get<T extends Table<any>>(
		tables: T | T[],
		id?: string | number,
	): Promise<Infer<T> | null> | TaggedQuery<Infer<T> | null> {
		// Convenience overload: get by primary key
		if (id !== undefined) {
			const table = tables as T;
			const pk = table.meta.primary;
			if (!pk) {
				return Promise.reject(
					new Error(`Table ${table.name} has no primary key defined`),
				);
			}
			const {strings, values} = buildSelectByPkParts(table.name, pk, id);
			return this.#driver
				.get<Record<string, unknown>>(strings, values)
				.then((row) => {
					if (!row) return null;
					return decodeData(table, row) as Infer<T>;
				});
		}

		// Tagged template query
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const {strings: colStrings, values: colValues} =
				buildSelectCols(tableArray);
			const {strings: expandedStrings, values: expandedValues} =
				expandFragments(strings, values);

			// Build: SELECT <cols> FROM <table> <where>
			const queryStrings: string[] = ["SELECT "];
			const queryValues: unknown[] = [];

			// Merge column template
			queryStrings[0] += colStrings[0];
			for (let i = 1; i < colStrings.length; i++) {
				queryStrings.push(colStrings[i]);
			}
			queryValues.push(...colValues);

			// Add FROM <table>
			queryStrings[queryStrings.length - 1] += " FROM ";
			queryValues.push(ident(tableArray[0].name));
			queryStrings.push(" ");

			// Merge WHERE template
			queryStrings[queryStrings.length - 1] += expandedStrings[0];
			for (let i = 1; i < expandedStrings.length; i++) {
				queryStrings.push(expandedStrings[i]);
			}
			queryValues.push(...expandedValues);

			const row = await this.#driver.get<Record<string, unknown>>(
				makeTemplate(queryStrings),
				queryValues,
			);
			return normalizeOne<Infer<T>>(row, tableArray as Table<any>[]);
		};
	}

	// ==========================================================================
	// Mutations - Validate Through Zod
	// ==========================================================================

	/**
	 * Insert one or more entities.
	 *
	 * Uses RETURNING to get the actual inserted row(s) (with DB defaults).
	 *
	 * @example
	 * // Single insert
	 * const user = await db.insert(Users, {
	 *   id: crypto.randomUUID(),
	 *   email: "alice@example.com",
	 *   name: "Alice",
	 * });
	 *
	 * // Bulk insert
	 * const users = await db.insert(Users, [
	 *   { id: "1", email: "alice@example.com", name: "Alice" },
	 *   { id: "2", email: "bob@example.com", name: "Bob" },
	 * ]);
	 */
	async insert<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Insert<T>,
	): Promise<Infer<T>>;
	async insert<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Insert<T>[],
	): Promise<Infer<T>[]>;
	async insert<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Insert<T> | Insert<T>[],
	): Promise<Infer<T> | Infer<T>[]> {
		// Handle array insert
		if (Array.isArray(data)) {
			if (data.length === 0) {
				return [];
			}
			// Insert each row and collect results
			const results: Infer<T>[] = [];
			for (const row of data) {
				results.push(await this.#insertOne(table, row));
			}
			return results;
		}

		return this.#insertOne(table, data);
	}

	async #insertOne<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Insert<T>,
	): Promise<Infer<T>> {
		if (table.meta.isPartial) {
			throw new Error(
				`Cannot insert into partial table "${table.name}". Use the full table definition instead.`,
			);
		}

		if ((table.meta as any).isDerived) {
			throw new Error(
				`Cannot insert into derived table "${table.name}". Derived tables are SELECT-only.`,
			);
		}

		const dataWithSchemaExprs = injectSchemaExpressions(
			table,
			data as Record<string, unknown>,
			"insert",
		);

		const {regularData, expressions, symbols} = extractDBExpressions(
			dataWithSchemaExprs,
			table,
		);

		let schema = table.schema;
		const skipFields = {...expressions, ...symbols};
		if (Object.keys(skipFields).length > 0) {
			const skipFieldSchemas = Object.keys(skipFields).reduce(
				(acc, key) => {
					acc[key] = (table.schema.shape as any)[key].optional();
					return acc;
				},
				{} as Record<string, z.ZodTypeAny>,
			);
			schema = table.schema.extend(skipFieldSchemas);
		}
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			schema,
			regularData,
		);
		const encoded = encodeData(table, validated);

		const insertParts = buildInsertParts(
			table.name,
			encoded,
			expressions,
			symbols,
		);

		if (this.#driver.supportsReturning) {
			const {strings, values} = appendReturning(insertParts);
			const row = await this.#driver.get<Record<string, unknown>>(
				strings,
				values,
			);
			return decodeData(table, row) as Infer<T>;
		}

		// Fallback: INSERT then SELECT
		await this.#driver.run(insertParts.strings, insertParts.values);

		const pk = table.meta.primary;
		const pkValue = pk
			? (encoded[pk] ?? (expressions[pk] ? undefined : null))
			: null;
		if (pk && pkValue !== undefined && pkValue !== null) {
			const {strings, values} = buildSelectByPkParts(table.name, pk, pkValue);
			const row = await this.#driver.get<Record<string, unknown>>(
				strings,
				values,
			);
			if (row) {
				return decodeData(table, row) as Infer<T>;
			}
		}

		return validated as Infer<T>;
	}

	/**
	 * Update entities.
	 *
	 * @example
	 * // Update by primary key
	 * const user = await db.update(Users, { name: "Bob" }, userId);
	 *
	 * // Update multiple by primary keys
	 * const users = await db.update(Users, { active: true }, [id1, id2, id3]);
	 *
	 * // Update with custom WHERE clause
	 * const count = await db.update(Users, { active: false })`WHERE lastLogin < ${cutoff}`;
	 */
	update<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		id: string | number,
	): Promise<Infer<T> | null>;
	update<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		ids: (string | number)[],
	): Promise<(Infer<T> | null)[]>;
	update<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
	): TaggedQuery<Infer<T>[]>;
	update<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		idOrIds?: string | number | (string | number)[],
	):
		| Promise<Infer<T> | null>
		| Promise<(Infer<T> | null)[]>
		| TaggedQuery<Infer<T>[]> {
		// Template overload - update with custom WHERE
		if (idOrIds === undefined) {
			return async (strings: TemplateStringsArray, ...values: unknown[]) => {
				return this.#updateWithWhere(table, data, strings, values);
			};
		}

		// Array of IDs
		if (Array.isArray(idOrIds)) {
			return this.#updateByIds(table, data, idOrIds);
		}

		// Single ID
		return this.#updateById(table, data, idOrIds);
	}

	async #updateById<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		id: string | number,
	): Promise<Infer<T> | null> {
		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		if ((table.meta as any).isDerived) {
			throw new Error(
				`Cannot update derived table "${table.name}". Derived tables are SELECT-only.`,
			);
		}

		const dataWithSchemaExprs = injectSchemaExpressions(
			table,
			data as Record<string, unknown>,
			"update",
		);

		const {regularData, expressions, symbols} = extractDBExpressions(
			dataWithSchemaExprs,
			table,
		);

		const partialSchema = table.schema.partial();
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			partialSchema,
			regularData,
		);

		const allColumns = [
			...Object.keys(validated),
			...Object.keys(expressions),
			...Object.keys(symbols),
		];
		if (allColumns.length === 0) {
			throw new Error("No fields to update");
		}

		const encoded = encodeData(table, validated);
		const updateParts = buildUpdateByIdParts(
			table.name,
			pk,
			encoded,
			expressions,
			id,
			symbols,
		);

		if (this.#driver.supportsReturning) {
			const {strings, values} = appendReturning(updateParts);
			const row = await this.#driver.get<Record<string, unknown>>(
				strings,
				values,
			);
			if (!row) return null;
			return decodeData(table, row) as Infer<T>;
		}

		// Fallback: UPDATE then SELECT
		await this.#driver.run(updateParts.strings, updateParts.values);

		const {strings: selectStrings, values: selectValues} = buildSelectByPkParts(
			table.name,
			pk,
			id,
		);
		const row = await this.#driver.get<Record<string, unknown>>(
			selectStrings,
			selectValues,
		);
		if (!row) return null;
		return decodeData(table, row) as Infer<T>;
	}

	async #updateByIds<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		ids: (string | number)[],
	): Promise<(Infer<T> | null)[]> {
		if (ids.length === 0) {
			return [];
		}

		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		if ((table.meta as any).isDerived) {
			throw new Error(
				`Cannot update derived table "${table.name}". Derived tables are SELECT-only.`,
			);
		}

		const dataWithSchemaExprs = injectSchemaExpressions(
			table,
			data as Record<string, unknown>,
			"update",
		);

		const {regularData, expressions, symbols} = extractDBExpressions(
			dataWithSchemaExprs,
			table,
		);

		const partialSchema = table.schema.partial();
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			partialSchema,
			regularData,
		);

		const allColumns = [
			...Object.keys(validated),
			...Object.keys(expressions),
			...Object.keys(symbols),
		];
		if (allColumns.length === 0) {
			throw new Error("No fields to update");
		}

		const encoded = encodeData(table, validated);
		const updateParts = buildUpdateByIdsParts(
			table.name,
			pk,
			encoded,
			expressions,
			ids,
			symbols,
		);

		if (this.#driver.supportsReturning) {
			const {strings, values} = appendReturning(updateParts);
			const rows = await this.#driver.all<Record<string, unknown>>(
				strings,
				values,
			);

			const resultMap = new Map<string | number, Infer<T>>();
			for (const row of rows) {
				const entity = decodeData(table, row) as Infer<T>;
				resultMap.set(row[pk] as string | number, entity);
			}

			return ids.map((id) => resultMap.get(id) ?? null);
		}

		// Fallback: UPDATE then SELECT
		await this.#driver.run(updateParts.strings, updateParts.values);

		const {strings: selectStrings, values: selectValues} =
			buildSelectByPksParts(table.name, pk, ids);
		const rows = await this.#driver.all<Record<string, unknown>>(
			selectStrings,
			selectValues,
		);

		const resultMap = new Map<string | number, Infer<T>>();
		for (const row of rows) {
			const entity = decodeData(table, row) as Infer<T>;
			resultMap.set(row[pk] as string | number, entity);
		}

		return ids.map((id) => resultMap.get(id) ?? null);
	}

	async #updateWithWhere<T extends Table<any>>(
		table: T & FullTableOnly<T>,
		data: Partial<Insert<T>>,
		strings: TemplateStringsArray,
		templateValues: unknown[],
	): Promise<Infer<T>[]> {
		if ((table.meta as any).isDerived) {
			throw new Error(
				`Cannot update derived table "${table.name}". Derived tables are SELECT-only.`,
			);
		}

		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const dataWithSchemaExprs = injectSchemaExpressions(
			table,
			data as Record<string, unknown>,
			"update",
		);

		const {regularData, expressions} = extractDBExpressions(
			dataWithSchemaExprs,
			table,
		);

		const partialSchema = table.schema.partial();
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			partialSchema,
			regularData,
		);

		const allColumns = [...Object.keys(validated), ...Object.keys(expressions)];
		if (allColumns.length === 0) {
			throw new Error("No fields to update");
		}

		const encoded = encodeData(table, validated);

		// Build UPDATE template: UPDATE <table> SET <col1> = <val1>, ... WHERE ...
		const {strings: whereStrings, values: whereValues} = expandFragments(
			strings,
			templateValues,
		);

		const setCols = Object.keys(encoded);
		const exprCols = Object.keys(expressions);

		const queryStrings: string[] = ["UPDATE "];
		const queryValues: unknown[] = [];

		// Table name
		queryValues.push(ident(table.name));
		queryStrings.push(" SET ");

		// SET assignments for regular values
		for (let i = 0; i < setCols.length; i++) {
			const col = setCols[i];
			queryValues.push(ident(col));
			queryStrings.push(" = ");
			queryValues.push(encoded[col]);
			queryStrings.push(i < setCols.length - 1 ? ", " : "");
		}

		// Expression assignments using mergeExpression
		for (let i = 0; i < exprCols.length; i++) {
			if (setCols.length > 0 || i > 0) {
				queryStrings[queryStrings.length - 1] += ", ";
			}
			queryValues.push(ident(exprCols[i]));
			queryStrings.push(" = ");
			mergeExpression(queryStrings, queryValues, expressions[exprCols[i]]);
		}

		// Merge WHERE template
		queryStrings[queryStrings.length - 1] += " " + whereStrings[0];
		for (let i = 1; i < whereStrings.length; i++) {
			queryStrings.push(whereStrings[i]);
		}
		queryValues.push(...whereValues);

		if (this.#driver.supportsReturning) {
			queryStrings[queryStrings.length - 1] += " RETURNING *";
			const rows = await this.#driver.all<Record<string, unknown>>(
				makeTemplate(queryStrings),
				queryValues,
			);
			return rows.map((row) => decodeData(table, row) as Infer<T>);
		}

		// Fallback: Get IDs first, then UPDATE, then SELECT
		// Build SELECT to get IDs first
		const selectIdStrings: string[] = ["SELECT "];
		const selectIdValues: unknown[] = [];
		selectIdValues.push(ident(pk));
		selectIdStrings.push(" FROM ");
		selectIdValues.push(ident(table.name));
		selectIdStrings.push(" " + whereStrings[0]);
		for (let i = 1; i < whereStrings.length; i++) {
			selectIdStrings.push(whereStrings[i]);
		}
		selectIdValues.push(...whereValues);

		const idRows = await this.#driver.all<Record<string, unknown>>(
			makeTemplate(selectIdStrings),
			selectIdValues,
		);
		const ids = idRows.map((r) => r[pk] as string | number);

		if (ids.length === 0) {
			return [];
		}

		// Run UPDATE
		await this.#driver.run(makeTemplate(queryStrings), queryValues);

		// SELECT by IDs
		const {strings: selectStrings, values: selectVals} = buildSelectByPksParts(
			table.name,
			pk,
			ids,
		);
		const rows = await this.#driver.all<Record<string, unknown>>(
			selectStrings,
			selectVals,
		);

		return rows.map((row) => decodeData(table, row) as Infer<T>);
	}

	/**
	 * Delete entities.
	 *
	 * @example
	 * // Delete by primary key (returns 0 or 1)
	 * const count = await db.delete(Users, userId);
	 *
	 * // Delete multiple by primary keys
	 * const count = await db.delete(Users, [id1, id2, id3]);
	 *
	 * // Delete with custom WHERE clause
	 * const count = await db.delete(Users)`WHERE inactive = ${true}`;
	 */
	delete<T extends Table<any>>(table: T, id: string | number): Promise<number>;
	delete<T extends Table<any>>(
		table: T,
		ids: (string | number)[],
	): Promise<number>;
	delete<T extends Table<any>>(table: T): TaggedQuery<number>;
	delete<T extends Table<any>>(
		table: T,
		idOrIds?: string | number | (string | number)[],
	): Promise<number> | TaggedQuery<number> {
		if (idOrIds === undefined) {
			return async (strings: TemplateStringsArray, ...values: unknown[]) => {
				const {strings: expandedStrings, values: expandedValues} =
					expandFragments(strings, values);
				// Build: DELETE FROM <table> <where>
				const deleteStrings: string[] = ["DELETE FROM "];
				const deleteValues: unknown[] = [ident(table.name)];
				deleteStrings.push(" " + expandedStrings[0]);
				for (let i = 1; i < expandedStrings.length; i++) {
					deleteStrings.push(expandedStrings[i]);
				}
				deleteValues.push(...expandedValues);
				return this.#driver.run(makeTemplate(deleteStrings), deleteValues);
			};
		}

		if (Array.isArray(idOrIds)) {
			return this.#deleteByIds(table, idOrIds);
		}

		return this.#deleteById(table, idOrIds);
	}

	async #deleteById<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<number> {
		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const {strings, values} = buildDeleteByPkParts(table.name, pk, id);
		return this.#driver.run(strings, values);
	}

	async #deleteByIds<T extends Table<any>>(
		table: T,
		ids: (string | number)[],
	): Promise<number> {
		if (ids.length === 0) {
			return 0;
		}

		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const {strings, values} = buildDeleteByPksParts(table.name, pk, ids);
		return this.#driver.run(strings, values);
	}

	/**
	 * Soft delete entities by marking the soft delete field with the current timestamp.
	 *
	 * @example
	 * // Soft delete by primary key (returns 0 or 1)
	 * const count = await db.softDelete(Users, userId);
	 *
	 * // Soft delete multiple by primary keys
	 * const count = await db.softDelete(Users, [id1, id2, id3]);
	 *
	 * // Soft delete with custom WHERE clause
	 * const count = await db.softDelete(Users)`WHERE inactive = ${true}`;
	 */
	softDelete<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<number>;
	softDelete<T extends Table<any>>(
		table: T,
		ids: (string | number)[],
	): Promise<number>;
	softDelete<T extends Table<any>>(table: T): TaggedQuery<number>;
	softDelete<T extends Table<any>>(
		table: T,
		idOrIds?: string | number | (string | number)[],
	): Promise<number> | TaggedQuery<number> {
		const softDeleteField = table.meta.softDeleteField;
		if (!softDeleteField) {
			throw new Error(
				`Table ${table.name} does not have a soft delete field. Use softDelete() wrapper to mark a field.`,
			);
		}

		// Template overload - soft delete with custom WHERE
		if (idOrIds === undefined) {
			return async (strings: TemplateStringsArray, ...values: unknown[]) => {
				return this.#softDeleteWithWhere(table, strings, values);
			};
		}

		// Array of IDs
		if (Array.isArray(idOrIds)) {
			return this.#softDeleteByIds(table, idOrIds);
		}

		// Single ID
		return this.#softDeleteById(table, idOrIds);
	}

	async #softDeleteById<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<number> {
		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const softDeleteField = table.meta.softDeleteField!;

		const schemaExprs = injectSchemaExpressions(table, {}, "update");
		const {expressions, symbols} = extractDBExpressions(schemaExprs);

		// Build: UPDATE <table> SET <softDeleteField> = CURRENT_TIMESTAMP, ... WHERE <pk> = <id>
		const queryStrings: string[] = ["UPDATE "];
		const queryValues: unknown[] = [];

		queryValues.push(ident(table.name));
		queryStrings.push(" SET ");

		// First assignment: softDeleteField = CURRENT_TIMESTAMP
		queryValues.push(ident(softDeleteField));
		queryStrings.push(" = CURRENT_TIMESTAMP");

		// Expression assignments
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(" = ");
				mergeExpression(queryStrings, queryValues, expr);
			}
		}

		// Symbol assignments
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(` = ${resolveSQLSymbol(sym)}`);
			}
		}

		// WHERE clause
		queryStrings[queryStrings.length - 1] += " WHERE ";
		queryValues.push(ident(pk));
		queryStrings.push(" = ");
		queryValues.push(id);
		queryStrings.push("");

		return this.#driver.run(makeTemplate(queryStrings), queryValues);
	}

	async #softDeleteByIds<T extends Table<any>>(
		table: T,
		ids: (string | number)[],
	): Promise<number> {
		if (ids.length === 0) {
			return 0;
		}

		const pk = table.meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const softDeleteField = table.meta.softDeleteField!;

		const schemaExprs = injectSchemaExpressions(table, {}, "update");
		const {expressions, symbols} = extractDBExpressions(schemaExprs);

		// Build: UPDATE <table> SET <softDeleteField> = CURRENT_TIMESTAMP, ... WHERE <pk> IN (...)
		const queryStrings: string[] = ["UPDATE "];
		const queryValues: unknown[] = [];

		queryValues.push(ident(table.name));
		queryStrings.push(" SET ");

		// First assignment: softDeleteField = CURRENT_TIMESTAMP
		queryValues.push(ident(softDeleteField));
		queryStrings.push(" = CURRENT_TIMESTAMP");

		// Expression assignments
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(" = ");
				mergeExpression(queryStrings, queryValues, expr);
			}
		}

		// Symbol assignments
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(` = ${resolveSQLSymbol(sym)}`);
			}
		}

		// WHERE clause with IN
		queryStrings[queryStrings.length - 1] += " WHERE ";
		queryValues.push(ident(pk));
		queryStrings.push(" IN (");

		for (let i = 0; i < ids.length; i++) {
			queryValues.push(ids[i]);
			queryStrings.push(i < ids.length - 1 ? ", " : ")");
		}

		return this.#driver.run(makeTemplate(queryStrings), queryValues);
	}

	async #softDeleteWithWhere<T extends Table<any>>(
		table: T,
		strings: TemplateStringsArray,
		templateValues: unknown[],
	): Promise<number> {
		const softDeleteField = table.meta.softDeleteField!;

		const schemaExprs = injectSchemaExpressions(table, {}, "update");
		const {expressions, symbols} = extractDBExpressions(schemaExprs);

		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			templateValues,
		);

		// Build: UPDATE <table> SET <softDeleteField> = CURRENT_TIMESTAMP, ... <where>
		const queryStrings: string[] = ["UPDATE "];
		const queryValues: unknown[] = [];

		queryValues.push(ident(table.name));
		queryStrings.push(" SET ");

		// First assignment: softDeleteField = CURRENT_TIMESTAMP
		queryValues.push(ident(softDeleteField));
		queryStrings.push(" = CURRENT_TIMESTAMP");

		// Expression assignments
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(" = ");
				mergeExpression(queryStrings, queryValues, expr);
			}
		}

		// Symbol assignments
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				queryStrings[queryStrings.length - 1] += ", ";
				queryValues.push(ident(field));
				queryStrings.push(` = ${resolveSQLSymbol(sym)}`);
			}
		}

		// Merge WHERE template
		queryStrings[queryStrings.length - 1] += " " + expandedStrings[0];
		for (let i = 1; i < expandedStrings.length; i++) {
			queryStrings.push(expandedStrings[i]);
		}
		queryValues.push(...expandedValues);

		return this.#driver.run(makeTemplate(queryStrings), queryValues);
	}

	// ==========================================================================
	// Raw - No Normalization
	// ==========================================================================

	/**
	 * Execute a raw query and return rows.
	 *
	 * @example
	 * const counts = await db.query<{ count: number }>`
	 *   SELECT COUNT(*) as count FROM posts WHERE author_id = ${userId}
	 * `;
	 */
	async query<T = Record<string, unknown>>(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<T[]> {
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			values,
		);
		return this.#driver.all<T>(expandedStrings, expandedValues);
	}

	/**
	 * Execute a statement (INSERT, UPDATE, DELETE, DDL).
	 *
	 * @example
	 * await db.exec`CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY)`;
	 */
	async exec(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<number> {
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			values,
		);
		return this.#driver.run(expandedStrings, expandedValues);
	}

	/**
	 * Execute a query and return a single value.
	 *
	 * @example
	 * const count = await db.val<number>`SELECT COUNT(*) FROM posts`;
	 */
	async val<T>(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<T | null> {
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			values,
		);
		return this.#driver.val<T>(expandedStrings, expandedValues);
	}

	// ==========================================================================
	// Debugging
	// ==========================================================================

	/**
	 * Print the generated SQL and parameters without executing.
	 * Useful for debugging query composition and fragment expansion.
	 * Note: SQL shown uses ? placeholders; actual query may use $1, $2 etc.
	 */
	print(
		strings: TemplateStringsArray,
		...values: unknown[]
	): {sql: string; params: unknown[]} {
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			values,
		);
		// Join with ? placeholders for display
		let sql = expandedStrings[0];
		for (let i = 1; i < expandedStrings.length; i++) {
			sql += "?" + expandedStrings[i];
		}
		return {sql, params: expandedValues};
	}

	// ==========================================================================
	// Schema Ensure Methods
	// ==========================================================================

	/**
	 * Ensure a table exists with its columns and indexes.
	 *
	 * **For new tables**: Creates the table with full structure including
	 * primary key, unique constraints, foreign keys, and indexes.
	 *
	 * **For existing tables**: Only performs safe, additive operations:
	 * - Adds missing columns
	 * - Adds missing non-unique indexes
	 *
	 * Unique constraints and foreign keys on existing tables require
	 * explicit `ensureConstraints()` call (they can fail or lock).
	 *
	 * @throws {EnsureError} If DDL execution fails
	 * @throws {SchemaDriftError} If existing table has missing constraints
	 *   (directs user to run ensureConstraints)
	 *
	 * @example
	 * // In migration handler
	 * await db.ensureTable(Users);
	 * await db.ensureTable(Posts); // FK to Users - ensure Users first
	 */
	async ensureTable<T extends Table<any>>(table: T): Promise<EnsureResult> {
		if (!this.#driver.ensureTable) {
			throw new Error(
				"Driver does not implement ensureTable(). " +
					"Schema ensure methods require a driver with schema management support.",
			);
		}

		const doEnsure = () => this.#driver.ensureTable!(table);

		// Wrap in migration lock if available
		if (this.#driver.withMigrationLock) {
			return await this.#driver.withMigrationLock(doEnsure);
		}
		return await doEnsure();
	}

	/**
	 * Ensure constraints (unique, foreign key) are applied to an existing table.
	 *
	 * **WARNING**: This operation can be expensive and cause locks on large tables.
	 * It performs preflight checks to detect data violations before applying constraints.
	 *
	 * For each declared constraint:
	 * 1. Preflight: Check for violations (duplicates for UNIQUE, orphans for FK)
	 * 2. If violations found: Throw ConstraintPreflightError with diagnostic query
	 * 3. If clean: Apply the constraint
	 *
	 * @throws {Error} If table doesn't exist
	 * @throws {ConstraintPreflightError} If data violates a constraint
	 * @throws {EnsureError} If DDL execution fails
	 *
	 * @example
	 * // After ensuring table structure
	 * await db.ensureTable(Users);
	 * // Explicitly apply constraints (may lock, may fail)
	 * await db.ensureConstraints(Users);
	 */
	async ensureConstraints<T extends Table<any>>(
		table: T,
	): Promise<EnsureResult> {
		if (!this.#driver.ensureConstraints) {
			throw new Error(
				"Driver does not implement ensureConstraints(). " +
					"Schema ensure methods require a driver with schema management support.",
			);
		}

		const doEnsure = () => this.#driver.ensureConstraints!(table);

		// Wrap in migration lock if available
		if (this.#driver.withMigrationLock) {
			return await this.#driver.withMigrationLock(doEnsure);
		}
		return await doEnsure();
	}

	/**
	 * Copy column data for safe rename migrations.
	 *
	 * Executes: UPDATE <table> SET <toField> = <fromField> WHERE <toField> IS NULL
	 *
	 * This is idempotent - rows where toField already has a value are skipped.
	 * The fromField may be a legacy column not in the current schema.
	 *
	 * @param table The table to update
	 * @param fromField Source column (may be legacy/not in schema)
	 * @param toField Destination column (must exist in schema)
	 * @returns Number of rows updated
	 *
	 * @example
	 * // Rename "email" to "emailAddress":
	 * // 1. Add new column
	 * await db.ensureTable(UsersWithEmailAddress);
	 * // 2. Copy data
	 * const updated = await db.copyColumn(Users, "email", "emailAddress");
	 * // 3. Later: remove old column (manual migration)
	 */
	async copyColumn<T extends Table<any>>(
		table: T,
		fromField: string,
		toField: string,
	): Promise<number> {
		// Validate toField exists in schema
		const fields = Object.keys(table.meta.fields);
		if (!fields.includes(toField)) {
			throw new Error(
				`Destination field "${toField}" does not exist in table "${table.name}". Available fields: ${fields.join(", ")}`,
			);
		}

		// Delegate to driver if it implements copyColumn
		if (this.#driver.copyColumn) {
			const doCopy = () => this.#driver.copyColumn!(table, fromField, toField);

			// Wrap in migration lock if available
			if (this.#driver.withMigrationLock) {
				return await this.#driver.withMigrationLock(doCopy);
			}
			return await doCopy();
		}

		// Default implementation: simple UPDATE statement
		const doCopy = async (): Promise<number> => {
			const tableName = table.name;

			// Validate fromField exists in actual database table
			// (it may not be in the schema if it's a legacy column being migrated away)
			const columnExists = await this.#checkColumnExists(tableName, fromField);
			if (!columnExists) {
				throw new EnsureError(
					`Source field "${fromField}" does not exist in table "${tableName}"`,
					{operation: "copyColumn", table: tableName, step: 0},
				);
			}

			try {
				// UPDATE <table> SET <toField> = <fromField> WHERE <toField> IS NULL
				const updateStrings = makeTemplate([
					"UPDATE ",
					" SET ",
					" = ",
					" WHERE ",
					" IS NULL",
				]);
				const updateValues = [
					ident(tableName),
					ident(toField),
					ident(fromField),
					ident(toField),
				];

				return await this.#driver.run(updateStrings, updateValues);
			} catch (error) {
				throw new EnsureError(
					`copyColumn failed: ${error instanceof Error ? error.message : String(error)}`,
					{operation: "copyColumn", table: tableName, step: 0},
					{cause: error},
				);
			}
		};

		// Run under migration lock if available
		if (this.#driver.withMigrationLock) {
			return await this.#driver.withMigrationLock(doCopy);
		}
		return await doCopy();
	}

	/**
	 * Check if a column exists in the actual database table.
	 * Queries the actual table structure to verify column existence.
	 */
	async #checkColumnExists(tableName: string, columnName: string): Promise<boolean> {
		// Use driver's getColumns if available
		if (this.#driver.getColumns) {
			const columns = await this.#driver.getColumns(tableName);
			return columns.some((col) => col.name === columnName);
		}

		// Try PRAGMA table_info first (SQLite)
		try {
			const pragmaStrings = makeTemplate(["PRAGMA table_info(", ")"]);
			const pragmaValues = [ident(tableName)];
			const columns = await this.#driver.all<{name: string}>(pragmaStrings, pragmaValues);
			if (columns.length > 0) {
				return columns.some((col) => col.name === columnName);
			}
		} catch {
			// Not SQLite, try information_schema
		}

		// Fallback: information_schema (PostgreSQL/MySQL)
		try {
			const schemaStrings = makeTemplate([
				"SELECT column_name FROM information_schema.columns WHERE table_name = ",
				" AND column_name = ",
				" LIMIT 1",
			]);
			const schemaValues = [tableName, columnName];
			const result = await this.#driver.all(schemaStrings, schemaValues);
			return result.length > 0;
		} catch {
			// Last resort: assume column exists and let the UPDATE fail naturally
			return true;
		}
	}

	// ==========================================================================
	// Transactions
	// ==========================================================================

	/**
	 * Execute a function within a database transaction.
	 *
	 * If the function completes successfully, the transaction is committed.
	 * If the function throws an error, the transaction is rolled back.
	 *
	 * All operations within the transaction callback use the same database
	 * connection, ensuring transactional consistency.
	 *
	 * @example
	 * await db.transaction(async (tx) => {
	 *   const user = await tx.insert(users, { id: "1", name: "Alice" });
	 *   await tx.insert(posts, { id: "1", authorId: user.id, title: "Hello" });
	 *   // If any insert fails, both are rolled back
	 * });
	 */
	async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
		return await this.#driver.transaction(async (txDriver) => {
			const tx = new Transaction(txDriver);
			return await fn(tx);
		});
	}
}
