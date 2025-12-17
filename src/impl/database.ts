/**
 * Database wrapper - the main API for schema-driven SQL.
 *
 * Provides typed queries with entity normalization and reference resolution.
 * Extends EventTarget for IndexedDB-style migration events.
 */

import type {Table, Infer, Insert, FullTableOnly} from "./table.js";
import {validateWithStandardSchema} from "./table.js";
import {z} from "zod";
import {
	normalize,
	normalizeOne,
	isSQLFragment,
	type SQLFragment,
} from "./query.js";

// ============================================================================
// DB Expressions - Runtime values evaluated by the database
// ============================================================================

const DB_EXPR = Symbol.for("@b9g/zen:db-expr");

/**
 * Internal type for resolved DB expressions (raw SQL to inject).
 */
interface DBExpression {
	readonly [DB_EXPR]: true;
	readonly sql: string;
	readonly params: unknown[];
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
 * Create a DB expression from raw SQL and optional params.
 */
function createDBExpr(sql: string, params: unknown[] = []): DBExpression {
	return {[DB_EXPR]: true, sql, params};
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
			result[fieldName] = createDBExpr(meta.sql!, meta.params ?? []);
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
}

// ============================================================================
// Template Building Helpers
// ============================================================================

/**
 * Build a TemplateStringsArray from string parts.
 * Used to construct queries programmatically while still using the driver's
 * template-based interface.
 */
function makeTemplate(parts: string[]): TemplateStringsArray {
	return Object.assign([...parts], {raw: parts}) as TemplateStringsArray;
}

/**
 * Quote an identifier (table name, column name) using SQL standard double quotes.
 * All drivers use this - MySQL requires ANSI_QUOTES mode.
 */
function quoteIdent(name: string): string {
	return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Build INSERT template: INSERT INTO "table" ("col1", "col2") VALUES (?, ?)
 * Returns template parts and values array.
 * Symbols are included as placeholder values - resolved at driver call time.
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

	const columnList = allCols.map((c) => quoteIdent(c)).join(", ");

	// Build template: regular values and symbols get placeholders, expressions get their SQL
	// Expression SQL may contain ? placeholders for their params
	const valueCols = [...regularCols, ...symbolCols];
	const parts: string[] = [
		`INSERT INTO ${quoteIdent(tableName)} (${columnList}) VALUES (`,
	];
	for (let i = 1; i < valueCols.length; i++) {
		parts.push(", ");
	}
	// Add expression SQL (may contain ? placeholders for expression params)
	const exprSql = exprCols.map((c) => expressions[c].sql).join(", ");
	if (valueCols.length > 0 && exprCols.length > 0) {
		parts.push(`, ${exprSql})`);
	} else if (exprCols.length > 0) {
		parts[0] += `${exprSql})`;
	} else {
		parts.push(")");
	}

	// Values: regular data, symbols, then expression params (in column order)
	const values = [
		...regularCols.map((c) => data[c]),
		...symbolCols.map((c) => symbols[c]),
		...exprCols.flatMap((c) => expressions[c].params),
	];

	return {
		strings: makeTemplate(parts),
		values,
	};
}

/**
 * Build UPDATE template: UPDATE "table" SET "col1" = ?, "col2" = ? WHERE "pk" = ?
 * Returns template parts and values array.
 * Symbols are included as placeholder values - resolved at driver call time.
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

	// Template: UPDATE "table" SET "col1" = ?, "col2" = ? WHERE "pk" = ?
	const parts: string[] = [`UPDATE ${quoteIdent(tableName)} SET `];
	for (let i = 0; i < valueCols.length; i++) {
		if (i === 0) {
			parts[0] += `${quoteIdent(valueCols[i])} = `;
		} else {
			parts.push(`, ${quoteIdent(valueCols[i])} = `);
		}
	}
	// Add expression assignments (SQL may contain ? placeholders for expression params)
	const exprAssignments = exprCols
		.map((c) => `${quoteIdent(c)} = ${expressions[c].sql}`)
		.join(", ");
	if (valueCols.length > 0 && exprCols.length > 0) {
		parts.push(`, ${exprAssignments} WHERE ${quoteIdent(pk)} = `);
	} else if (exprCols.length > 0) {
		parts[0] += `${exprAssignments} WHERE ${quoteIdent(pk)} = `;
	} else {
		parts.push(` WHERE ${quoteIdent(pk)} = `);
	}
	parts.push("");

	// Values: regular data, symbols, expression params, then id
	const values = [
		...regularCols.map((c) => data[c]),
		...symbolCols.map((c) => symbols[c]),
		...exprCols.flatMap((c) => expressions[c].params),
		id,
	];

	return {
		strings: makeTemplate(parts),
		values,
	};
}

/**
 * Build UPDATE template for multiple IDs: UPDATE "table" SET ... WHERE "pk" IN (?, ?, ?)
 * Symbols are included as placeholder values - resolved at driver call time.
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

	// Build SET clause parts
	const parts: string[] = [`UPDATE ${quoteIdent(tableName)} SET `];
	for (let i = 0; i < valueCols.length; i++) {
		if (i === 0) {
			parts[0] += `${quoteIdent(valueCols[i])} = `;
		} else {
			parts.push(`, ${quoteIdent(valueCols[i])} = `);
		}
	}

	// Add expression assignments (SQL may contain ? placeholders for expression params)
	const exprAssignments = exprCols
		.map((c) => `${quoteIdent(c)} = ${expressions[c].sql}`)
		.join(", ");

	if (valueCols.length > 0 && exprCols.length > 0) {
		parts.push(`, ${exprAssignments} WHERE ${quoteIdent(pk)} IN (`);
	} else if (exprCols.length > 0) {
		parts[0] += `${exprAssignments} WHERE ${quoteIdent(pk)} IN (`;
	} else {
		parts.push(` WHERE ${quoteIdent(pk)} IN (`);
	}

	// Add ID placeholders
	for (let i = 1; i < ids.length; i++) {
		parts.push(", ");
	}
	parts.push(")");

	// Values: regular data, symbols, expression params, then ids
	const values = [
		...regularCols.map((c) => data[c]),
		...symbolCols.map((c) => symbols[c]),
		...exprCols.flatMap((c) => expressions[c].params),
		...ids,
	];

	return {
		strings: makeTemplate(parts),
		values,
	};
}

/**
 * Build SELECT column list: "table"."col1" AS "table.col1", "table"."col2" AS "table.col2", ...
 * Handles derived expressions too.
 */
function buildSelectCols(tables: Table<any>[]): {
	sql: string;
	params: unknown[];
} {
	const columns: string[] = [];
	const params: unknown[] = [];

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

			const qualifiedCol = `${quoteIdent(tableName)}.${quoteIdent(fieldName)}`;
			const alias = `${tableName}.${fieldName}`;
			columns.push(`${qualifiedCol} AS ${quoteIdent(alias)}`);
		}

		// Append derived expressions with auto-generated aliases
		const derivedExprs = (table.meta as any).derivedExprs ?? [];
		for (const expr of derivedExprs) {
			const alias = `${tableName}.${expr.fieldName}`;
			columns.push(`(${expr.sql}) AS ${quoteIdent(alias)}`);
			params.push(...expr.params);
		}
	}

	return {sql: columns.join(", "), params};
}

/**
 * Build SELECT by primary key template: SELECT * FROM "table" WHERE "pk" = ?
 */
function buildSelectByPkParts(
	tableName: string,
	pk: string,
	id: unknown,
): {strings: TemplateStringsArray; values: unknown[]} {
	return {
		strings: makeTemplate([
			`SELECT * FROM ${quoteIdent(tableName)} WHERE ${quoteIdent(pk)} = `,
			"",
		]),
		values: [id],
	};
}

/**
 * Build SELECT by multiple IDs: SELECT * FROM "table" WHERE "pk" IN (?, ?, ?)
 */
function buildSelectByPksParts(
	tableName: string,
	pk: string,
	ids: unknown[],
): {strings: TemplateStringsArray; values: unknown[]} {
	const parts: string[] = [
		`SELECT * FROM ${quoteIdent(tableName)} WHERE ${quoteIdent(pk)} IN (`,
	];
	for (let i = 1; i < ids.length; i++) {
		parts.push(", ");
	}
	parts.push(")");

	return {
		strings: makeTemplate(parts),
		values: ids,
	};
}

/**
 * Build DELETE by primary key template: DELETE FROM "table" WHERE "pk" = ?
 */
function buildDeleteByPkParts(
	tableName: string,
	pk: string,
	id: unknown,
): {strings: TemplateStringsArray; values: unknown[]} {
	return {
		strings: makeTemplate([
			`DELETE FROM ${quoteIdent(tableName)} WHERE ${quoteIdent(pk)} = `,
			"",
		]),
		values: [id],
	};
}

/**
 * Build DELETE by multiple IDs: DELETE FROM "table" WHERE "pk" IN (?, ?, ?)
 */
function buildDeleteByPksParts(
	tableName: string,
	pk: string,
	ids: unknown[],
): {strings: TemplateStringsArray; values: unknown[]} {
	const parts: string[] = [
		`DELETE FROM ${quoteIdent(tableName)} WHERE ${quoteIdent(pk)} IN (`,
	];
	for (let i = 1; i < ids.length; i++) {
		parts.push(", ");
	}
	parts.push(")");

	return {
		strings: makeTemplate(parts),
		values: ids,
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
		if (isSQLFragment(value)) {
			// Expand fragment: merge its SQL into strings, add its params to values
			const fragment = value as SQLFragment;
			// Append fragment SQL to last string part
			newStrings[newStrings.length - 1] += fragment.sql + strings[i + 1];
			newValues.push(...fragment.params);
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
 * - Keep migrations idempotent when possible (use ensureColumn, ensureIndex)
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
			const {sql: cols, params: colParams} = buildSelectCols(tableArray);
			const prefix = `SELECT ${cols} FROM ${quoteIdent(tableArray[0].name)} `;
			const {strings: expandedStrings, values: expandedValues} =
				expandFragments(strings, values);
			const prefixedStrings = makeTemplate([
				prefix + expandedStrings[0],
				...expandedStrings.slice(1),
			]);
			const rows = await this.#driver.all<Record<string, unknown>>(
				prefixedStrings,
				[...colParams, ...expandedValues],
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
					const decoded = decodeData(table, row);
					return validateWithStandardSchema<Infer<T>>(
						table.schema,
						decoded,
					) as Infer<T>;
				});
		}

		// Tagged template query
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const {sql: cols, params: colParams} = buildSelectCols(tableArray);
			const prefix = `SELECT ${cols} FROM ${quoteIdent(tableArray[0].name)} `;
			const {strings: expandedStrings, values: expandedValues} =
				expandFragments(strings, values);
			const prefixedStrings = makeTemplate([
				prefix + expandedStrings[0],
				...expandedStrings.slice(1),
			]);
			const row = await this.#driver.get<Record<string, unknown>>(
				prefixedStrings,
				[...colParams, ...expandedValues],
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
			const decoded = decodeData(table, row);
			return validateWithStandardSchema<Infer<T>>(
				table.schema,
				decoded,
			) as Infer<T>;
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
				const decoded = decodeData(table, row);
				return validateWithStandardSchema<Infer<T>>(
					table.schema,
					decoded,
				) as Infer<T>;
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
			const decoded = decodeData(table, row);
			return validateWithStandardSchema<Infer<T>>(
				table.schema,
				decoded,
			) as Infer<T>;
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
		const decoded = decodeData(table, row);
		return validateWithStandardSchema<Infer<T>>(
			table.schema,
			decoded,
		) as Infer<T>;
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
				const decoded = decodeData(table, row);
				const entity = validateWithStandardSchema<Infer<T>>(
					table.schema,
					decoded,
				) as Infer<T>;
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
			const decoded = decodeData(table, row);
			const entity = validateWithStandardSchema<Infer<T>>(
				table.schema,
				decoded,
			) as Infer<T>;
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

		// Build SET clause
		const setCols = Object.keys(encoded);
		const exprCols = Object.keys(expressions);
		const setClauseParts: string[] = [];
		for (const col of setCols) {
			setClauseParts.push(`${quoteIdent(col)} = `);
		}
		const exprAssignments = exprCols
			.map((c) => `${quoteIdent(c)} = ${expressions[c].sql}`)
			.join(", ");

		// Build complete UPDATE template
		const {strings: whereStrings, values: whereValues} = expandFragments(
			strings,
			templateValues,
		);
		const setValues = setCols.map((c) => encoded[c]);

		// Construct: UPDATE "table" SET "col1" = ?, "col2" = ? WHERE ...
		const prefix = `UPDATE ${quoteIdent(table.name)} SET `;
		const parts: string[] = [prefix];
		for (let i = 0; i < setCols.length; i++) {
			if (i === 0) {
				parts[0] += `${quoteIdent(setCols[i])} = `;
			} else {
				parts.push(`, ${quoteIdent(setCols[i])} = `);
			}
		}
		if (setCols.length > 0 && exprCols.length > 0) {
			parts.push(`, ${exprAssignments} ${whereStrings[0]}`);
		} else if (exprCols.length > 0) {
			parts[0] += `${exprAssignments} ${whereStrings[0]}`;
		} else {
			parts.push(` ${whereStrings[0]}`);
		}
		// Add rest of WHERE template parts
		for (let i = 1; i < whereStrings.length; i++) {
			parts.push(whereStrings[i]);
		}

		const allValues = [...setValues, ...whereValues];

		if (this.#driver.supportsReturning) {
			parts[parts.length - 1] += " RETURNING *";
			const rows = await this.#driver.all<Record<string, unknown>>(
				makeTemplate(parts),
				allValues,
			);
			return rows.map((row) => {
				const decoded = decodeData(table, row);
				return validateWithStandardSchema<Infer<T>>(
					table.schema,
					decoded,
				) as Infer<T>;
			});
		}

		// Fallback: Get IDs first, then UPDATE, then SELECT
		// Build SELECT to get IDs first
		const selectIdParts = [
			`SELECT ${quoteIdent(pk)} FROM ${quoteIdent(table.name)} ${whereStrings[0]}`,
			...whereStrings.slice(1),
		];
		const idRows = await this.#driver.all<Record<string, unknown>>(
			makeTemplate(selectIdParts),
			whereValues,
		);
		const ids = idRows.map((r) => r[pk] as string | number);

		if (ids.length === 0) {
			return [];
		}

		// Run UPDATE
		await this.#driver.run(makeTemplate(parts), allValues);

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

		return rows.map((row) => {
			const decoded = decodeData(table, row);
			return validateWithStandardSchema<Infer<T>>(
				table.schema,
				decoded,
			) as Infer<T>;
		});
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
				const prefixedStrings = makeTemplate([
					`DELETE FROM ${quoteIdent(table.name)} ${expandedStrings[0]}`,
					...expandedStrings.slice(1),
				]);
				return this.#driver.run(prefixedStrings, expandedValues);
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

		const setClauses: string[] = [
			`${quoteIdent(softDeleteField)} = CURRENT_TIMESTAMP`,
		];
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${expr.sql}`);
			}
		}
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${resolveSQLSymbol(sym)}`);
			}
		}

		const setClause = setClauses.join(", ");
		const sql = `UPDATE ${quoteIdent(table.name)} SET ${setClause} WHERE ${quoteIdent(pk)} = `;
		const parts = makeTemplate([sql, ""]);
		return this.#driver.run(parts, [id]);
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

		const setClauses: string[] = [
			`${quoteIdent(softDeleteField)} = CURRENT_TIMESTAMP`,
		];
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${expr.sql}`);
			}
		}
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${resolveSQLSymbol(sym)}`);
			}
		}

		const setClause = setClauses.join(", ");
		const parts: string[] = [
			`UPDATE ${quoteIdent(table.name)} SET ${setClause} WHERE ${quoteIdent(pk)} IN (`,
		];
		for (let i = 1; i < ids.length; i++) {
			parts.push(", ");
		}
		parts.push(")");

		return this.#driver.run(makeTemplate(parts), ids);
	}

	async #softDeleteWithWhere<T extends Table<any>>(
		table: T,
		strings: TemplateStringsArray,
		templateValues: unknown[],
	): Promise<number> {
		const softDeleteField = table.meta.softDeleteField!;

		const schemaExprs = injectSchemaExpressions(table, {}, "update");
		const {expressions, symbols} = extractDBExpressions(schemaExprs);

		const setClauses: string[] = [
			`${quoteIdent(softDeleteField)} = CURRENT_TIMESTAMP`,
		];
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${expr.sql}`);
			}
		}
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${resolveSQLSymbol(sym)}`);
			}
		}

		const setClause = setClauses.join(", ");
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			templateValues,
		);
		const prefixedStrings = makeTemplate([
			`UPDATE ${quoteIdent(table.name)} SET ${setClause} ${expandedStrings[0]}`,
			...expandedStrings.slice(1),
		]);

		return this.#driver.run(prefixedStrings, expandedValues);
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
		// Use TEXT for timestamp - works across all databases
		const createTable = makeTemplate([
			`CREATE TABLE IF NOT EXISTS _migrations (
				version INTEGER PRIMARY KEY,
				applied_at TEXT DEFAULT CURRENT_TIMESTAMP
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
			const {sql: cols, params: colParams} = buildSelectCols(tableArray);
			const prefix = `SELECT ${cols} FROM ${quoteIdent(tableArray[0].name)} `;
			const {strings: expandedStrings, values: expandedValues} =
				expandFragments(strings, values);
			const prefixedStrings = makeTemplate([
				prefix + expandedStrings[0],
				...expandedStrings.slice(1),
			]);
			const rows = await this.#driver.all<Record<string, unknown>>(
				prefixedStrings,
				[...colParams, ...expandedValues],
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
					const decoded = decodeData(table, row);
					return validateWithStandardSchema<Infer<T>>(
						table.schema,
						decoded,
					) as Infer<T>;
				});
		}

		// Tagged template query
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const {sql: cols, params: colParams} = buildSelectCols(tableArray);
			const prefix = `SELECT ${cols} FROM ${quoteIdent(tableArray[0].name)} `;
			const {strings: expandedStrings, values: expandedValues} =
				expandFragments(strings, values);
			const prefixedStrings = makeTemplate([
				prefix + expandedStrings[0],
				...expandedStrings.slice(1),
			]);
			const row = await this.#driver.get<Record<string, unknown>>(
				prefixedStrings,
				[...colParams, ...expandedValues],
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
			const decoded = decodeData(table, row);
			return validateWithStandardSchema<Infer<T>>(
				table.schema,
				decoded,
			) as Infer<T>;
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
				const decoded = decodeData(table, row);
				return validateWithStandardSchema<Infer<T>>(
					table.schema,
					decoded,
				) as Infer<T>;
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
			const decoded = decodeData(table, row);
			return validateWithStandardSchema<Infer<T>>(
				table.schema,
				decoded,
			) as Infer<T>;
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
		const decoded = decodeData(table, row);
		return validateWithStandardSchema<Infer<T>>(
			table.schema,
			decoded,
		) as Infer<T>;
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
				const decoded = decodeData(table, row);
				const entity = validateWithStandardSchema<Infer<T>>(
					table.schema,
					decoded,
				) as Infer<T>;
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
			const decoded = decodeData(table, row);
			const entity = validateWithStandardSchema<Infer<T>>(
				table.schema,
				decoded,
			) as Infer<T>;
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

		// Build SET clause
		const setCols = Object.keys(encoded);
		const exprCols = Object.keys(expressions);
		const exprAssignments = exprCols
			.map((c) => `${quoteIdent(c)} = ${expressions[c].sql}`)
			.join(", ");

		// Build complete UPDATE template
		const {strings: whereStrings, values: whereValues} = expandFragments(
			strings,
			templateValues,
		);
		const setValues = setCols.map((c) => encoded[c]);

		// Construct: UPDATE "table" SET "col1" = ?, "col2" = ? WHERE ...
		const prefix = `UPDATE ${quoteIdent(table.name)} SET `;
		const parts: string[] = [prefix];
		for (let i = 0; i < setCols.length; i++) {
			if (i === 0) {
				parts[0] += `${quoteIdent(setCols[i])} = `;
			} else {
				parts.push(`, ${quoteIdent(setCols[i])} = `);
			}
		}
		if (setCols.length > 0 && exprCols.length > 0) {
			parts.push(`, ${exprAssignments} ${whereStrings[0]}`);
		} else if (exprCols.length > 0) {
			parts[0] += `${exprAssignments} ${whereStrings[0]}`;
		} else {
			parts.push(` ${whereStrings[0]}`);
		}
		// Add rest of WHERE template parts
		for (let i = 1; i < whereStrings.length; i++) {
			parts.push(whereStrings[i]);
		}

		const allValues = [...setValues, ...whereValues];

		if (this.#driver.supportsReturning) {
			parts[parts.length - 1] += " RETURNING *";
			const rows = await this.#driver.all<Record<string, unknown>>(
				makeTemplate(parts),
				allValues,
			);
			return rows.map((row) => {
				const decoded = decodeData(table, row);
				return validateWithStandardSchema<Infer<T>>(
					table.schema,
					decoded,
				) as Infer<T>;
			});
		}

		// Fallback: Get IDs first, then UPDATE, then SELECT
		const selectIdParts = [
			`SELECT ${quoteIdent(pk)} FROM ${quoteIdent(table.name)} ${whereStrings[0]}`,
			...whereStrings.slice(1),
		];
		const idRows = await this.#driver.all<Record<string, unknown>>(
			makeTemplate(selectIdParts),
			whereValues,
		);
		const ids = idRows.map((r) => r[pk] as string | number);

		if (ids.length === 0) {
			return [];
		}

		// Run UPDATE
		await this.#driver.run(makeTemplate(parts), allValues);

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

		return rows.map((row) => {
			const decoded = decodeData(table, row);
			return validateWithStandardSchema<Infer<T>>(
				table.schema,
				decoded,
			) as Infer<T>;
		});
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
				const prefixedStrings = makeTemplate([
					`DELETE FROM ${quoteIdent(table.name)} ${expandedStrings[0]}`,
					...expandedStrings.slice(1),
				]);
				return this.#driver.run(prefixedStrings, expandedValues);
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

		const setClauses: string[] = [
			`${quoteIdent(softDeleteField)} = CURRENT_TIMESTAMP`,
		];
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${expr.sql}`);
			}
		}
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${resolveSQLSymbol(sym)}`);
			}
		}

		const setClause = setClauses.join(", ");
		const sql = `UPDATE ${quoteIdent(table.name)} SET ${setClause} WHERE ${quoteIdent(pk)} = `;
		const parts = makeTemplate([sql, ""]);
		return this.#driver.run(parts, [id]);
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

		const setClauses: string[] = [
			`${quoteIdent(softDeleteField)} = CURRENT_TIMESTAMP`,
		];
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${expr.sql}`);
			}
		}
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${resolveSQLSymbol(sym)}`);
			}
		}

		const setClause = setClauses.join(", ");
		const parts: string[] = [
			`UPDATE ${quoteIdent(table.name)} SET ${setClause} WHERE ${quoteIdent(pk)} IN (`,
		];
		for (let i = 1; i < ids.length; i++) {
			parts.push(", ");
		}
		parts.push(")");

		return this.#driver.run(makeTemplate(parts), ids);
	}

	async #softDeleteWithWhere<T extends Table<any>>(
		table: T,
		strings: TemplateStringsArray,
		templateValues: unknown[],
	): Promise<number> {
		const softDeleteField = table.meta.softDeleteField!;

		const schemaExprs = injectSchemaExpressions(table, {}, "update");
		const {expressions, symbols} = extractDBExpressions(schemaExprs);

		const setClauses: string[] = [
			`${quoteIdent(softDeleteField)} = CURRENT_TIMESTAMP`,
		];
		for (const [field, expr] of Object.entries(expressions)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${expr.sql}`);
			}
		}
		for (const [field, sym] of Object.entries(symbols)) {
			if (field !== softDeleteField) {
				setClauses.push(`${quoteIdent(field)} = ${resolveSQLSymbol(sym)}`);
			}
		}

		const setClause = setClauses.join(", ");
		const {strings: expandedStrings, values: expandedValues} = expandFragments(
			strings,
			templateValues,
		);
		const prefixedStrings = makeTemplate([
			`UPDATE ${quoteIdent(table.name)} SET ${setClause} ${expandedStrings[0]}`,
			...expandedStrings.slice(1),
		]);

		return this.#driver.run(prefixedStrings, expandedValues);
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
