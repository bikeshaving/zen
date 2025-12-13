/**
 * SQL Fragment Helpers
 *
 * Type-safe helpers that generate SQL fragments without emitting keywords.
 * All fragments are composable inside tagged templates.
 */

import type {Table, Infer} from "./table.js";
import {createFragment, type SQLFragment} from "./query.js";

// ============================================================================
// Operator DSL Types
// ============================================================================

/**
 * Condition operators for where/having clauses.
 */
export type ConditionOperators<T> = {
	$eq?: T;
	$lt?: T;
	$gt?: T;
	$lte?: T;
	$gte?: T;
	$like?: string;
	$in?: T[];
	$neq?: T;
	$isNull?: boolean;
};

/**
 * A condition value can be a plain value (shorthand for $eq) or an operator object.
 */
export type ConditionValue<T> = T | ConditionOperators<T>;

/**
 * Where conditions for a table - keys must exist in table schema.
 */
export type WhereConditions<T extends Table<any>> = {
	[K in keyof Infer<T>]?: ConditionValue<Infer<T>[K]>;
};

/**
 * Set values for updates - plain values only (no operators).
 */
export type SetValues<T extends Table<any>> = {
	[K in keyof Infer<T>]?: Infer<T>[K];
};

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Quote an identifier with double quotes (ANSI SQL standard).
 */
function quoteIdent(id: string): string {
	return `"${id.replace(/"/g, '""')}"`;
}

/**
 * Create a fully qualified column name: "table"."column"
 */
function qualifiedColumn(tableName: string, fieldName: string): string {
	return `${quoteIdent(tableName)}.${quoteIdent(fieldName)}`;
}

/**
 * Check if a value is an operator object.
 */
function isOperatorObject(value: unknown): value is ConditionOperators<unknown> {
	if (value === null || typeof value !== "object") return false;
	const keys = Object.keys(value);
	return keys.length > 0 && keys.every((k) => k.startsWith("$"));
}

/**
 * Build a condition fragment for a single field.
 */
function buildCondition(
	column: string,
	value: ConditionValue<unknown>,
): {sql: string; params: unknown[]} {
	if (isOperatorObject(value)) {
		const parts: string[] = [];
		const params: unknown[] = [];

		if (value.$eq !== undefined) {
			parts.push(`${column} = ?`);
			params.push(value.$eq);
		}
		if (value.$neq !== undefined) {
			parts.push(`${column} != ?`);
			params.push(value.$neq);
		}
		if (value.$lt !== undefined) {
			parts.push(`${column} < ?`);
			params.push(value.$lt);
		}
		if (value.$gt !== undefined) {
			parts.push(`${column} > ?`);
			params.push(value.$gt);
		}
		if (value.$gte !== undefined) {
			parts.push(`${column} >= ?`);
			params.push(value.$gte);
		}
		if (value.$lte !== undefined) {
			parts.push(`${column} <= ?`);
			params.push(value.$lte);
		}
		if (value.$like !== undefined) {
			parts.push(`${column} LIKE ?`);
			params.push(value.$like);
		}
		if (value.$in !== undefined && Array.isArray(value.$in)) {
			const placeholders = value.$in.map(() => "?").join(", ");
			parts.push(`${column} IN (${placeholders})`);
			params.push(...value.$in);
		}
		if (value.$isNull !== undefined) {
			parts.push(value.$isNull ? `${column} IS NULL` : `${column} IS NOT NULL`);
		}

		return {
			sql: parts.join(" AND "),
			params,
		};
	}

	// Plain value = $eq shorthand
	return {
		sql: `${column} = ?`,
		params: [value],
	};
}

// ============================================================================
// Fragment Helpers
// ============================================================================

/**
 * Generate an AND-joined conditional fragment for WHERE clauses.
 *
 * Emits fully qualified column names to avoid ambiguity in JOINs.
 *
 * @example
 * db.all(Posts)`
 *   WHERE ${where(Posts, { published: true, createdAt: { $gt: oneMonthAgo } })}
 * `
 * // Output: "posts"."published" = ? AND "posts"."createdAt" > ?
 */
export function where<T extends Table<any>>(
	table: T,
	conditions: WhereConditions<T>,
): SQLFragment {
	const entries = Object.entries(conditions);
	if (entries.length === 0) {
		return createFragment("1 = 1", []);
	}

	const parts: string[] = [];
	const params: unknown[] = [];

	for (const [field, value] of entries) {
		if (value === undefined) continue;

		const column = qualifiedColumn(table.name, field);
		const condition = buildCondition(column, value);
		parts.push(condition.sql);
		params.push(...condition.params);
	}

	if (parts.length === 0) {
		return createFragment("1 = 1", []);
	}

	return createFragment(parts.join(" AND "), params);
}

/**
 * Generate assignment fragment for UPDATE SET clauses.
 *
 * Column names are quoted but not table-qualified (SQL UPDATE syntax).
 *
 * @example
 * db.exec`
 *   UPDATE posts
 *   SET ${set(Posts, { title: "New Title", updatedAt: new Date() })}
 *   WHERE id = ${id}
 * `
 * // Output: "title" = ?, "updatedAt" = ?
 */
export function set<T extends Table<any>>(
	_table: T,
	values: SetValues<T>,
): SQLFragment {
	const entries = Object.entries(values);
	if (entries.length === 0) {
		throw new Error("set() requires at least one field");
	}

	const parts: string[] = [];
	const params: unknown[] = [];

	for (const [field, value] of entries) {
		if (value === undefined) continue;

		parts.push(`${quoteIdent(field)} = ?`);
		params.push(value);
	}

	if (parts.length === 0) {
		throw new Error("set() requires at least one non-undefined field");
	}

	return createFragment(parts.join(", "), params);
}

/**
 * Generate foreign-key equality fragment for JOIN ON clauses.
 *
 * Emits fully qualified, quoted column names.
 *
 * @example
 * db.all(Posts, Users)`
 *   JOIN users ON ${on(Posts, "authorId")}
 * `
 * // Output: "users"."id" = "posts"."authorId"
 */
export function on<T extends Table<any>>(
	table: T,
	field: keyof Infer<T> & string,
): SQLFragment {
	const refs = table.references();
	const ref = refs.find((r) => r.fieldName === field);

	if (!ref) {
		throw new Error(
			`Field "${field}" is not a foreign key reference in table "${table.name}"`,
		);
	}

	const refColumn = qualifiedColumn(ref.table.name, ref.referencedField);
	const fkColumn = qualifiedColumn(table.name, field);

	return createFragment(`${refColumn} = ${fkColumn}`, []);
}

/**
 * Generate value tuples fragment for INSERT statements.
 *
 * Each row is validated against the table schema. The columns array
 * determines the order of values and must match the SQL column list.
 *
 * @example
 * db.exec`
 *   INSERT INTO posts (id, title, published)
 *   VALUES ${values(Posts, rows, ["id", "title", "published"])}
 * `
 * // Output: (?, ?, ?), (?, ?, ?)
 */
export function values<T extends Table<any>>(
	table: T,
	rows: Partial<Infer<T>>[],
	columns: (keyof Infer<T> & string)[],
): SQLFragment {
	if (rows.length === 0) {
		throw new Error("values() requires at least one row");
	}

	if (columns.length === 0) {
		throw new Error("values() requires at least one column");
	}

	const partialSchema = table.schema.partial();
	const params: unknown[] = [];
	const tuples: string[] = [];

	for (const row of rows) {
		const validated = partialSchema.parse(row);
		const rowPlaceholders: string[] = [];

		for (const col of columns) {
			rowPlaceholders.push("?");
			params.push(validated[col]);
		}

		tuples.push(`(${rowPlaceholders.join(", ")})`);
	}

	return createFragment(tuples.join(", "), params);
}
