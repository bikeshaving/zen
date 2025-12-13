/**
 * Query layer - tagged template SQL with parameterized queries.
 *
 * Generates SELECT statements with prefixed column aliases for entity normalization.
 */

import {type Table, isTable} from "./table.js";

// ============================================================================
// Types
// ============================================================================

export type SQLDialect = "sqlite" | "postgresql" | "mysql";

export interface QueryOptions {
	dialect?: SQLDialect;
}

export interface ParsedQuery {
	sql: string;
	params: unknown[];
}

// ============================================================================
// SQL Fragments
// ============================================================================

const SQL_FRAGMENT = Symbol.for("@b9g/zealot:fragment");

/**
 * A SQL fragment with embedded parameters.
 *
 * When interpolated in a tagged template, the SQL is injected directly
 * and params are added to the parameter list.
 */
export interface SQLFragment {
	readonly [SQL_FRAGMENT]: true;
	readonly sql: string;
	readonly params: readonly unknown[];
}

/**
 * Check if a value is a SQL fragment.
 */
export function isSQLFragment(value: unknown): value is SQLFragment {
	return (
		value !== null &&
		typeof value === "object" &&
		SQL_FRAGMENT in value &&
		(value as any)[SQL_FRAGMENT] === true
	);
}

/**
 * Create a SQL fragment from raw SQL and parameters.
 *
 * @internal Used by fragment helpers (where, set, on, etc.)
 */
export function createFragment(
	sql: string,
	params: unknown[] = [],
): SQLFragment {
	return {
		[SQL_FRAGMENT]: true,
		sql,
		params,
	};
}

// ============================================================================
// Query Building
// ============================================================================

function quoteIdent(name: string, dialect: SQLDialect): string {
	if (dialect === "mysql") {
		// MySQL: backticks, doubled to escape
		return `\`${name.replace(/`/g, "``")}\``;
	}
	// PostgreSQL and SQLite: double quotes, doubled to escape
	return `"${name.replace(/"/g, '""')}"`;
}

function placeholder(index: number, dialect: SQLDialect): string {
	if (dialect === "postgresql") {
		return `$${index}`;
	}
	return "?";
}

/**
 * Build SELECT clause with prefixed column aliases.
 *
 * @example
 * buildSelectColumns([posts, users], "sqlite")
 * // SELECT "posts"."id" AS "posts.id", "posts"."title" AS "posts.title", ...
 */
export function buildSelectColumns(
	tables: Table<any>[],
	dialect: SQLDialect = "sqlite",
): string {
	const columns: string[] = [];

	for (const table of tables) {
		const tableName = table.name;
		const shape = table.schema.shape;

		for (const fieldName of Object.keys(shape)) {
			const qualifiedCol = `${quoteIdent(tableName, dialect)}.${quoteIdent(fieldName, dialect)}`;
			const alias = `${tableName}.${fieldName}`;
			columns.push(`${qualifiedCol} AS ${quoteIdent(alias, dialect)}`);
		}
	}

	return columns.join(", ");
}

/**
 * Parse a tagged template into SQL string and params array.
 *
 * Supports:
 * - SQL fragments: their SQL is injected directly, params added to list
 * - Table objects: interpolated as quoted table names
 * - Other values: become parameterized placeholders
 *
 * @example
 * parseTemplate`WHERE id = ${userId} AND active = ${true}`
 * // { sql: "WHERE id = ? AND active = ?", params: ["user-123", true] }
 *
 * @example
 * parseTemplate`WHERE ${where(Users, { role: "admin" })}`
 * // { sql: "WHERE role = ?", params: ["admin"] }
 *
 * @example
 * parseTemplate`FROM ${Posts} JOIN ${Users} ON ...`
 * // { sql: 'FROM "posts" JOIN "users" ON ...', params: [] }
 */
export function parseTemplate(
	strings: TemplateStringsArray,
	values: unknown[],
	dialect: SQLDialect = "sqlite",
): ParsedQuery {
	const params: unknown[] = [];
	let sql = "";

	for (let i = 0; i < strings.length; i++) {
		sql += strings[i];
		if (i < values.length) {
			const value = values[i];
			if (isSQLFragment(value)) {
				// Inject fragment SQL, replacing ? placeholders with dialect-appropriate ones
				let fragmentSQL = value.sql;
				for (const param of value.params) {
					params.push(param);
					// Replace first ? with the correct placeholder for this dialect
					fragmentSQL = fragmentSQL.replace(
						"?",
						placeholder(params.length, dialect),
					);
				}
				sql += fragmentSQL;
			} else if (isTable(value)) {
				// Inject quoted table name
				sql += quoteIdent(value.name, dialect);
			} else {
				params.push(value);
				sql += placeholder(params.length, dialect);
			}
		}
	}

	return {sql: sql.trim(), params};
}

/**
 * Build a full SELECT query for tables with user-provided clauses.
 *
 * @example
 * buildQuery([posts, users], "JOIN users ON users.id = posts.author_id WHERE published = ?", "sqlite")
 */
export function buildQuery(
	tables: Table<any>[],
	userClauses: string,
	dialect: SQLDialect = "sqlite",
): string {
	if (tables.length === 0) {
		throw new Error("At least one table is required");
	}

	const mainTable = tables[0].name;
	const selectCols = buildSelectColumns(tables, dialect);
	const fromClause = quoteIdent(mainTable, dialect);

	let sql = `SELECT ${selectCols} FROM ${fromClause}`;

	if (userClauses.trim()) {
		sql += ` ${userClauses}`;
	}

	return sql;
}

/**
 * Create a tagged template function for querying tables.
 *
 * @example
 * const query = createQuery([posts, users], "sqlite");
 * const { sql, params } = query`
 *   JOIN users ON users.id = posts.author_id
 *   WHERE published = ${true}
 * `;
 */
export function createQuery(
	tables: Table<any>[],
	dialect: SQLDialect = "sqlite",
): (strings: TemplateStringsArray, ...values: unknown[]) => ParsedQuery {
	return (strings: TemplateStringsArray, ...values: unknown[]) => {
		const {sql: userClauses, params} = parseTemplate(strings, values, dialect);
		const sql = buildQuery(tables, userClauses, dialect);
		return {sql, params};
	};
}

// ============================================================================
// Raw Query Helpers
// ============================================================================

/**
 * Parse a raw SQL template (no table-based SELECT generation).
 *
 * @example
 * const { sql, params } = rawQuery`SELECT COUNT(*) FROM posts WHERE author_id = ${userId}`;
 */
export function rawQuery(
	strings: TemplateStringsArray,
	...values: unknown[]
): ParsedQuery {
	return parseTemplate(strings, values, "sqlite");
}

/**
 * Create a raw query function for a specific dialect.
 */
export function createRawQuery(
	dialect: SQLDialect,
): (strings: TemplateStringsArray, ...values: unknown[]) => ParsedQuery {
	return (strings: TemplateStringsArray, ...values: unknown[]) => {
		return parseTemplate(strings, values, dialect);
	};
}

// ============================================================================
// Entity Normalization
// ============================================================================

/**
 * Entity normalization - Apollo-style entity deduplication with reference resolution.
 *
 * Takes raw SQL results with prefixed columns and returns normalized entities
 * with references resolved to actual object instances.
 */

// ============================================================================
// Normalization Types
// ============================================================================

/**
 * Raw row from SQL query with prefixed column names.
 * @example { "posts.id": "p1", "posts.authorId": "u1", "users.id": "u1", "users.name": "Alice" }
 */
export type RawRow = Record<string, unknown>;

/**
 * Entity map keyed by "table:primaryKey"
 */
export type EntityMap = Map<string, Record<string, unknown>>;

/**
 * Table map by table name for lookup
 */
export type TableMap = Map<string, Table<any>>;

// ============================================================================
// Parsing
// ============================================================================

/**
 * Extract entity data from a raw row for a specific table.
 *
 * @example
 * extractEntityData({ "posts.id": "p1", "users.id": "u1" }, "posts")
 * // { id: "p1" }
 */
export function extractEntityData(
	row: RawRow,
	tableName: string,
): Record<string, unknown> | null {
	const prefix = `${tableName}.`;
	const entity: Record<string, unknown> = {};
	let hasData = false;

	for (const [key, value] of Object.entries(row)) {
		if (key.startsWith(prefix)) {
			const fieldName = key.slice(prefix.length);
			entity[fieldName] = value;
			if (value !== null && value !== undefined) {
				hasData = true;
			}
		}
	}

	return hasData ? entity : null;
}

/**
 * Get the primary key value for an entity.
 */
export function getPrimaryKeyValue(
	entity: Record<string, unknown>,
	table: Table<any>,
): string | null {
	const pk = table.primaryKey();

	if (pk === null) {
		return null;
	}

	const value = entity[pk];
	return value !== null && value !== undefined ? String(value) : null;
}

/**
 * Create entity key for the entity map.
 */
export function entityKey(tableName: string, primaryKey: string): string {
	return `${tableName}:${primaryKey}`;
}

// ============================================================================
// Normalization
// ============================================================================

/**
 * Build an entity map from raw rows.
 *
 * Entities are deduplicated - same primary key = same object instance.
 * Each entity is parsed through its table's schema for type coercion
 * (e.g., z.coerce.date() converts date strings to Date objects).
 */
export function buildEntityMap(
	rows: RawRow[],
	tables: Table<any>[],
): EntityMap {
	const entities: EntityMap = new Map();

	for (const row of rows) {
		for (const table of tables) {
			const data = extractEntityData(row, table.name);
			if (!data) continue;

			const pk = getPrimaryKeyValue(data, table);
			if (!pk) continue;

			const key = entityKey(table.name, pk);

			if (!entities.has(key)) {
				// Parse through schema for type coercion (dates, numbers, etc.)
				const parsed = table.schema.parse(data);
				entities.set(key, parsed);
			}
		}
	}

	return entities;
}

/**
 * Resolve references for all entities in the map.
 *
 * Walks each table's references() and adds resolved entities as properties.
 */
export function resolveReferences(
	entities: EntityMap,
	tables: Table<any>[],
): void {
	for (const table of tables) {
		const refs = table.references();
		if (refs.length === 0) continue;

		const prefix = `${table.name}:`;

		for (const [key, entity] of entities) {
			if (!key.startsWith(prefix)) continue;

			for (const ref of refs) {
				const foreignKeyValue = entity[ref.fieldName];
				if (foreignKeyValue === null || foreignKeyValue === undefined) {
					entity[ref.as] = null;
					continue;
				}

				const refKey = entityKey(ref.table.name, String(foreignKeyValue));
				const refEntity = entities.get(refKey);

				entity[ref.as] = refEntity ?? null;
			}
		}
	}
}

/**
 * Extract main table entities from the entity map in row order.
 *
 * Maintains the order from the original query results.
 */
export function extractMainEntities<T>(
	rows: RawRow[],
	mainTable: Table<any>,
	entities: EntityMap,
): T[] {
	const results: T[] = [];
	const seen = new Set<string>();

	for (const row of rows) {
		const data = extractEntityData(row, mainTable.name);
		if (!data) continue;

		const pk = getPrimaryKeyValue(data, mainTable);
		if (!pk) continue;

		const key = entityKey(mainTable.name, pk);

		if (seen.has(key)) continue;
		seen.add(key);

		const entity = entities.get(key);
		if (entity) {
			results.push(entity as T);
		}
	}

	return results;
}

// ============================================================================
// Validation
// ============================================================================

/**
 * Detect column prefixes in rows that don't match any provided table.
 * Throws if a JOIN includes a table not passed to normalize().
 */
function validateRegisteredTables(rows: RawRow[], tables: Table<any>[]): void {
	if (rows.length === 0) return;

	const registeredPrefixes = new Set(tables.map((t) => `${t.name}.`));
	const unregisteredTables: string[] = [];

	// Check first row for unregistered prefixes
	const firstRow = rows[0];
	for (const key of Object.keys(firstRow)) {
		const dotIndex = key.indexOf(".");
		if (dotIndex === -1) continue;

		const prefix = key.slice(0, dotIndex + 1);
		if (!registeredPrefixes.has(prefix)) {
			const tableName = key.slice(0, dotIndex);
			if (!unregisteredTables.includes(tableName)) {
				unregisteredTables.push(tableName);
			}
		}
	}

	if (unregisteredTables.length > 0) {
		const tableList = unregisteredTables.map((t) => `"${t}"`).join(", ");
		throw new Error(
			`Query results contain columns for table(s) ${tableList} not passed to all()/one(). ` +
				`Add them to the tables array, or use query() for raw results.`,
		);
	}
}

// ============================================================================
// Main API
// ============================================================================

/**
 * Normalize raw SQL rows into deduplicated entities with resolved references.
 *
 * @example
 * const rows = [
 *   { "posts.id": "p1", "posts.authorId": "u1", "users.id": "u1", "users.name": "Alice" },
 *   { "posts.id": "p2", "posts.authorId": "u1", "users.id": "u1", "users.name": "Alice" },
 * ];
 *
 * const posts = normalize(rows, [posts, users]);
 * // posts[0].author === posts[1].author  // Same instance!
 */
export function normalize<T>(rows: RawRow[], tables: Table<any>[]): T[] {
	if (tables.length === 0) {
		throw new Error("At least one table is required");
	}

	if (rows.length === 0) {
		return [];
	}

	// Validate all joined tables are registered
	validateRegisteredTables(rows, tables);

	const entities = buildEntityMap(rows, tables);
	resolveReferences(entities, tables);

	const mainTable = tables[0];
	return extractMainEntities<T>(rows, mainTable, entities);
}

/**
 * Normalize a single row into an entity.
 *
 * Returns null if the main table has no data (e.g., no match).
 */
export function normalizeOne<T>(
	row: RawRow | null,
	tables: Table<any>[],
): T | null {
	if (!row) return null;

	const results = normalize<T>([row], tables);
	return results[0] ?? null;
}
