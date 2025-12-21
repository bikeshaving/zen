/**
 * Query layer - tagged template SQL with parameterized queries.
 *
 * Generates SELECT statements with prefixed column aliases for entity normalization.
 */

import {
	type Table,
	isTable,
	validateWithStandardSchema,
	decodeData,
	type DriverDecoder,
} from "./table.js";
import {
	ident,
	makeTemplate,
	isSQLTemplate,
	type SQLTemplate,
} from "./template.js";
import {renderSQL, type SQLDialect} from "./sql.js";

export type {SQLDialect} from "./sql.js";

export interface QueryOptions {
	dialect?: SQLDialect;
}

export interface ParsedQuery {
	sql: string;
	params: unknown[];
}

/**
 * Render a SQL template to {sql, params} format.
 * Useful for testing and debugging template output.
 *
 * @param template - SQLTemplate tuple [strings, ...values]
 * @param dialect - SQL dialect for identifier quoting (default: sqlite)
 * @returns {sql, params} for the rendered template
 */
export function renderFragment(
	template: SQLTemplate,
	dialect: SQLDialect = "sqlite",
): {sql: string; params: unknown[]} {
	return renderSQL(template[0], template.slice(1), dialect);
}

// ============================================================================
// Query Building
// ============================================================================

/**
 * Build SELECT clause as a template with ident markers.
 *
 * Returns a template tuple that can be rendered with renderSQL().
 * All identifiers use ident() markers instead of dialect-specific quoting.
 *
 * @example
 * const {strings, values} = buildSelectColumnsTemplate([posts, users]);
 * // values contains ident markers: [ident("posts"), ident("id"), ident("posts.id"), ...]
 */
export function buildSelectColumnsTemplate(tables: Table<any>[]): {
	strings: TemplateStringsArray;
	values: unknown[];
} {
	// Build arrays that maintain template invariant: strings.length === values.length + 1
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

			// Each column: "table"."column" AS "table.column"
			// Template: [prefix, ".", " AS ", ""] with [ident(table), ident(col), ident(alias)]
			const alias = `${tableName}.${fieldName}`;

			// Add comma separator if not first
			if (needsComma) {
				strings[strings.length - 1] += ", ";
			}
			needsComma = true;

			// Add: ${ident(table)}.${ident(col)} AS ${ident(alias)}
			values.push(ident(tableName));
			strings.push(".");
			values.push(ident(fieldName));
			strings.push(" AS ");
			values.push(ident(alias));
			strings.push("");
		}

		// Append derived expressions with auto-generated aliases
		const derivedExprs = (table.meta as any).derivedExprs ?? [];
		for (const expr of derivedExprs) {
			const alias = `${tableName}.${expr.fieldName}`;

			// Add comma separator if not first
			if (needsComma) {
				strings[strings.length - 1] += ", ";
			}
			needsComma = true;

			// Add opening paren and expression (expr.template is SQLTemplate: [strings, ...values])
			const template = expr.template;
			const templateStrings = template[0];
			const templateValues = template.slice(1);
			strings[strings.length - 1] += "(" + templateStrings[0];
			for (let i = 0; i < templateValues.length; i++) {
				values.push(templateValues[i]);
				strings.push(templateStrings[i + 1]);
			}

			// Add ) AS ${ident(alias)}
			strings[strings.length - 1] += ") AS ";
			values.push(ident(alias));
			strings.push("");
		}
	}

	return {strings: makeTemplate(strings), values};
}

/**
 * Build SELECT clause with prefixed column aliases.
 *
 * For derived tables (created via Table.derive()), this function:
 * - Skips derived fields when outputting regular columns
 * - Appends derived SQL expressions with auto-generated aliases
 *
 * @example
 * buildSelectColumns([posts, users], "sqlite")
 * // { sql: '"posts"."id" AS "posts.id", ...', params: [] }
 *
 * @example
 * // With derived table
 * const PostsWithCount = Posts.derive('likeCount', z.number())`COUNT(*)`;
 * buildSelectColumns([PostsWithCount], "sqlite")
 * // { sql: '"posts"."id" AS "posts.id", ..., COUNT(*) AS "posts.likeCount"', params: [] }
 */
export function buildSelectColumns(
	tables: Table<any>[],
	dialect: SQLDialect = "sqlite",
): {sql: string; params: unknown[]} {
	const template = buildSelectColumnsTemplate(tables);
	return renderSQL(template.strings, template.values, dialect);
}

/**
 * Expand a template by flattening nested fragments and converting tables to ident markers.
 *
 * This produces a flat template tuple with:
 * - SQLTemplate tuples merged in
 * - Table objects converted to ident() markers
 * - Regular values passed through
 *
 * The result can be rendered with renderSQL() for final SQL output.
 *
 * @param strings - Template strings
 * @param values - Template values
 * @returns Flat template tuple {strings, values}
 */
export function expandTemplate(
	strings: TemplateStringsArray,
	values: unknown[],
): {strings: TemplateStringsArray; values: unknown[]} {
	const newStrings: string[] = [strings[0]];
	const newValues: unknown[] = [];

	for (let i = 0; i < values.length; i++) {
		const value = values[i];

		if (isSQLTemplate(value)) {
			// Merge SQL template directly (tuple format: [strings, ...values])
			const valueStrings = value[0];
			const valueValues = value.slice(1);
			newStrings[newStrings.length - 1] += valueStrings[0];
			for (let j = 0; j < valueValues.length; j++) {
				newValues.push(valueValues[j]);
				newStrings.push(valueStrings[j + 1]);
			}
			newStrings[newStrings.length - 1] += strings[i + 1];
		} else if (isTable(value)) {
			// Convert table to ident marker
			newValues.push(ident(value.name));
			newStrings.push(strings[i + 1]);
		} else {
			// Regular value - pass through
			newValues.push(value);
			newStrings.push(strings[i + 1]);
		}
	}

	return {strings: makeTemplate(newStrings), values: newValues};
}

/**
 * Parse a tagged template into SQL string and params array.
 *
 * Supports:
 * - SQL templates/fragments: merged and parameterized
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
	// Expand nested fragments/tables to flat template
	const expanded = expandTemplate(strings, values);
	// Render to SQL with dialect-specific quoting
	const result = renderSQL(expanded.strings, expanded.values, dialect);
	return {sql: result.sql.trim(), params: result.params};
}

/**
 * Build a full SELECT query as a template with ident markers.
 *
 * Returns a template tuple that can be rendered with renderSQL().
 * The userClauses string is appended as-is (it should already contain placeholders).
 *
 * @example
 * const {strings, values} = buildQueryTemplate([posts, users], "WHERE published = ?");
 * // values contains ident markers plus any params from derived expressions
 */
/**
 * @internal
 * Build a query template from tables and raw SQL clauses.
 *
 * WARNING: userClauses is appended verbatim as raw SQL. Do not pass untrusted input.
 * For parameterized queries, use Database methods or tagged templates instead.
 */
export function buildQueryTemplate(
	tables: Table<any>[],
	userClauses: string = "",
): {strings: TemplateStringsArray; values: unknown[]} {
	if (tables.length === 0) {
		throw new Error("At least one table is required");
	}

	const mainTable = tables[0].name;
	const selectTemplate = buildSelectColumnsTemplate(tables);

	// Build: "SELECT " + selectCols + " FROM " + ident(mainTable) + " " + userClauses
	const strings: string[] = ["SELECT "];
	const values: unknown[] = [];

	// Merge selectTemplate
	strings[strings.length - 1] += selectTemplate.strings[0];
	for (let i = 0; i < selectTemplate.values.length; i++) {
		values.push(selectTemplate.values[i]);
		strings.push(selectTemplate.strings[i + 1]);
	}

	// Add FROM + table ident
	strings[strings.length - 1] += " FROM ";
	values.push(ident(mainTable));

	// Add user clauses
	if (userClauses.trim()) {
		strings.push(` ${userClauses}`);
	} else {
		strings.push("");
	}

	return {strings: makeTemplate(strings), values};
}

/**
 * @internal
 * Build a full SELECT query for tables with user-provided clauses.
 *
 * WARNING: userClauses is appended verbatim as raw SQL. Do not pass untrusted input.
 * For parameterized queries, use Database methods or tagged templates instead.
 *
 * @example
 * buildQuery([posts, users], "JOIN users ON users.id = posts.author_id WHERE published = ?", "sqlite")
 */
export function buildQuery(
	tables: Table<any>[],
	userClauses: string,
	dialect: SQLDialect = "sqlite",
): {sql: string; params: unknown[]} {
	const template = buildQueryTemplate(tables, userClauses);
	return renderSQL(template.strings, template.values, dialect);
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
		const {sql: userClauses, params: userParams} = parseTemplate(
			strings,
			values,
			dialect,
		);
		const {sql, params: selectParams} = buildQuery(
			tables,
			userClauses,
			dialect,
		);
		// Derived expression params come first (they're in the SELECT), then user params
		return {sql, params: [...selectParams, ...userParams]};
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
	const pk = table.meta.primary;

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
	driver?: DriverDecoder,
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
				// Decode JSON strings back to objects/arrays, then validate
				const decoded = decodeData(table, data, driver);
				const parsed = validateWithStandardSchema<Record<string, unknown>>(
					table.schema,
					decoded,
				);
				entities.set(key, parsed);
			}
		}
	}

	return entities;
}

/**
 * Resolve references for all entities in the map.
 *
 * Resolves both forward references (belongs-to) and reverse references (has-many).
 *
 * **Forward references** (`as`): Populates referenced entity as a single object.
 * **Reverse references** (`reverseAs`): Populates array of referencing entities.
 *
 * **Performance**: Uses indexing to avoid O(n²) scans - builds index once per table,
 * then attaches in O(n).
 *
 * **Ordering**: Reverse relationship arrays follow query result order, which is
 * database-dependent unless you specify ORDER BY in your SQL.
 */
export function resolveReferences(
	entities: EntityMap,
	tables: Table<any>[],
): void {
	// Forward references (belongs-to)
	// Enumerable: true (for serialization), writable: false (for immutability)
	for (const table of tables) {
		const refs = table.references();
		if (refs.length === 0) continue;

		const prefix = `${table.name}:`;

		for (const [key, entity] of entities) {
			if (!key.startsWith(prefix)) continue;

			for (const ref of refs) {
				const foreignKeyValue = entity[ref.fieldName];
				const refEntity =
					foreignKeyValue === null || foreignKeyValue === undefined
						? null
						: (entities.get(
								entityKey(ref.table.name, String(foreignKeyValue)),
							) ?? null);

				Object.defineProperty(entity, ref.as, {
					value: refEntity,
					enumerable: true,
					writable: false,
					configurable: true,
				});
			}
		}
	}

	// Reverse references (has-many) - indexed to avoid O(n²)
	// Build index: Map<tableName:fieldName, Map<foreignKeyValue, entity[]>>
	const reverseIndex = new Map<string, Map<string, any[]>>();

	for (const table of tables) {
		const refs = table.references();
		if (refs.length === 0) continue;

		for (const ref of refs) {
			if (!ref.reverseAs) continue;

			const indexKey = `${table.name}:${ref.fieldName}`;
			const fkIndex = new Map<string, any[]>();

			const prefix = `${table.name}:`;
			for (const [key, entity] of entities) {
				if (!key.startsWith(prefix)) continue;

				const fkValue = entity[ref.fieldName];
				if (fkValue === null || fkValue === undefined) continue;

				const fkStr = String(fkValue);
				if (!fkIndex.has(fkStr)) {
					fkIndex.set(fkStr, []);
				}
				fkIndex.get(fkStr)!.push(entity);
			}

			reverseIndex.set(indexKey, fkIndex);
		}
	}

	// Populate reverse relationships using index
	// Enumerable: false (prevents circular JSON), writable: false (immutable)
	for (const table of tables) {
		const refs = table.references();
		if (refs.length === 0) continue;

		for (const ref of refs) {
			if (!ref.reverseAs) continue;

			const indexKey = `${table.name}:${ref.fieldName}`;
			const fkIndex = reverseIndex.get(indexKey);
			if (!fkIndex) continue;

			const targetPrefix = `${ref.table.name}:`;
			for (const [key, entity] of entities) {
				if (!key.startsWith(targetPrefix)) continue;

				const pk = entity[ref.table.meta.primary!];
				if (pk === null || pk === undefined) continue;

				const pkStr = String(pk);
				Object.defineProperty(entity, ref.reverseAs, {
					value: fkIndex.get(pkStr) ?? [],
					enumerable: false,
					writable: false,
					configurable: true,
				});
			}
		}
	}
}

/**
 * Apply derived properties to entities.
 *
 * Derived properties are non-enumerable lazy getters that transform already-fetched data.
 * They must be pure functions (no I/O, no side effects).
 */
export function applyDerivedProperties(
	entities: EntityMap,
	tables: Table<any>[],
): void {
	for (const table of tables) {
		const derive = table.meta.derive;
		if (!derive) continue;

		const prefix = `${table.name}:`;

		for (const [key, entity] of entities) {
			if (!key.startsWith(prefix)) continue;

			for (const [propName, deriveFn] of Object.entries(derive)) {
				Object.defineProperty(entity, propName, {
					get() {
						return deriveFn(this);
					},
					enumerable: false,
					configurable: true,
				});
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
export function normalize<T>(
	rows: RawRow[],
	tables: Table<any>[],
	driver?: DriverDecoder,
): T[] {
	if (tables.length === 0) {
		throw new Error("At least one table is required");
	}

	if (rows.length === 0) {
		return [];
	}

	// Validate all joined tables are registered
	validateRegisteredTables(rows, tables);

	const entities = buildEntityMap(rows, tables, driver);
	resolveReferences(entities, tables);
	applyDerivedProperties(entities, tables);

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
	driver?: DriverDecoder,
): T | null {
	if (!row) return null;

	const results = normalize<T>([row], tables, driver);
	return results[0] ?? null;
}
