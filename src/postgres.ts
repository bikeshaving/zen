/**
 * postgres.js adapter for @b9g/zen
 *
 * Provides a Driver implementation for postgres.js.
 * Uses connection pooling - call close() when done to end all connections.
 *
 * Requires: postgres
 */

import type {Driver, Table, EnsureResult} from "./zen.js";
import {
	ConstraintViolationError,
	isSQLBuiltin,
	isSQLIdentifier,
} from "./zen.js";
import type {View} from "./impl/table.js";
import {getTableMeta} from "./impl/table.js";
import {
	EnsureError,
	SchemaDriftError,
	ConstraintPreflightError,
} from "./impl/errors.js";
import {generateDDL, generateColumnDDL, generateViewDDL} from "./impl/ddl.js";
import {
	renderDDL,
	quoteIdent as quoteIdentDialect,
	resolveSQLBuiltin,
} from "./impl/sql.js";
import postgres from "postgres";

const DIALECT = "postgresql" as const;

/**
 * Quote an identifier using PostgreSQL double quotes.
 */
function quoteIdent(name: string): string {
	return quoteIdentDialect(name, DIALECT);
}

/**
 * Build SQL from template parts using $1, $2, etc. placeholders.
 * SQL symbols and identifiers are inlined directly; other values use placeholders.
 */
function buildSQL(
	strings: TemplateStringsArray,
	values: unknown[],
): {sql: string; params: unknown[]} {
	let sql = strings[0];
	const params: unknown[] = [];
	let paramIndex = 1;

	for (let i = 0; i < values.length; i++) {
		const value = values[i];
		if (isSQLBuiltin(value)) {
			sql += resolveSQLBuiltin(value) + strings[i + 1];
		} else if (isSQLIdentifier(value)) {
			sql += quoteIdent(value.name) + strings[i + 1];
		} else {
			sql += `$${paramIndex++}` + strings[i + 1];
			params.push(value);
		}
	}

	return {sql, params};
}

/**
 * Options for the postgres adapter.
 */
export interface PostgresOptions {
	/** Maximum number of connections in the pool (default: 10) */
	max?: number;
	/** Idle timeout in seconds before closing connections (default: 30) */
	idleTimeout?: number;
	/** Connection timeout in seconds (default: 30) */
	connectTimeout?: number;
}

/**
 * PostgreSQL driver using postgres.js.
 *
 * @example
 * import PostgresDriver from "@b9g/zen/postgres";
 * import {Database} from "@b9g/zen";
 *
 * const driver = new PostgresDriver("postgresql://localhost/mydb");
 * const db = new Database(driver);
 *
 * db.addEventListener("upgradeneeded", (e) => {
 *   e.waitUntil(runMigrations(e));
 * });
 *
 * await db.open(1);
 *
 * // When done:
 * await driver.close();
 */
export default class PostgresDriver implements Driver {
	readonly supportsReturning = true;
	#sql: ReturnType<typeof postgres>;

	constructor(url: string, options: PostgresOptions = {}) {
		this.#sql = postgres(url, {
			max: options.max ?? 10,
			idle_timeout: options.idleTimeout ?? 30,
			connect_timeout: options.connectTimeout ?? 30,
			onnotice: () => {}, // Suppress PostgreSQL NOTICE messages
		});
	}

	/**
	 * Convert PostgreSQL errors to Zealot errors.
	 */
	#handleError(error: unknown): never {
		if (error && typeof error === "object" && "code" in error) {
			const code = (error as any).code;
			const message = (error as any).message || String(error);
			const constraint =
				(error as any).constraint_name || (error as any).constraint;
			const table = (error as any).table_name || (error as any).table;
			const column = (error as any).column_name || (error as any).column;

			let kind: "unique" | "foreign_key" | "check" | "not_null" | "unknown" =
				"unknown";
			if (code === "23505") kind = "unique";
			else if (code === "23503") kind = "foreign_key";
			else if (code === "23514") kind = "check";
			else if (code === "23502") kind = "not_null";

			if (kind !== "unknown") {
				throw new ConstraintViolationError(
					message,
					{
						kind,
						constraint,
						table,
						column,
					},
					{
						cause: error,
					},
				);
			}
		}
		throw error;
	}

	async all<T>(strings: TemplateStringsArray, values: unknown[]): Promise<T[]> {
		try {
			const {sql, params} = buildSQL(strings, values);
			const result = await this.#sql.unsafe<T[]>(sql, params as any[]);
			return result;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async get<T>(
		strings: TemplateStringsArray,
		values: unknown[],
	): Promise<T | null> {
		try {
			const {sql, params} = buildSQL(strings, values);
			const result = await this.#sql.unsafe<T[]>(sql, params as any[]);
			return result[0] ?? null;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async run(strings: TemplateStringsArray, values: unknown[]): Promise<number> {
		try {
			const {sql, params} = buildSQL(strings, values);
			const result = await this.#sql.unsafe(sql, params as any[]);
			return result.count;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async val<T>(
		strings: TemplateStringsArray,
		values: unknown[],
	): Promise<T | null> {
		try {
			const {sql, params} = buildSQL(strings, values);
			const result = await this.#sql.unsafe(sql, params as any[]);
			const row = result[0];
			if (!row) return null;
			const rowValues = Object.values(row as object);
			return rowValues[0] as T;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async close(): Promise<void> {
		await this.#sql.end();
	}

	// ==========================================================================
	// Type Encoding/Decoding
	// ==========================================================================

	/**
	 * Encode a JS value for database insertion.
	 * PostgreSQL: postgres.js library handles Date and boolean natively.
	 */
	encodeValue(value: unknown, fieldType: string): unknown {
		if (value === null || value === undefined) {
			return value;
		}

		switch (fieldType) {
			case "datetime":
				// postgres.js handles Date natively
				return value;

			case "boolean":
				// postgres.js handles boolean natively
				return value;

			case "json":
				// Stringify for JSONB storage
				return JSON.stringify(value);

			default:
				return value;
		}
	}

	/**
	 * Decode a database value to JS.
	 * PostgreSQL: postgres.js returns proper types for most things.
	 */
	decodeValue(value: unknown, fieldType: string): unknown {
		if (value === null || value === undefined) {
			return value;
		}

		switch (fieldType) {
			case "datetime":
				// postgres.js returns Date objects
				if (value instanceof Date) {
					if (isNaN(value.getTime())) {
						throw new Error(`Invalid Date object received from database`);
					}
				}
				return value;

			case "boolean":
				// postgres.js returns native booleans
				return value;

			case "json":
				// postgres.js returns parsed JSONB as objects
				if (typeof value === "string") {
					return JSON.parse(value);
				}
				return value;

			default:
				return value;
		}
	}

	async transaction<T>(fn: (txDriver: Driver) => Promise<T>): Promise<T> {
		const handleError = this.#handleError.bind(this);
		// Capture encode/decode methods for transaction driver
		const encodeValue = this.encodeValue.bind(this);
		const decodeValue = this.decodeValue.bind(this);

		const result = await this.#sql.begin(async (txSql) => {
			const txDriver: Driver = {
				supportsReturning: true,
				encodeValue,
				decodeValue,
				all: async <R>(
					strings: TemplateStringsArray,
					values: unknown[],
				): Promise<R[]> => {
					try {
						const {sql, params} = buildSQL(strings, values);
						const result = await txSql.unsafe<R[]>(sql, params as any[]);
						return result;
					} catch (error) {
						return handleError(error);
					}
				},
				get: async <R>(
					strings: TemplateStringsArray,
					values: unknown[],
				): Promise<R | null> => {
					try {
						const {sql, params} = buildSQL(strings, values);
						const result = await txSql.unsafe<R[]>(sql, params as any[]);
						return result[0] ?? null;
					} catch (error) {
						return handleError(error);
					}
				},
				run: async (
					strings: TemplateStringsArray,
					values: unknown[],
				): Promise<number> => {
					try {
						const {sql, params} = buildSQL(strings, values);
						const result = await txSql.unsafe(sql, params as any[]);
						return result.count;
					} catch (error) {
						return handleError(error);
					}
				},
				val: async <R>(
					strings: TemplateStringsArray,
					values: unknown[],
				): Promise<R | null> => {
					try {
						const {sql, params} = buildSQL(strings, values);
						const result = await txSql.unsafe(sql, params as any[]);
						const row = result[0];
						if (!row) return null;
						const rowValues = Object.values(row as object);
						return rowValues[0] as R;
					} catch (error) {
						return handleError(error);
					}
				},
				close: async () => {
					// No-op for transaction driver
				},
				transaction: async () => {
					throw new Error("Nested transactions are not supported");
				},
			};

			return await fn(txDriver);
		});
		return result as T;
	}

	async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
		const MIGRATION_LOCK_ID = 1952393421;

		await this.#sql`SELECT pg_advisory_lock(${MIGRATION_LOCK_ID})`;
		try {
			return await fn();
		} finally {
			await this.#sql`SELECT pg_advisory_unlock(${MIGRATION_LOCK_ID})`;
		}
	}

	// ========================================================================
	// Schema Management Methods
	// ========================================================================

	/**
	 * Ensure table exists with the specified structure.
	 * Creates table if missing, adds missing columns/indexes.
	 * Throws SchemaDriftError if constraints are missing.
	 */
	async ensureTable<T extends Table<any>>(table: T): Promise<EnsureResult> {
		const meta = getTableMeta(table);
		if (meta.isView) {
			throw new Error(
				`Cannot ensure view "${table.name}". Use the base table "${meta.viewOf}" instead.`,
			);
		}
		const tableName = table.name;
		let step = 0;
		let applied = false;

		try {
			const exists = await this.#tableExists(tableName);

			if (!exists) {
				step = 1;
				// Create table with full structure using DDL generation
				const ddlTemplate = generateDDL(table, {dialect: DIALECT});
				const ddlSQL = renderDDL(ddlTemplate[0], ddlTemplate.slice(1), DIALECT);

				for (const stmt of ddlSQL.split(";").filter((s) => s.trim())) {
					await this.#sql.unsafe(stmt.trim());
				}
				applied = true;
			} else {
				step = 2;
				// Add missing columns, indexes, check constraints
				const columnsApplied = await this.#ensureMissingColumns(table);
				applied = applied || columnsApplied;

				step = 3;
				const indexesApplied = await this.#ensureMissingIndexes(table);
				applied = applied || indexesApplied;

				step = 4;
				await this.#checkMissingConstraints(table);
			}

			step = 5;
			const viewsApplied = await this.#ensureViews(table);
			applied = applied || viewsApplied;

			return {applied};
		} catch (error) {
			if (
				error instanceof SchemaDriftError ||
				error instanceof ConstraintPreflightError
			) {
				throw error;
			}

			throw new EnsureError(
				`Failed to ensure table "${tableName}" exists (step ${step})`,
				{
					operation: "ensureTable",
					table: tableName,
					step,
				},
				{
					cause: error,
				},
			);
		}
	}

	async ensureView<T extends View<any>>(viewObj: T): Promise<EnsureResult> {
		// Generate and execute the view DDL
		const ddlTemplate = generateViewDDL(viewObj, {dialect: DIALECT});
		const ddlSQL = renderDDL(ddlTemplate[0], ddlTemplate.slice(1), DIALECT);

		// Execute each statement (DROP VIEW + CREATE VIEW)
		for (const stmt of ddlSQL.split(";").filter((s) => s.trim())) {
			await this.#sql.unsafe(stmt.trim());
		}

		return {applied: true};
	}

	/**
	 * Ensure the active view exists for this table (if it has soft delete).
	 * Creates the view using generateViewDDL.
	 */
	async #ensureViews<T extends Table<any>>(table: T): Promise<boolean> {
		const meta = getTableMeta(table);

		// If table has soft delete, ensure the active view is registered
		// by accessing .active (which lazily creates it)
		if (meta.softDeleteField && !meta.activeView) {
			void (table as any).active;
		}

		const activeView = meta.activeView;

		// Skip if no active view registered
		if (!activeView) {
			return false;
		}

		// Generate and execute the view DDL
		const ddlTemplate = generateViewDDL(activeView, {dialect: DIALECT});
		const ddlSQL = renderDDL(ddlTemplate[0], ddlTemplate.slice(1), DIALECT);

		// Execute each statement (DROP VIEW + CREATE VIEW)
		for (const stmt of ddlSQL.split(";").filter((s) => s.trim())) {
			await this.#sql.unsafe(stmt.trim());
		}

		return true;
	}

	/**
	 * Ensure constraints exist on the table.
	 * Applies unique and foreign key constraints with preflight checks.
	 */
	async ensureConstraints<T extends Table<any>>(
		table: T,
	): Promise<EnsureResult> {
		const meta = getTableMeta(table);
		if (meta.isView) {
			throw new Error(
				`Cannot ensure view "${table.name}". Use the base table "${meta.viewOf}" instead.`,
			);
		}
		const tableName = table.name;
		let step = 0;
		let applied = false;

		try {
			step = 1;
			const existingConstraints = await this.#getConstraints(tableName);

			step = 2;
			const uniqueApplied = await this.#ensureUniqueConstraints(
				table,
				existingConstraints,
			);
			applied = applied || uniqueApplied;

			step = 3;
			const fkApplied = await this.#ensureForeignKeys(
				table,
				existingConstraints,
			);
			applied = applied || fkApplied;

			return {applied};
		} catch (error) {
			if (
				error instanceof SchemaDriftError ||
				error instanceof ConstraintPreflightError
			) {
				throw error;
			}

			throw new EnsureError(
				`Failed to ensure constraints on table "${tableName}" (step ${step})`,
				{
					operation: "ensureConstraints",
					table: tableName,
					step,
				},
				{
					cause: error,
				},
			);
		}
	}

	// ========================================================================
	// Private Helper Methods
	// ========================================================================

	async #tableExists(tableName: string): Promise<boolean> {
		const result = await this.#sql`
			SELECT EXISTS (
				SELECT FROM information_schema.tables
				WHERE table_schema = 'public'
				AND table_name = ${tableName}
			) as exists
		`;
		return result[0]?.exists ?? false;
	}

	async #getColumns(
		tableName: string,
	): Promise<{name: string; type: string; notnull: boolean}[]> {
		const result = await this.#sql<
			{column_name: string; data_type: string; is_nullable: string}[]
		>`
			SELECT column_name, data_type, is_nullable
			FROM information_schema.columns
			WHERE table_schema = 'public'
			AND table_name = ${tableName}
			ORDER BY ordinal_position
		`;

		return result.map((row) => ({
			name: row.column_name,
			type: row.data_type,
			notnull: row.is_nullable === "NO",
		}));
	}

	async #getIndexes(
		tableName: string,
	): Promise<{name: string; columns: string[]; unique: boolean}[]> {
		const result = await this.#sql<{indexname: string; indexdef: string}[]>`
			SELECT indexname, indexdef
			FROM pg_indexes
			WHERE schemaname = 'public'
			AND tablename = ${tableName}
		`;

		return result.map((row) => {
			// Parse column names from index definition
			// Example: CREATE UNIQUE INDEX idx_name ON table (col1, col2)
			const match = row.indexdef.match(/\((.*?)\)/);
			const columns = match
				? match[1].split(",").map((c) => c.trim().replace(/"/g, ""))
				: [];
			const unique = row.indexdef.includes("UNIQUE INDEX");

			return {
				name: row.indexname,
				columns,
				unique,
			};
		});
	}

	async #getConstraints(tableName: string): Promise<
		Array<{
			name: string;
			type: "unique" | "foreign_key" | "primary_key" | "check";
			columns: string[];
			referencedTable?: string;
			referencedColumns?: string[];
		}>
	> {
		const result = await this.#sql<
			{
				constraint_name: string;
				constraint_type: string;
				column_names: string;
				foreign_table_name: string | null;
				foreign_column_names: string | null;
			}[]
		>`
			SELECT
				tc.constraint_name,
				tc.constraint_type,
				array_agg(kcu.column_name ORDER BY kcu.ordinal_position)::text as column_names,
				ccu.table_name as foreign_table_name,
				array_agg(ccu.column_name ORDER BY kcu.ordinal_position)::text as foreign_column_names
			FROM information_schema.table_constraints tc
			LEFT JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
			LEFT JOIN information_schema.constraint_column_usage ccu
				ON tc.constraint_name = ccu.constraint_name
				AND tc.table_schema = ccu.table_schema
			WHERE tc.table_schema = 'public'
			AND tc.table_name = ${tableName}
			GROUP BY tc.constraint_name, tc.constraint_type, ccu.table_name
		`;

		return result.map((row) => {
			let type: "unique" | "foreign_key" | "primary_key" | "check";
			if (row.constraint_type === "UNIQUE") type = "unique";
			else if (row.constraint_type === "FOREIGN KEY") type = "foreign_key";
			else if (row.constraint_type === "PRIMARY KEY") type = "primary_key";
			else type = "check";

			// Parse PostgreSQL array format: {col1,col2}
			const parseArray = (str: string | null): string[] => {
				if (!str) return [];
				const match = str.match(/^\{(.*)\}$/);
				return match ? match[1].split(",").map((s) => s.trim()) : [];
			};

			return {
				name: row.constraint_name,
				type,
				columns: parseArray(row.column_names),
				referencedTable: row.foreign_table_name ?? undefined,
				referencedColumns: row.foreign_column_names
					? parseArray(row.foreign_column_names)
					: undefined,
			};
		});
	}

	async #ensureMissingColumns(table: Table): Promise<boolean> {
		const existingCols = await this.#getColumns(table.name);
		const existingColNames = new Set(existingCols.map((c) => c.name));
		const schemaFields = Object.keys(table.schema.shape);

		let applied = false;

		for (const fieldName of schemaFields) {
			if (!existingColNames.has(fieldName)) {
				await this.#addColumn(table, fieldName);
				applied = true;
			}
		}

		return applied;
	}

	async #addColumn(table: Table, fieldName: string): Promise<void> {
		const zodType = table.schema.shape[fieldName] as any;
		const fieldMeta = getTableMeta(table).fields[fieldName] || {};

		const colTemplate = generateColumnDDL(
			fieldName,
			zodType,
			fieldMeta,
			DIALECT,
		);
		const colSQL = renderDDL(colTemplate[0], colTemplate.slice(1), DIALECT);

		await this.#sql.unsafe(
			`ALTER TABLE ${quoteIdent(table.name)} ADD COLUMN IF NOT EXISTS ${colSQL}`,
		);
	}

	async #ensureMissingIndexes(table: Table): Promise<boolean> {
		const existingIndexes = await this.#getIndexes(table.name);
		const existingIndexNames = new Set(existingIndexes.map((idx) => idx.name));
		const meta = getTableMeta(table);

		let applied = false;

		// Per-field indexes (non-unique)
		for (const fieldName of meta.indexed) {
			const indexName = `idx_${table.name}_${fieldName}`;
			if (!existingIndexNames.has(indexName)) {
				await this.#createIndex(table.name, [fieldName], false);
				applied = true;
			}
		}

		// Compound indexes
		for (const indexCols of table.indexes) {
			const indexName = `idx_${table.name}_${indexCols.join("_")}`;
			if (!existingIndexNames.has(indexName)) {
				await this.#createIndex(table.name, indexCols, false);
				applied = true;
			}
		}

		return applied;
	}

	async #createIndex(
		tableName: string,
		columns: string[],
		unique: boolean,
	): Promise<string> {
		// Use different prefixes for unique vs non-unique to avoid conflicts
		const prefix = unique ? "uniq" : "idx";
		const indexName = `${prefix}_${tableName}_${columns.join("_")}`;
		const uniqueClause = unique ? "UNIQUE " : "";
		const columnList = columns.map(quoteIdent).join(", ");
		const sql = `CREATE ${uniqueClause}INDEX IF NOT EXISTS ${quoteIdent(indexName)} ON ${quoteIdent(tableName)} (${columnList})`;
		await this.#sql.unsafe(sql);
		return indexName;
	}

	async #checkMissingConstraints(table: Table): Promise<void> {
		const existingConstraints = await this.#getConstraints(table.name);
		const meta = getTableMeta(table);

		// Check unique constraints
		for (const fieldName of Object.keys(meta.fields)) {
			const fieldMeta = meta.fields[fieldName];
			if (fieldMeta.unique) {
				const hasConstraint = existingConstraints.some(
					(c) =>
						c.type === "unique" &&
						c.columns.length === 1 &&
						c.columns[0] === fieldName,
				);
				if (!hasConstraint) {
					throw new SchemaDriftError(
						`Table "${table.name}" is missing UNIQUE constraint on column "${fieldName}"`,
						{
							table: table.name,
							drift: `missing unique:${fieldName}`,
							suggestion: `Run ensureConstraints() to apply constraints`,
						},
					);
				}
			}
		}

		// Check foreign keys
		for (const ref of meta.references) {
			const hasFK = existingConstraints.some(
				(c) =>
					c.type === "foreign_key" &&
					c.columns.length === 1 &&
					c.columns[0] === ref.fieldName &&
					c.referencedTable === ref.table.name &&
					c.referencedColumns?.[0] === ref.referencedField,
			);
			if (!hasFK) {
				throw new SchemaDriftError(
					`Table "${table.name}" is missing FOREIGN KEY constraint on column "${ref.fieldName}"`,
					{
						table: table.name,
						drift: `missing foreign_key:${ref.fieldName}->${ref.table.name}.${ref.referencedField}`,
						suggestion: `Run ensureConstraints() to apply constraints`,
					},
				);
			}
		}
	}

	async #ensureUniqueConstraints(
		table: Table,
		existingConstraints: Array<{
			name: string;
			type: "unique" | "foreign_key" | "primary_key" | "check";
			columns: string[];
		}>,
	): Promise<boolean> {
		const meta = getTableMeta(table);
		let applied = false;

		// Per-field unique constraints
		for (const fieldName of Object.keys(meta.fields)) {
			const fieldMeta = meta.fields[fieldName];
			if (fieldMeta.unique) {
				const hasConstraint = existingConstraints.some(
					(c) => c.type === "unique" && c.columns.includes(fieldName),
				);

				if (!hasConstraint) {
					// Preflight check
					await this.#preflightUnique(table.name, [fieldName]);

					// Create unique index
					await this.#createIndex(table.name, [fieldName], true);
					applied = true;
				}
			}
		}

		return applied;
	}

	async #ensureForeignKeys(
		table: Table,
		existingConstraints: Array<{
			name: string;
			type: "unique" | "foreign_key" | "primary_key" | "check";
			columns: string[];
			referencedTable?: string;
			referencedColumns?: string[];
		}>,
	): Promise<boolean> {
		const meta = getTableMeta(table);
		let applied = false;

		for (const ref of meta.references) {
			const hasFK = existingConstraints.some(
				(c) =>
					c.type === "foreign_key" &&
					c.columns.length === 1 &&
					c.columns[0] === ref.fieldName &&
					c.referencedTable === ref.table.name &&
					c.referencedColumns?.[0] === ref.referencedField,
			);

			if (!hasFK) {
				// Preflight check
				await this.#preflightForeignKey(
					table.name,
					ref.fieldName,
					ref.table.name,
					ref.referencedField,
				);

				// Add foreign key constraint
				const constraintName = `fk_${table.name}_${ref.fieldName}`;
				const onDelete = ref.onDelete
					? ` ON DELETE ${ref.onDelete.toUpperCase()}`
					: "";

				await this.#sql.unsafe(
					`ALTER TABLE ${quoteIdent(table.name)} ADD CONSTRAINT ${quoteIdent(constraintName)} FOREIGN KEY (${quoteIdent(ref.fieldName)}) REFERENCES ${quoteIdent(ref.table.name)} (${quoteIdent(ref.referencedField)})${onDelete}`,
				);
				applied = true;
			}
		}

		return applied;
	}

	async #preflightUnique(tableName: string, columns: string[]): Promise<void> {
		const columnList = columns.map(quoteIdent).join(", ");
		const result = await this.#sql.unsafe<{count: string}[]>(
			`SELECT COUNT(*) as count FROM ${quoteIdent(tableName)} GROUP BY ${columnList} HAVING COUNT(*) > 1`,
		);

		const violationCount = result.length;

		if (violationCount > 0) {
			const diagQuery = `SELECT ${columnList}, COUNT(*) FROM ${quoteIdent(tableName)} GROUP BY ${columnList} HAVING COUNT(*) > 1`;

			throw new ConstraintPreflightError(
				`Cannot add UNIQUE constraint on "${tableName}"(${columns.join(", ")}): duplicate values exist`,
				{
					table: tableName,
					constraint: `unique:${columns.join(",")}`,
					violationCount,
					query: diagQuery,
				},
			);
		}
	}

	async #preflightForeignKey(
		tableName: string,
		column: string,
		refTable: string,
		refColumn: string,
	): Promise<void> {
		const result = await this.#sql.unsafe<{count: string}[]>(
			`SELECT COUNT(*) as count FROM ${quoteIdent(tableName)} t WHERE t.${quoteIdent(column)} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ${quoteIdent(refTable)} r WHERE r.${quoteIdent(refColumn)} = t.${quoteIdent(column)})`,
		);

		const violationCount = parseInt(result[0]?.count ?? "0", 10);

		if (violationCount > 0) {
			const diagQuery = `SELECT t.${quoteIdent(column)} FROM ${quoteIdent(tableName)} t WHERE t.${quoteIdent(column)} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ${quoteIdent(refTable)} r WHERE r.${quoteIdent(refColumn)} = t.${quoteIdent(column)})`;

			throw new ConstraintPreflightError(
				`Cannot add FOREIGN KEY constraint on "${tableName}"(${column}): ${violationCount} orphaned rows exist`,
				{
					table: tableName,
					constraint: `foreign_key:${column}->${refTable}.${refColumn}`,
					violationCount,
					query: diagQuery,
				},
			);
		}
	}
}
