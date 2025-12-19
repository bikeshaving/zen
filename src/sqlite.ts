/**
 * better-sqlite3 adapter for @b9g/zen
 *
 * Provides a Driver implementation for better-sqlite3 (Node.js).
 * The connection is persistent - call close() when done.
 *
 * Requires: better-sqlite3
 */

import type {Driver, EnsureResult} from "./zen.js";
import {
	ConstraintViolationError,
	isSQLBuiltin,
	isSQLIdentifier,
} from "./zen.js";
import type {Table} from "./impl/table.js";
import {
	EnsureError,
	SchemaDriftError,
	ConstraintPreflightError,
} from "./impl/errors.js";
import {generateDDL, generateColumnDDL} from "./impl/ddl.js";
import {
	renderDDL,
	quoteIdent as quoteIdentDialect,
	resolveSQLBuiltin,
} from "./impl/sql.js";
import Database from "better-sqlite3";

const DIALECT = "sqlite" as const;

/**
 * Quote an identifier using SQLite double quotes.
 */
function quoteIdent(name: string): string {
	return quoteIdentDialect(name, DIALECT);
}

/**
 * Build SQL from template parts using ? placeholders.
 * SQL symbols and identifiers are inlined directly; other values use placeholders.
 */
function buildSQL(
	strings: TemplateStringsArray,
	values: unknown[],
): {sql: string; params: unknown[]} {
	let sql = strings[0];
	const params: unknown[] = [];

	for (let i = 0; i < values.length; i++) {
		let value = values[i];
		if (isSQLBuiltin(value)) {
			sql += resolveSQLBuiltin(value) + strings[i + 1];
		} else if (isSQLIdentifier(value)) {
			sql += quoteIdent(value.name) + strings[i + 1];
		} else {
			sql += "?" + strings[i + 1];
			// Convert booleans to integers - better-sqlite3 doesn't accept true/false
			if (typeof value === "boolean") {
				value = value ? 1 : 0;
			}
			params.push(value);
		}
	}

	return {sql, params};
}

/**
 * SQLite driver using better-sqlite3.
 *
 * @example
 * import SQLiteDriver from "@b9g/zen/sqlite";
 * import {Database} from "@b9g/zen";
 *
 * const driver = new SQLiteDriver("file:app.db");
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
export default class SQLiteDriver implements Driver {
	readonly supportsReturning = true;
	#db: Database.Database;

	constructor(url: string) {
		// Handle file: prefix
		const path = url.startsWith("file:") ? url.slice(5) : url;
		this.#db = new Database(path);

		// Enable WAL mode for better concurrency
		this.#db.pragma("journal_mode = WAL");

		// Enable foreign key constraints
		this.#db.pragma("foreign_keys = ON");
	}

	/**
	 * Convert SQLite errors to Zealot errors.
	 */
	#handleError(error: unknown): never {
		if (error && typeof error === "object" && "code" in error) {
			const code = (error as any).code;
			const message = (error as any).message || String(error);

			// SQLite constraint violations
			if (code === "SQLITE_CONSTRAINT" || code === "SQLITE_CONSTRAINT_UNIQUE") {
				// Extract table.column from message
				// Example: "UNIQUE constraint failed: users.email"
				const match = message.match(/constraint failed: (\w+)\.(\w+)/i);
				const table = match ? match[1] : undefined;
				const column = match ? match[2] : undefined;
				const constraint = match ? `${table}.${column}` : undefined;

				// Determine kind from error code
				let kind: "unique" | "foreign_key" | "check" | "not_null" | "unknown" =
					"unknown";
				if (code === "SQLITE_CONSTRAINT_UNIQUE") kind = "unique";
				else if (message.includes("UNIQUE")) kind = "unique";
				else if (message.includes("FOREIGN KEY")) kind = "foreign_key";
				else if (message.includes("NOT NULL")) kind = "not_null";
				else if (message.includes("CHECK")) kind = "check";

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
			return this.#db.prepare(sql).all(...params) as T[];
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
			return (this.#db.prepare(sql).get(...params) as T) ?? null;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async run(strings: TemplateStringsArray, values: unknown[]): Promise<number> {
		try {
			const {sql, params} = buildSQL(strings, values);
			const result = this.#db.prepare(sql).run(...params);
			return result.changes;
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
			return this.#db
				.prepare(sql)
				.pluck()
				.get(...params) as T;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async close(): Promise<void> {
		this.#db.close();
	}

	async transaction<T>(fn: (txDriver: Driver) => Promise<T>): Promise<T> {
		// better-sqlite3 uses a single connection, so we pass `this` as the transaction driver.
		// All operations will use the same connection within the transaction.
		this.#db.exec("BEGIN");
		try {
			const result = await fn(this);
			this.#db.exec("COMMIT");
			return result;
		} catch (error) {
			this.#db.exec("ROLLBACK");
			throw error;
		}
	}

	async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
		// SQLite: BEGIN EXCLUSIVE acquires database-level write lock
		// This prevents all other connections from reading or writing
		this.#db.exec("BEGIN EXCLUSIVE");
		try {
			const result = await fn();
			this.#db.exec("COMMIT");
			return result;
		} catch (error) {
			this.#db.exec("ROLLBACK");
			throw error;
		}
	}

	// ==========================================================================
	// Schema Management
	// ==========================================================================

	async ensureTable<T extends Table<any>>(table: T): Promise<EnsureResult> {
		const tableName = table.name;
		let step = 0;
		let applied = false;

		try {
			// Step 0: Check if table exists
			const exists = await this.#tableExists(tableName);

			if (!exists) {
				// Step 1: Create table with full structure
				step = 1;
				const ddlTemplate = generateDDL(table, {dialect: DIALECT});
				const ddlSQL = renderDDL(ddlTemplate[0], ddlTemplate.slice(1), DIALECT);

				// Execute each statement (CREATE TABLE + CREATE INDEX statements)
				for (const stmt of ddlSQL.split(";").filter((s) => s.trim())) {
					this.#db.exec(stmt.trim());
				}
				applied = true;
			} else {
				// Step 2: Add missing columns
				step = 2;
				const columnsApplied = await this.#ensureMissingColumns(table);
				applied = applied || columnsApplied;

				// Step 3: Add missing non-unique indexes
				step = 3;
				const indexesApplied = await this.#ensureMissingIndexes(table);
				applied = applied || indexesApplied;

				// Step 4: Check for missing constraints (throws SchemaDriftError)
				step = 4;
				await this.#checkMissingConstraints(table);
			}

			return {applied};
		} catch (error) {
			if (error instanceof SchemaDriftError || error instanceof EnsureError) {
				throw error;
			}
			throw new EnsureError(
				`ensureTable failed at step ${step}: ${error instanceof Error ? error.message : String(error)}`,
				{operation: "ensureTable", table: tableName, step},
				{cause: error},
			);
		}
	}

	async ensureConstraints<T extends Table<any>>(
		table: T,
	): Promise<EnsureResult> {
		const tableName = table.name;
		let step = 0;
		let applied = false;

		try {
			// Step 0: Verify table exists
			const exists = await this.#tableExists(tableName);
			if (!exists) {
				throw new Error(
					`Table "${tableName}" does not exist. Run ensureTable() first.`,
				);
			}

			// Step 1: Get current constraints
			step = 1;
			const existingConstraints = await this.#getConstraints(tableName);

			// Step 2: Apply missing unique constraints with preflight
			step = 2;
			const uniquesApplied = await this.#ensureUniqueConstraints(
				table,
				existingConstraints,
			);
			applied = applied || uniquesApplied;

			// Step 3: Apply missing foreign keys with preflight
			step = 3;
			const fksApplied = await this.#ensureForeignKeys(
				table,
				existingConstraints,
			);
			applied = applied || fksApplied;

			return {applied};
		} catch (error) {
			if (
				error instanceof ConstraintPreflightError ||
				error instanceof EnsureError
			) {
				throw error;
			}
			throw new EnsureError(
				`ensureConstraints failed at step ${step}: ${error instanceof Error ? error.message : String(error)}`,
				{operation: "ensureConstraints", table: tableName, step},
				{cause: error},
			);
		}
	}

	// ==========================================================================
	// Introspection Helpers (private)
	// ==========================================================================

	async #tableExists(tableName: string): Promise<boolean> {
		const result = this.#db
			.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`)
			.all(tableName);
		return result.length > 0;
	}

	async #getColumns(
		tableName: string,
	): Promise<{name: string; type: string; notnull: boolean}[]> {
		const result = this.#db
			.prepare(`PRAGMA table_info(${quoteIdent(tableName)})`)
			.all();
		return (result as any[]).map((row) => ({
			name: row.name,
			type: row.type,
			notnull: row.notnull === 1,
		}));
	}

	async #getIndexes(
		tableName: string,
	): Promise<{name: string; columns: string[]; unique: boolean}[]> {
		const indexList = this.#db
			.prepare(`PRAGMA index_list(${quoteIdent(tableName)})`)
			.all() as any[];

		const indexes: {name: string; columns: string[]; unique: boolean}[] = [];
		for (const idx of indexList) {
			if (idx.origin === "pk") continue; // Skip primary key index
			const indexInfo = this.#db
				.prepare(`PRAGMA index_info(${quoteIdent(idx.name)})`)
				.all() as any[];
			indexes.push({
				name: idx.name,
				columns: indexInfo.map((col) => col.name),
				unique: idx.unique === 1,
			});
		}
		return indexes;
	}

	async #getConstraints(tableName: string): Promise<
		{
			name: string;
			type: "unique" | "foreign_key" | "primary_key";
			columns: string[];
			referencedTable?: string;
			referencedColumns?: string[];
		}[]
	> {
		const constraints: any[] = [];

		// Get unique constraints from indexes
		const indexes = await this.#getIndexes(tableName);
		for (const idx of indexes) {
			if (idx.unique) {
				constraints.push({
					name: idx.name,
					type: "unique",
					columns: idx.columns,
				});
			}
		}

		// Get foreign keys
		const fks = this.#db
			.prepare(`PRAGMA foreign_key_list(${quoteIdent(tableName)})`)
			.all() as any[];

		// Group by id (each FK can span multiple columns)
		const fkMap = new Map<
			number,
			{table: string; from: string[]; to: string[]}
		>();
		for (const fk of fks) {
			if (!fkMap.has(fk.id)) {
				fkMap.set(fk.id, {table: fk.table, from: [], to: []});
			}
			const entry = fkMap.get(fk.id)!;
			entry.from.push(fk.from);
			entry.to.push(fk.to);
		}

		for (const [id, fk] of fkMap) {
			constraints.push({
				name: `fk_${tableName}_${id}`,
				type: "foreign_key",
				columns: fk.from,
				referencedTable: fk.table,
				referencedColumns: fk.to,
			});
		}

		return constraints;
	}

	// ==========================================================================
	// Schema Ensure Helpers (private)
	// ==========================================================================

	async #ensureMissingColumns<T extends Table<any>>(
		table: T,
	): Promise<boolean> {
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

	async #addColumn<T extends Table<any>>(
		table: T,
		fieldName: string,
	): Promise<void> {
		const zodType = table.schema.shape[fieldName];
		const fieldMeta = table.meta.fields[fieldName] || {};

		const colTemplate = generateColumnDDL(
			fieldName,
			zodType,
			fieldMeta,
			DIALECT,
		);
		const colSQL = renderDDL(colTemplate[0], colTemplate.slice(1), DIALECT);

		// SQLite doesn't support IF NOT EXISTS in ALTER TABLE ADD COLUMN
		const sql = `ALTER TABLE ${quoteIdent(table.name)} ADD COLUMN ${colSQL}`;
		this.#db.exec(sql);
	}

	async #ensureMissingIndexes<T extends Table<any>>(
		table: T,
	): Promise<boolean> {
		const existingIndexes = await this.#getIndexes(table.name);
		const existingIndexNames = new Set(existingIndexes.map((i) => i.name));
		const meta = table.meta;

		let applied = false;

		// Per-field indexes
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
		// when upgrading from non-unique to unique index
		const prefix = unique ? "uniq" : "idx";
		const indexName = `${prefix}_${tableName}_${columns.join("_")}`;
		const uniqueClause = unique ? "UNIQUE " : "";
		const columnList = columns.map(quoteIdent).join(", ");
		const sql = `CREATE ${uniqueClause}INDEX IF NOT EXISTS ${quoteIdent(indexName)} ON ${quoteIdent(tableName)} (${columnList})`;
		this.#db.exec(sql);
		return indexName;
	}

	async #checkMissingConstraints<T extends Table<any>>(
		table: T,
	): Promise<void> {
		const existingConstraints = await this.#getConstraints(table.name);
		const meta = table.meta;

		// Check for missing unique constraints
		for (const fieldName of Object.keys(meta.fields)) {
			const fieldMeta = meta.fields[fieldName];
			if (fieldMeta.unique) {
				const hasUnique = existingConstraints.some(
					(c) =>
						c.type === "unique" &&
						c.columns.length === 1 &&
						c.columns[0] === fieldName,
				);
				if (!hasUnique) {
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

		// Check for missing foreign keys
		for (const ref of meta.references) {
			const hasFk = existingConstraints.some(
				(c) =>
					c.type === "foreign_key" &&
					c.columns.length === 1 &&
					c.columns[0] === ref.fieldName &&
					c.referencedTable === ref.table.name &&
					c.referencedColumns?.[0] === ref.referencedField,
			);
			if (!hasFk) {
				throw new SchemaDriftError(
					`Table "${table.name}" is missing FOREIGN KEY on column "${ref.fieldName}" -> "${ref.table.name}"."${ref.referencedField}"`,
					{
						table: table.name,
						drift: `missing fk:${ref.fieldName}`,
						suggestion: `Run ensureConstraints() to apply constraints`,
					},
				);
			}
		}
	}

	// ==========================================================================
	// Constraint Application Helpers (private)
	// ==========================================================================

	async #ensureUniqueConstraints<T extends Table<any>>(
		table: T,
		existingConstraints: {
			name: string;
			type: string;
			columns: string[];
		}[],
	): Promise<boolean> {
		const meta = table.meta;
		let applied = false;

		for (const fieldName of Object.keys(meta.fields)) {
			const fieldMeta = meta.fields[fieldName];
			if (fieldMeta.unique) {
				const hasUnique = existingConstraints.some(
					(c) =>
						c.type === "unique" &&
						c.columns.length === 1 &&
						c.columns[0] === fieldName,
				);

				if (!hasUnique) {
					// Preflight: check for duplicates
					await this.#preflightUnique(table.name, [fieldName]);

					// Apply constraint via unique index
					await this.#createIndex(table.name, [fieldName], true);
					applied = true;
				}
			}
		}

		return applied;
	}

	async #ensureForeignKeys<T extends Table<any>>(
		table: T,
		existingConstraints: {
			name: string;
			type: string;
			columns: string[];
			referencedTable?: string;
			referencedColumns?: string[];
		}[],
	): Promise<boolean> {
		const meta = table.meta;

		for (const ref of meta.references) {
			const hasFk = existingConstraints.some(
				(c) =>
					c.type === "foreign_key" &&
					c.columns.length === 1 &&
					c.columns[0] === ref.fieldName &&
					c.referencedTable === ref.table.name &&
					c.referencedColumns?.[0] === ref.referencedField,
			);

			if (!hasFk) {
				// SQLite cannot add FKs to existing tables
				throw new Error(
					`Adding foreign key constraints to existing SQLite tables requires table rebuild. ` +
						`Table "${table.name}" column "${ref.fieldName}" -> "${ref.table.name}"."${ref.referencedField}". ` +
						`Please use a manual migration.`,
				);
			}
		}

		return false;
	}

	async #preflightUnique(tableName: string, columns: string[]): Promise<void> {
		const columnList = columns.map(quoteIdent).join(", ");
		const sql = `SELECT ${columnList}, COUNT(*) as cnt FROM ${quoteIdent(tableName)} GROUP BY ${columnList} HAVING COUNT(*) > 1 LIMIT 1`;

		const result = this.#db.prepare(sql).all();

		if (result.length > 0) {
			const diagQuery = `SELECT ${columns.join(", ")}, COUNT(*) as cnt FROM ${tableName} GROUP BY ${columns.join(", ")} HAVING COUNT(*) > 1`;

			// Count total duplicates
			const countSql = `SELECT COUNT(*) as total FROM (${sql.replace(" LIMIT 1", "")}) t`;
			const countResult = this.#db.prepare(countSql).all();
			const violationCount = (countResult[0] as any)?.total ?? 1;

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
}
