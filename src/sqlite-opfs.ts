/**
 * SQLite OPFS adapter for @b9g/zen
 *
 * Provides a Driver implementation using @sqlite.org/sqlite-wasm with
 * opfs-sahpool VFS for persistent storage in the browser.
 *
 * Must run in a Web Worker for OPFS access.
 *
 * Requires: @sqlite.org/sqlite-wasm
 *
 * @example
 * // worker.ts
 * import {createSQLiteOPFSDriver} from "@b9g/zen/sqlite-opfs";
 * import {Database} from "@b9g/zen";
 *
 * const driver = await createSQLiteOPFSDriver("myapp.db");
 * const db = new Database(driver);
 * await db.open(1);
 */

import type {Driver, EnsureResult} from "./zen.js";
import {
	ConstraintViolationError,
	isSQLBuiltin,
	isSQLIdentifier,
} from "./zen.js";
import type {Table, View} from "./impl/table.js";
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
import sqlite3InitModule, {
	type Database as SQLite3Database,
	type BindingSpec,
} from "@sqlite.org/sqlite-wasm";

const DIALECT = "sqlite" as const;

/**
 * Quote an identifier using SQLite double quotes.
 */
function quoteIdent(name: string): string {
	return quoteIdentDialect(name, DIALECT);
}

/**
 * Build SQL from template parts using ? placeholders.
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
			if (typeof value === "boolean") {
				value = value ? 1 : 0;
			}
			params.push(value);
		}
	}

	return {sql, params};
}

/**
 * SQLite OPFS driver options.
 */
export interface SQLiteOPFSDriverOptions {
	/**
	 * OPFS directory for database storage.
	 * @default "/sqlite3"
	 */
	directory?: string;

	/**
	 * Clear existing data on initialization.
	 * @default false
	 */
	clearOnInit?: boolean;
}

/**
 * Create a SQLite driver with OPFS persistence.
 *
 * This function initializes the sqlite3 WASM module and sets up
 * the opfs-sahpool VFS for persistent storage.
 *
 * @param filename - Database filename (stored in OPFS)
 * @param options - Driver options
 * @returns Promise resolving to the driver instance
 *
 * @example
 * const driver = await createSQLiteOPFSDriver("myapp.db");
 * const db = new Database(driver);
 * await db.open(1);
 */
export async function createSQLiteOPFSDriver(
	filename: string,
	options: SQLiteOPFSDriverOptions = {},
): Promise<SQLiteOPFSDriver> {
	const {directory = "/sqlite3", clearOnInit = false} = options;

	// Initialize the sqlite3 module
	const sqlite3 = await sqlite3InitModule({
		print: console.log,
		printErr: console.error,
	});

	// Install the OPFS SAH Pool VFS
	const poolUtil = await sqlite3.installOpfsSAHPoolVfs({
		directory,
		clearOnInit,
	});

	// Open the database
	const db = new poolUtil.OpfsSAHPoolDb(filename);

	// Enable foreign key constraints
	db.exec("PRAGMA foreign_keys = ON");

	return new SQLiteOPFSDriver(db);
}

/**
 * SQLite OPFS driver using @sqlite.org/sqlite-wasm.
 *
 * Use createSQLiteOPFSDriver() to create instances - do not instantiate directly.
 */
export class SQLiteOPFSDriver implements Driver {
	readonly supportsReturning = true;
	#db: SQLite3Database;

	/** @internal */
	constructor(db: SQLite3Database) {
		this.#db = db;
	}

	/**
	 * Convert SQLite errors to zen errors.
	 */
	#handleError(error: unknown): never {
		if (error && typeof error === "object" && "message" in error) {
			const message = String((error as Error).message);

			// SQLite constraint violations
			if (message.includes("UNIQUE constraint failed")) {
				const match = message.match(/constraint failed: (\w+)\.(\w+)/i);
				const table = match ? match[1] : undefined;
				const column = match ? match[2] : undefined;
				const constraint = match ? `${table}.${column}` : undefined;

				throw new ConstraintViolationError(
					message,
					{kind: "unique", constraint, table, column},
					{cause: error},
				);
			}

			if (message.includes("FOREIGN KEY constraint failed")) {
				throw new ConstraintViolationError(
					message,
					{kind: "foreign_key"},
					{cause: error},
				);
			}

			if (message.includes("NOT NULL constraint failed")) {
				const match = message.match(/constraint failed: (\w+)\.(\w+)/i);
				const table = match ? match[1] : undefined;
				const column = match ? match[2] : undefined;

				throw new ConstraintViolationError(
					message,
					{kind: "not_null", table, column},
					{cause: error},
				);
			}
		}
		throw error;
	}

	async all<T>(strings: TemplateStringsArray, values: unknown[]): Promise<T[]> {
		try {
			const {sql, params} = buildSQL(strings, values);
			return this.#db.selectObjects(sql, params as BindingSpec) as T[];
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
			const rows = this.#db.selectObjects(sql, params as BindingSpec);
			return (rows[0] as T) ?? null;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async run(strings: TemplateStringsArray, values: unknown[]): Promise<number> {
		try {
			const {sql, params} = buildSQL(strings, values);
			this.#db.exec({sql, bind: params as BindingSpec});
			// Get changes count
			const changes = this.#db.selectValue("SELECT changes()") as number;
			return changes;
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
			const result = this.#db.selectValue(sql, params as BindingSpec);
			return (result as T) ?? null;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async close(): Promise<void> {
		this.#db.close();
	}

	// ==========================================================================
	// Type Encoding/Decoding
	// ==========================================================================

	encodeValue(value: unknown, fieldType: string): unknown {
		if (value === null || value === undefined) {
			return value;
		}

		switch (fieldType) {
			case "datetime":
				if (value instanceof Date && !isNaN(value.getTime())) {
					return value.toISOString();
				}
				return value;

			case "boolean":
				return value ? 1 : 0;

			case "json":
				return JSON.stringify(value);

			default:
				return value;
		}
	}

	decodeValue(value: unknown, fieldType: string): unknown {
		if (value === null || value === undefined) {
			return value;
		}

		switch (fieldType) {
			case "datetime":
				if (value instanceof Date) {
					if (isNaN(value.getTime())) {
						throw new Error(`Invalid Date object received from database`);
					}
					return value;
				}
				if (typeof value === "string") {
					const date = new Date(value);
					if (isNaN(date.getTime())) {
						throw new Error(
							`Invalid date value: "${value}" cannot be parsed as a valid date`,
						);
					}
					return date;
				}
				return value;

			case "boolean":
				if (typeof value === "number") {
					return value !== 0;
				}
				return value;

			case "json":
				if (typeof value === "string") {
					return JSON.parse(value);
				}
				return value;

			default:
				return value;
		}
	}

	async transaction<T>(fn: (txDriver: Driver) => Promise<T>): Promise<T> {
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
		// opfs-sahpool is single connection, so BEGIN EXCLUSIVE works
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
			// Step 0: Check if table exists
			const exists = await this.#tableExists(tableName);

			if (!exists) {
				// Step 1: Create table with full structure
				step = 1;
				const ddlTemplate = generateDDL(table, {dialect: DIALECT});
				const ddlSQL = renderDDL(ddlTemplate[0], ddlTemplate.slice(1), DIALECT);

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

				// Step 4: Check for missing constraints
				step = 4;
				await this.#checkMissingConstraints(table);
			}

			// Step 5: Ensure views exist
			step = 5;
			const viewsApplied = await this.#ensureViews(table);
			applied = applied || viewsApplied;

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

	async ensureView<T extends View<any>>(viewObj: T): Promise<EnsureResult> {
		const ddlTemplate = generateViewDDL(viewObj, {dialect: DIALECT});
		const ddlSQL = renderDDL(ddlTemplate[0], ddlTemplate.slice(1), DIALECT);

		for (const stmt of ddlSQL.split(";").filter((s) => s.trim())) {
			this.#db.exec(stmt.trim());
		}

		return {applied: true};
	}

	async #ensureViews<T extends Table<any>>(table: T): Promise<boolean> {
		const meta = getTableMeta(table);

		if (meta.softDeleteField && !meta.activeView) {
			void (table as any).active;
		}

		const activeView = meta.activeView;

		if (!activeView) {
			return false;
		}

		const ddlTemplate = generateViewDDL(activeView, {dialect: DIALECT});
		const ddlSQL = renderDDL(ddlTemplate[0], ddlTemplate.slice(1), DIALECT);

		for (const stmt of ddlSQL.split(";").filter((s) => s.trim())) {
			this.#db.exec(stmt.trim());
		}

		return true;
	}

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
			const exists = await this.#tableExists(tableName);
			if (!exists) {
				throw new Error(
					`Table "${tableName}" does not exist. Run ensureTable() first.`,
				);
			}

			step = 1;
			const existingConstraints = await this.#getConstraints(tableName);

			step = 2;
			const uniquesApplied = await this.#ensureUniqueConstraints(
				table,
				existingConstraints,
			);
			applied = applied || uniquesApplied;

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
	// Introspection Helpers
	// ==========================================================================

	async #tableExists(tableName: string): Promise<boolean> {
		const result = this.#db.selectObjects(
			`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`,
			[tableName],
		);
		return result.length > 0;
	}

	async #getColumns(
		tableName: string,
	): Promise<{name: string; type: string; notnull: boolean}[]> {
		const result = this.#db.selectObjects(
			`PRAGMA table_info(${quoteIdent(tableName)})`,
		);
		return result.map((row: any) => ({
			name: row.name,
			type: row.type,
			notnull: row.notnull === 1,
		}));
	}

	async #getIndexes(
		tableName: string,
	): Promise<{name: string; columns: string[]; unique: boolean}[]> {
		const indexList = this.#db.selectObjects(
			`PRAGMA index_list(${quoteIdent(tableName)})`,
		) as any[];

		const indexes: {name: string; columns: string[]; unique: boolean}[] = [];
		for (const idx of indexList) {
			if (idx.origin === "pk") continue;
			const indexInfo = this.#db.selectObjects(
				`PRAGMA index_info(${quoteIdent(idx.name)})`,
			) as any[];
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

		const fks = this.#db.selectObjects(
			`PRAGMA foreign_key_list(${quoteIdent(tableName)})`,
		) as any[];

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
	// Schema Ensure Helpers
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
		const fieldMeta = getTableMeta(table).fields[fieldName] || {};

		const colTemplate = generateColumnDDL(
			fieldName,
			zodType,
			fieldMeta,
			DIALECT,
		);
		const colSQL = renderDDL(colTemplate[0], colTemplate.slice(1), DIALECT);

		const sql = `ALTER TABLE ${quoteIdent(table.name)} ADD COLUMN ${colSQL}`;
		this.#db.exec(sql);
	}

	async #ensureMissingIndexes<T extends Table<any>>(
		table: T,
	): Promise<boolean> {
		const existingIndexes = await this.#getIndexes(table.name);
		const existingIndexNames = new Set(existingIndexes.map((i) => i.name));
		const meta = getTableMeta(table);

		let applied = false;

		for (const fieldName of meta.indexed) {
			const indexName = `idx_${table.name}_${fieldName}`;
			if (!existingIndexNames.has(indexName)) {
				await this.#createIndex(table.name, [fieldName], false);
				applied = true;
			}
		}

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
		const meta = getTableMeta(table);

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

	async #ensureUniqueConstraints<T extends Table<any>>(
		table: T,
		existingConstraints: {
			name: string;
			type: string;
			columns: string[];
		}[],
	): Promise<boolean> {
		const meta = getTableMeta(table);
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
					await this.#preflightUnique(table.name, [fieldName]);
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
		const meta = getTableMeta(table);

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

		const result = this.#db.selectObjects(sql);

		if (result.length > 0) {
			const diagQuery = `SELECT ${columns.join(", ")}, COUNT(*) as cnt FROM ${tableName} GROUP BY ${columns.join(", ")} HAVING COUNT(*) > 1`;

			const countSql = `SELECT COUNT(*) as total FROM (${sql.replace(" LIMIT 1", "")}) t`;
			const countResult = this.#db.selectObjects(countSql);
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

export default SQLiteOPFSDriver;
