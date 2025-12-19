/**
 * mysql2 adapter for @b9g/zen
 *
 * Provides a Driver implementation for mysql2.
 * Uses connection pooling - call close() when done to end all connections.
 *
 * Requires: mysql2
 */

import type {Driver, Table, EnsureResult} from "./zen.js";
import {
	ConstraintViolationError,
	isSQLBuiltin,
	isSQLIdentifier,
} from "./zen.js";
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
import mysql from "mysql2/promise";

const DIALECT = "mysql" as const;

/**
 * Quote an identifier using MySQL backticks.
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
		const value = values[i];
		if (isSQLBuiltin(value)) {
			sql += resolveSQLBuiltin(value) + strings[i + 1];
		} else if (isSQLIdentifier(value)) {
			sql += quoteIdent(value.name) + strings[i + 1];
		} else {
			sql += "?" + strings[i + 1];
			params.push(value);
		}
	}

	return {sql, params};
}

/**
 * Options for the mysql adapter.
 */
export interface MySQLOptions {
	/** Maximum number of connections in the pool (default: 10) */
	connectionLimit?: number;
	/** Idle timeout in milliseconds (default: 60000) */
	idleTimeout?: number;
	/** Connection timeout in milliseconds (default: 10000) */
	connectTimeout?: number;
}

/**
 * MySQL driver using mysql2.
 *
 * @example
 * import MySQLDriver from "@b9g/zen/mysql";
 * import {Database} from "@b9g/zen";
 *
 * const driver = new MySQLDriver("mysql://localhost/mydb");
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
export default class MySQLDriver implements Driver {
	readonly supportsReturning = false;
	#pool: mysql.Pool;

	constructor(url: string, options: MySQLOptions = {}) {
		this.#pool = mysql.createPool({
			uri: url,
			connectionLimit: options.connectionLimit ?? 10,
			idleTimeout: options.idleTimeout ?? 60000,
			connectTimeout: options.connectTimeout ?? 10000,
		});
	}

	/**
	 * Convert MySQL errors to Zealot errors.
	 */
	#handleError(error: unknown): never {
		if (error && typeof error === "object" && "code" in error) {
			const code = (error as any).code;
			const message = (error as any).message || String(error);

			let kind: "unique" | "foreign_key" | "check" | "not_null" | "unknown" =
				"unknown";
			let constraint: string | undefined;
			let table: string | undefined;
			let column: string | undefined;

			if (code === "ER_DUP_ENTRY") {
				kind = "unique";
				const keyMatch = message.match(/for key '([^']+)'/i);
				constraint = keyMatch ? keyMatch[1] : undefined;
				if (constraint) {
					const parts = constraint.split(".");
					if (parts.length > 1) {
						table = parts[0];
					}
				}
			} else if (
				code === "ER_NO_REFERENCED_ROW_2" ||
				code === "ER_ROW_IS_REFERENCED_2"
			) {
				kind = "foreign_key";
				const constraintMatch = message.match(/CONSTRAINT `([^`]+)`/i);
				constraint = constraintMatch ? constraintMatch[1] : undefined;
				const tableMatch = message.match(/`([^`]+)`\.`([^`]+)`/);
				if (tableMatch) {
					table = tableMatch[2];
				}
			}

			if (
				code === "ER_DUP_ENTRY" ||
				code === "ER_NO_REFERENCED_ROW_2" ||
				code === "ER_ROW_IS_REFERENCED_2"
			) {
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
			const [rows] = await this.#pool.execute(sql, params);
			return rows as T[];
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
			const [rows] = await this.#pool.execute(sql, params);
			return ((rows as unknown[])[0] as T) ?? null;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async run(strings: TemplateStringsArray, values: unknown[]): Promise<number> {
		try {
			const {sql, params} = buildSQL(strings, values);
			const [result] = await this.#pool.execute(sql, params);
			return (result as mysql.ResultSetHeader).affectedRows ?? 0;
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
			const [rows] = await this.#pool.execute(sql, params);
			const row = (rows as unknown[])[0];
			if (!row) return null;
			const rowValues = Object.values(row as object);
			return rowValues[0] as T;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async close(): Promise<void> {
		await this.#pool.end();
	}

	async transaction<T>(fn: (txDriver: Driver) => Promise<T>): Promise<T> {
		const connection = await this.#pool.getConnection();
		const handleError = this.#handleError.bind(this);

		try {
			await connection.execute("START TRANSACTION", []);

			const txDriver: Driver = {
				supportsReturning: false,
				all: async <R>(
					strings: TemplateStringsArray,
					values: unknown[],
				): Promise<R[]> => {
					try {
						const {sql, params} = buildSQL(strings, values);
						const [rows] = await connection.execute(sql, params);
						return rows as R[];
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
						const [rows] = await connection.execute(sql, params);
						return ((rows as unknown[])[0] as R) ?? null;
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
						const [result] = await connection.execute(sql, params);
						return (result as mysql.ResultSetHeader).affectedRows ?? 0;
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
						const [rows] = await connection.execute(sql, params);
						const row = (rows as unknown[])[0];
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

			const result = await fn(txDriver);
			await connection.execute("COMMIT", []);
			return result;
		} catch (error) {
			await connection.execute("ROLLBACK", []);
			throw error;
		} finally {
			connection.release();
		}
	}

	async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
		const LOCK_NAME = "zen_migration";
		const LOCK_TIMEOUT = 10;

		const [lockResult] = await this.#pool.execute(`SELECT GET_LOCK(?, ?)`, [
			LOCK_NAME,
			LOCK_TIMEOUT,
		]);
		const acquired = (lockResult as any[])[0]?.["GET_LOCK(?, ?)"] === 1;

		if (!acquired) {
			throw new Error(
				`Failed to acquire migration lock after ${LOCK_TIMEOUT}s. Another migration may be in progress.`,
			);
		}

		try {
			return await fn();
		} finally {
			await this.#pool.execute(`SELECT RELEASE_LOCK(?)`, [LOCK_NAME]);
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
					await this.#pool.execute(stmt.trim(), []);
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

	/**
	 * Ensure constraints exist on the table.
	 * Applies unique and foreign key constraints with preflight checks.
	 */
	async ensureConstraints<T extends Table<any>>(
		table: T,
	): Promise<EnsureResult> {
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
		const [rows] = await this.#pool.execute(
			`SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`,
			[tableName],
		);
		return ((rows as any[])[0]?.count ?? 0) > 0;
	}

	async #getColumns(
		tableName: string,
	): Promise<{name: string; type: string; notnull: boolean}[]> {
		const [rows] = await this.#pool.execute<mysql.RowDataPacket[]>(
			`SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position`,
			[tableName],
		);

		return rows.map((row) => ({
			name: row.column_name,
			type: row.data_type,
			notnull: row.is_nullable === "NO",
		}));
	}

	async #getIndexes(
		tableName: string,
	): Promise<{name: string; columns: string[]; unique: boolean}[]> {
		const [rows] = await this.#pool.execute<mysql.RowDataPacket[]>(
			`SELECT
				index_name,
				GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
				MAX(non_unique = 0) as is_unique
			FROM information_schema.statistics
			WHERE table_schema = DATABASE() AND table_name = ? AND index_name != 'PRIMARY'
			GROUP BY index_name`,
			[tableName],
		);

		return rows.map((row) => ({
			name: row.index_name,
			columns: row.columns.split(","),
			unique: row.is_unique === 1,
		}));
	}

	async #getConstraints(tableName: string): Promise<
		Array<{
			name: string;
			type: "unique" | "foreign_key" | "primary_key";
			columns: string[];
			referencedTable?: string;
			referencedColumns?: string[];
		}>
	> {
		const constraints: Array<{
			name: string;
			type: "unique" | "foreign_key" | "primary_key";
			columns: string[];
			referencedTable?: string;
			referencedColumns?: string[];
		}> = [];

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
		const [fkRows] = await this.#pool.execute<mysql.RowDataPacket[]>(
			`SELECT
				constraint_name,
				GROUP_CONCAT(column_name ORDER BY ordinal_position) as columns,
				referenced_table_name,
				GROUP_CONCAT(referenced_column_name ORDER BY ordinal_position) as ref_columns
			FROM information_schema.key_column_usage
			WHERE table_schema = DATABASE()
				AND table_name = ?
				AND referenced_table_name IS NOT NULL
			GROUP BY constraint_name, referenced_table_name`,
			[tableName],
		);

		for (const row of fkRows) {
			constraints.push({
				name: row.constraint_name,
				type: "foreign_key",
				columns: row.columns.split(","),
				referencedTable: row.referenced_table_name,
				referencedColumns: row.ref_columns.split(","),
			});
		}

		return constraints;
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
		const fieldMeta = table.meta.fields[fieldName] || {};

		const colTemplate = generateColumnDDL(
			fieldName,
			zodType,
			fieldMeta,
			DIALECT,
		);
		const colSQL = renderDDL(colTemplate[0], colTemplate.slice(1), DIALECT);

		// MySQL doesn't support IF NOT EXISTS for ADD COLUMN
		await this.#pool.execute(
			`ALTER TABLE ${quoteIdent(table.name)} ADD COLUMN ${colSQL}`,
			[],
		);
	}

	async #ensureMissingIndexes(table: Table): Promise<boolean> {
		const existingIndexes = await this.#getIndexes(table.name);
		const existingIndexNames = new Set(existingIndexes.map((idx) => idx.name));
		const meta = table.meta;

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
		// MySQL doesn't support IF NOT EXISTS for indexes
		const sql = `CREATE ${uniqueClause}INDEX ${quoteIdent(indexName)} ON ${quoteIdent(tableName)} (${columnList})`;
		await this.#pool.execute(sql, []);
		return indexName;
	}

	async #checkMissingConstraints(table: Table): Promise<void> {
		const existingConstraints = await this.#getConstraints(table.name);
		const meta = table.meta;

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
			type: "unique" | "foreign_key" | "primary_key";
			columns: string[];
		}>,
	): Promise<boolean> {
		const meta = table.meta;
		let applied = false;

		// Per-field unique constraints
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
			type: "unique" | "foreign_key" | "primary_key";
			columns: string[];
			referencedTable?: string;
			referencedColumns?: string[];
		}>,
	): Promise<boolean> {
		const meta = table.meta;
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

				await this.#pool.execute(
					`ALTER TABLE ${quoteIdent(table.name)} ADD CONSTRAINT ${quoteIdent(constraintName)} FOREIGN KEY (${quoteIdent(ref.fieldName)}) REFERENCES ${quoteIdent(ref.table.name)} (${quoteIdent(ref.referencedField)})${onDelete}`,
					[],
				);
				applied = true;
			}
		}

		return applied;
	}

	async #preflightUnique(tableName: string, columns: string[]): Promise<void> {
		const columnList = columns.map(quoteIdent).join(", ");
		const [rows] = await this.#pool.execute<mysql.RowDataPacket[]>(
			`SELECT COUNT(*) as count FROM ${quoteIdent(tableName)} GROUP BY ${columnList} HAVING COUNT(*) > 1`,
			[],
		);

		const violationCount = rows.length;

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
		const [rows] = await this.#pool.execute<mysql.RowDataPacket[]>(
			`SELECT COUNT(*) as count FROM ${quoteIdent(tableName)} t WHERE t.${quoteIdent(column)} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ${quoteIdent(refTable)} r WHERE r.${quoteIdent(refColumn)} = t.${quoteIdent(column)})`,
			[],
		);

		const violationCount = parseInt(String(rows[0]?.count ?? "0"), 10);

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
