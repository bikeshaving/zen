/**
 * Bun.SQL adapter for @b9g/zen
 *
 * Unified driver supporting PostgreSQL, MySQL, and SQLite via Bun's built-in SQL.
 * Zero dependencies - uses native Bun implementation.
 */

import {SQL} from "bun";
import type {Driver, Table, EnsureResult} from "./zen.js";
import {
	ConstraintViolationError,
	EnsureError,
	SchemaDriftError,
	ConstraintPreflightError,
	isSQLSymbol,
	isSQLIdentifier,
	NOW,
} from "./zen.js";
import {generateDDL} from "./impl/ddl.js";
import {renderDDL} from "./impl/test-driver.js";

type SQLDialect = "sqlite" | "postgresql" | "mysql";

/**
 * Detect SQL dialect from URL.
 */
function detectDialect(url: string): SQLDialect {
	if (url.startsWith("postgres://") || url.startsWith("postgresql://")) {
		return "postgresql";
	}
	if (
		url.startsWith("mysql://") ||
		url.startsWith("mysql2://") ||
		url.startsWith("mariadb://")
	) {
		return "mysql";
	}
	// sqlite://, file:, :memory:, or plain filename
	return "sqlite";
}

/**
 * Resolve SQL symbol to dialect-specific SQL.
 */
function resolveSQLSymbol(sym: symbol): string {
	switch (sym) {
		case NOW:
			return "CURRENT_TIMESTAMP";
		default:
			throw new Error(`Unknown SQL symbol: ${String(sym)}`);
	}
}

/**
 * Quote an identifier (table name, column name) per dialect.
 * MySQL: backticks, PostgreSQL/SQLite: double quotes.
 */
function quoteIdent(name: string, dialect: SQLDialect): string {
	if (dialect === "mysql") {
		return `\`${name.replace(/`/g, "``")}\``;
	}
	// PostgreSQL and SQLite use double quotes
	return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Build SQL from template parts using the appropriate placeholder style.
 * SQLite/MySQL use ?, PostgreSQL uses $1, $2, etc.
 * SQL symbols and identifiers are inlined directly; other values use placeholders.
 */
function buildSQL(
	strings: TemplateStringsArray,
	values: unknown[],
	dialect: SQLDialect,
): {sql: string; params: unknown[]} {
	let sql = strings[0];
	const params: unknown[] = [];
	let paramIndex = 1;

	for (let i = 0; i < values.length; i++) {
		const value = values[i];
		if (isSQLSymbol(value)) {
			// Inline the symbol's SQL directly
			sql += resolveSQLSymbol(value) + strings[i + 1];
		} else if (isSQLIdentifier(value)) {
			// Quote identifier per dialect
			sql += quoteIdent(value.name, dialect) + strings[i + 1];
		} else {
			// Add placeholder and keep value
			const placeholder = dialect === "postgresql" ? `$${paramIndex++}` : "?";
			sql += placeholder + strings[i + 1];
			params.push(value);
		}
	}

	return {sql, params};
}

/**
 * Bun driver using Bun's built-in SQL.
 * Supports PostgreSQL, MySQL, and SQLite with automatic dialect detection.
 *
 * @example
 * import BunDriver from "@b9g/zen/bun";
 * import {Database} from "@b9g/zen";
 *
 * const driver = new BunDriver("postgres://localhost/mydb");
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
export default class BunDriver implements Driver {
	readonly supportsReturning: boolean;
	#dialect: SQLDialect;
	#sql: SQL;

	constructor(url: string, options?: Record<string, unknown>) {
		this.#dialect = detectDialect(url);
		this.#sql = new SQL(url, options as any);
		// MySQL doesn't support RETURNING, PostgreSQL and SQLite do
		this.supportsReturning = this.#dialect !== "mysql";
	}

	/**
	 * Convert database errors to Zealot errors.
	 */
	#handleError(error: unknown): never {
		if (error && typeof error === "object" && "code" in error) {
			const code = (error as any).code;
			const message = (error as any).message || String(error);

			// Handle constraint violations based on dialect
			if (this.#dialect === "sqlite") {
				// SQLite errors
				if (
					code === "SQLITE_CONSTRAINT" ||
					code === "SQLITE_CONSTRAINT_UNIQUE"
				) {
					// Extract table.column from message
					// Example: "UNIQUE constraint failed: users.email"
					const match = message.match(/constraint failed: (\w+)\.(\w+)/i);
					const table = match ? match[1] : undefined;
					const column = match ? match[2] : undefined;
					const constraint = match ? `${table}.${column}` : undefined;

					// Determine kind from error code
					let kind:
						| "unique"
						| "foreign_key"
						| "check"
						| "not_null"
						| "unknown" = "unknown";
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
			} else if (this.#dialect === "postgresql") {
				// PostgreSQL errors (23xxx = integrity constraint violation)
				if (
					code === "23505" ||
					code === "23503" ||
					code === "23514" ||
					code === "23502"
				) {
					const constraint =
						(error as any).constraint_name || (error as any).constraint;
					const table = (error as any).table_name || (error as any).table;
					const column = (error as any).column_name || (error as any).column;

					// PostgreSQL constraint violations (23505 = unique, 23503 = fk, 23514 = check, 23502 = not null)
					let kind:
						| "unique"
						| "foreign_key"
						| "check"
						| "not_null"
						| "unknown" = "unknown";
					if (code === "23505") kind = "unique";
					else if (code === "23503") kind = "foreign_key";
					else if (code === "23514") kind = "check";
					else if (code === "23502") kind = "not_null";

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
			} else if (this.#dialect === "mysql") {
				// MySQL errors
				if (
					code === "ER_DUP_ENTRY" ||
					code === "ER_NO_REFERENCED_ROW_2" ||
					code === "ER_ROW_IS_REFERENCED_2"
				) {
					let kind:
						| "unique"
						| "foreign_key"
						| "check"
						| "not_null"
						| "unknown" = "unknown";
					let constraint: string | undefined;
					let table: string | undefined;
					let column: string | undefined;

					if (code === "ER_DUP_ENTRY") {
						kind = "unique";
						// Example: "Duplicate entry 'value' for key 'table.index_name'"
						const keyMatch = message.match(/for key '([^']+)'/i);
						constraint = keyMatch ? keyMatch[1] : undefined;
						// Extract table from constraint name (e.g., "users.email_unique" -> "users")
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
						// Example: "Cannot add or update a child row: a foreign key constraint fails (`db`.`table`, CONSTRAINT `fk_name` ...)"
						const constraintMatch = message.match(/CONSTRAINT `([^`]+)`/i);
						constraint = constraintMatch ? constraintMatch[1] : undefined;
						const tableMatch = message.match(/`([^`]+)`\.`([^`]+)`/);
						if (tableMatch) {
							table = tableMatch[2]; // Second match is table name
						}
					}

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
		}
		throw error;
	}

	async all<T>(strings: TemplateStringsArray, values: unknown[]): Promise<T[]> {
		try {
			const {sql, params} = buildSQL(strings, values, this.#dialect);
			const result = await this.#sql.unsafe(sql, params as any[]);
			return result as T[];
		} catch (error) {
			this.#handleError(error);
		}
	}

	async get<T>(
		strings: TemplateStringsArray,
		values: unknown[],
	): Promise<T | null> {
		try {
			const {sql, params} = buildSQL(strings, values, this.#dialect);
			const result = await this.#sql.unsafe(sql, params as any[]);
			return (result[0] as T) ?? null;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async run(strings: TemplateStringsArray, values: unknown[]): Promise<number> {
		try {
			const {sql, params} = buildSQL(strings, values, this.#dialect);
			const result = await this.#sql.unsafe(sql, params as any[]);
			// Bun.SQL: MySQL uses affectedRows, PostgreSQL/SQLite use count
			// MySQL has count property but it's always 0 for UPDATE
			if (this.#dialect === "mysql") {
				return (result as any).affectedRows ?? result.length;
			}
			return (result as any).count ?? result.length;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async val<T>(
		strings: TemplateStringsArray,
		values: unknown[],
	): Promise<T | null> {
		try {
			const {sql, params} = buildSQL(strings, values, this.#dialect);
			const result = await this.#sql.unsafe(sql, params as any[]);
			if (result.length === 0) return null;
			const row = result[0] as Record<string, unknown>;
			const firstKey = Object.keys(row)[0];
			return row[firstKey] as T;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async close(): Promise<void> {
		await this.#sql.close();
	}

	async transaction<T>(fn: (txDriver: Driver) => Promise<T>): Promise<T> {
		const dialect = this.#dialect;
		const handleError = this.#handleError.bind(this);
		const supportsReturning = this.supportsReturning;

		// Bun.SQL's transaction() reserves a connection and provides a scoped SQL instance
		return await (this.#sql as any).transaction(async (txSql: any) => {
			// Create a transaction-bound driver that uses the transaction SQL
			const txDriver: Driver = {
				supportsReturning,
				all: async <R>(
					strings: TemplateStringsArray,
					values: unknown[],
				): Promise<R[]> => {
					try {
						const {sql, params} = buildSQL(strings, values, dialect);
						const result = await txSql.unsafe(sql, params as any[]);
						return result as R[];
					} catch (error) {
						return handleError(error);
					}
				},
				get: async <R>(
					strings: TemplateStringsArray,
					values: unknown[],
				): Promise<R | null> => {
					try {
						const {sql, params} = buildSQL(strings, values, dialect);
						const result = await txSql.unsafe(sql, params as any[]);
						return (result[0] as R) ?? null;
					} catch (error) {
						return handleError(error);
					}
				},
				run: async (
					strings: TemplateStringsArray,
					values: unknown[],
				): Promise<number> => {
					try {
						const {sql, params} = buildSQL(strings, values, dialect);
						const result = await txSql.unsafe(sql, params as any[]);
						return (
							(result as any).count ??
							(result as any).affectedRows ??
							result.length
						);
					} catch (error) {
						return handleError(error);
					}
				},
				val: async <R>(
					strings: TemplateStringsArray,
					values: unknown[],
				): Promise<R | null> => {
					try {
						const {sql, params} = buildSQL(strings, values, dialect);
						const result = await txSql.unsafe(sql, params as any[]);
						if (result.length === 0) return null;
						const row = result[0] as Record<string, unknown>;
						const firstKey = Object.keys(row)[0];
						return row[firstKey] as R;
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
	}

	async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
		if (this.#dialect === "postgresql") {
			// PostgreSQL: advisory lock
			const MIGRATION_LOCK_ID = 1952393421;
			await this.#sql.unsafe(`SELECT pg_advisory_lock($1)`, [
				MIGRATION_LOCK_ID,
			]);
			try {
				return await fn();
			} finally {
				await this.#sql.unsafe(`SELECT pg_advisory_unlock($1)`, [
					MIGRATION_LOCK_ID,
				]);
			}
		} else if (this.#dialect === "mysql") {
			// MySQL: named lock
			const LOCK_NAME = "zen_migration";
			const LOCK_TIMEOUT = 10;
			const result = await this.#sql.unsafe(`SELECT GET_LOCK(?, ?)`, [
				LOCK_NAME,
				LOCK_TIMEOUT,
			]);
			const acquired = (result as any)[0]?.["GET_LOCK(?, ?)"] === 1;

			if (!acquired) {
				throw new Error(
					`Failed to acquire migration lock after ${LOCK_TIMEOUT}s. Another migration may be in progress.`,
				);
			}

			try {
				return await fn();
			} finally {
				await this.#sql.unsafe(`SELECT RELEASE_LOCK(?)`, [LOCK_NAME]);
			}
		} else {
			// SQLite: exclusive transaction
			await this.#sql.unsafe("BEGIN EXCLUSIVE", []);
			try {
				const result = await fn();
				await this.#sql.unsafe("COMMIT", []);
				return result;
			} catch (error) {
				await this.#sql.unsafe("ROLLBACK", []);
				throw error;
			}
		}
	}

	// ==========================================================================
	// Schema Ensure Methods
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
				const ddlTemplate = generateDDL(table, {dialect: this.#dialect});
				const ddlSQL = renderDDL(ddlTemplate[0], ddlTemplate[1], this.#dialect);

				// Execute each statement (CREATE TABLE + CREATE INDEX statements)
				for (const stmt of ddlSQL.split(";").filter((s) => s.trim())) {
					await this.#sql.unsafe(stmt.trim(), []);
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
		if (this.#dialect === "sqlite") {
			const result = await this.#sql.unsafe(
				`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`,
				[tableName],
			);
			return result.length > 0;
		} else if (this.#dialect === "postgresql") {
			const result = await this.#sql.unsafe(
				`SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = 'public'`,
				[tableName],
			);
			return result.length > 0;
		} else {
			// MySQL
			const result = await this.#sql.unsafe(
				`SELECT 1 FROM information_schema.tables WHERE table_name = ? AND table_schema = DATABASE()`,
				[tableName],
			);
			return result.length > 0;
		}
	}

	async #getColumns(
		tableName: string,
	): Promise<{name: string; type: string; notnull: boolean}[]> {
		if (this.#dialect === "sqlite") {
			const result = await this.#sql.unsafe(
				`PRAGMA table_info(${quoteIdent(tableName, "sqlite")})`,
				[],
			);
			return (result as any[]).map((row) => ({
				name: row.name,
				type: row.type,
				notnull: row.notnull === 1,
			}));
		} else if (this.#dialect === "postgresql") {
			const result = await this.#sql.unsafe(
				`SELECT column_name as name, data_type as type, is_nullable = 'NO' as notnull
				 FROM information_schema.columns WHERE table_name = $1 AND table_schema = 'public'`,
				[tableName],
			);
			return result as any[];
		} else {
			// MySQL
			const result = await this.#sql.unsafe(
				`SELECT column_name as name, data_type as type, is_nullable = 'NO' as notnull
				 FROM information_schema.columns WHERE table_name = ? AND table_schema = DATABASE()`,
				[tableName],
			);
			return result as any[];
		}
	}

	async #getIndexes(
		tableName: string,
	): Promise<{name: string; columns: string[]; unique: boolean}[]> {
		if (this.#dialect === "sqlite") {
			const indexList = (await this.#sql.unsafe(
				`PRAGMA index_list(${quoteIdent(tableName, "sqlite")})`,
				[],
			)) as any[];

			const indexes: {name: string; columns: string[]; unique: boolean}[] = [];
			for (const idx of indexList) {
				if (idx.origin === "pk") continue; // Skip primary key index
				const indexInfo = (await this.#sql.unsafe(
					`PRAGMA index_info(${quoteIdent(idx.name, "sqlite")})`,
					[],
				)) as any[];
				indexes.push({
					name: idx.name,
					columns: indexInfo.map((col) => col.name),
					unique: idx.unique === 1,
				});
			}
			return indexes;
		} else if (this.#dialect === "postgresql") {
			const result = await this.#sql.unsafe(
				`SELECT i.relname as name,
				        array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns,
				        ix.indisunique as unique
				 FROM pg_index ix
				 JOIN pg_class i ON i.oid = ix.indexrelid
				 JOIN pg_class t ON t.oid = ix.indrelid
				 JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
				 WHERE t.relname = $1 AND NOT ix.indisprimary
				 GROUP BY i.relname, ix.indisunique`,
				[tableName],
			);
			return result as any[];
		} else {
			// MySQL
			const result = await this.#sql.unsafe(
				`SELECT index_name as name,
				        GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns,
				        NOT non_unique as \`unique\`
				 FROM information_schema.statistics
				 WHERE table_name = ? AND table_schema = DATABASE() AND index_name != 'PRIMARY'
				 GROUP BY index_name, non_unique`,
				[tableName],
			);
			return (result as any[]).map((row) => ({
				...row,
				columns: row.columns.split(","),
			}));
		}
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
		if (this.#dialect === "sqlite") {
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
			const fks = (await this.#sql.unsafe(
				`PRAGMA foreign_key_list(${quoteIdent(tableName, "sqlite")})`,
				[],
			)) as any[];

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
		} else if (this.#dialect === "postgresql") {
			const result = await this.#sql.unsafe(
				`SELECT
				   tc.constraint_name as name,
				   tc.constraint_type as type,
				   array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
				   ccu.table_name as referenced_table,
				   array_agg(ccu.column_name ORDER BY kcu.ordinal_position) as referenced_columns
				 FROM information_schema.table_constraints tc
				 JOIN information_schema.key_column_usage kcu
				   ON tc.constraint_name = kcu.constraint_name
				   AND tc.table_schema = kcu.table_schema
				   AND tc.table_name = kcu.table_name
				 LEFT JOIN information_schema.constraint_column_usage ccu
				   ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
				 WHERE tc.table_name = $1 AND tc.table_schema = 'public'
				   AND tc.constraint_type IN ('UNIQUE', 'FOREIGN KEY')
				 GROUP BY tc.constraint_name, tc.constraint_type, ccu.table_name`,
				[tableName],
			);
			// PostgreSQL array_agg returns "{col1,col2}" format - parse to JS array
			const parseArray = (s: string | null): string[] => {
				if (!s) return [];
				// Remove curly braces and split by comma
				return s
					.replace(/^\{|\}$/g, "")
					.split(",")
					.filter(Boolean);
			};
			return (result as any[]).map((row) => ({
				name: row.name,
				type: row.type === "UNIQUE" ? "unique" : "foreign_key",
				columns: parseArray(row.columns),
				referencedTable: row.referenced_table,
				referencedColumns: parseArray(row.referenced_columns),
			}));
		} else {
			// MySQL
			const result = await this.#sql.unsafe(
				`SELECT
				   tc.constraint_name as name,
				   tc.constraint_type as type,
				   GROUP_CONCAT(DISTINCT kcu.column_name ORDER BY kcu.ordinal_position) as columns,
				   kcu.referenced_table_name as referenced_table,
				   GROUP_CONCAT(DISTINCT kcu.referenced_column_name ORDER BY kcu.ordinal_position) as referenced_columns
				 FROM information_schema.table_constraints tc
				 JOIN information_schema.key_column_usage kcu
				   ON tc.constraint_name = kcu.constraint_name
				   AND tc.table_schema = kcu.table_schema
				   AND tc.table_name = kcu.table_name
				 WHERE tc.table_name = ? AND tc.table_schema = DATABASE()
				   AND tc.constraint_type IN ('UNIQUE', 'FOREIGN KEY')
				 GROUP BY tc.constraint_name, tc.constraint_type, kcu.referenced_table_name`,
				[tableName],
			);
			return (result as any[]).map((row) => ({
				name: row.name,
				type: row.type === "UNIQUE" ? "unique" : "foreign_key",
				columns: row.columns.split(","),
				referencedTable: row.referenced_table,
				referencedColumns: row.referenced_columns?.split(","),
			}));
		}
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
		const {generateColumnDDL} = await import("./impl/ddl.js");
		const zodType = table.schema.shape[fieldName];
		const fieldMeta = table.meta.fields[fieldName] || {};

		const colTemplate = generateColumnDDL(
			fieldName,
			zodType,
			fieldMeta,
			this.#dialect,
		);
		const colSQL = renderDDL(colTemplate[0], colTemplate[1], this.#dialect);

		// Note: SQLite doesn't support IF NOT EXISTS in ALTER TABLE ADD COLUMN
		// PostgreSQL 9.6+ supports ADD COLUMN IF NOT EXISTS
		// MySQL doesn't support it
		const ifNotExists = this.#dialect === "postgresql" ? "IF NOT EXISTS " : "";
		const sql = `ALTER TABLE ${quoteIdent(table.name, this.#dialect)} ADD COLUMN ${ifNotExists}${colSQL}`;
		await this.#sql.unsafe(sql, []);
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
				await this.#createIndex(table.name, indexName, [fieldName], false);
				applied = true;
			}
		}

		// Compound indexes
		for (const indexCols of table.indexes) {
			const indexName = `idx_${table.name}_${indexCols.join("_")}`;
			if (!existingIndexNames.has(indexName)) {
				await this.#createIndex(table.name, indexName, indexCols, false);
				applied = true;
			}
		}

		return applied;
	}

	async #createIndex(
		tableName: string,
		indexName: string,
		columns: string[],
		unique: boolean,
	): Promise<void> {
		const uniqueKw = unique ? "UNIQUE " : "";
		const colList = columns.map((c) => quoteIdent(c, this.#dialect)).join(", ");
		// MySQL doesn't support IF NOT EXISTS for CREATE INDEX
		const ifNotExists = this.#dialect === "mysql" ? "" : "IF NOT EXISTS ";
		const sql = `CREATE ${uniqueKw}INDEX ${ifNotExists}${quoteIdent(indexName, this.#dialect)} ON ${quoteIdent(tableName, this.#dialect)} (${colList})`;
		await this.#sql.unsafe(sql, []);
	}

	async #checkMissingConstraints<T extends Table<any>>(
		table: T,
	): Promise<void> {
		const existingConstraints = await this.#getConstraints(table.name);
		const meta = table.meta;
		const fields = Object.keys(meta.fields);

		// Check for missing unique constraints
		for (const fieldName of fields) {
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
							suggestion: `Run db.ensureConstraints(${table.name}) to apply constraints`,
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
					`Table "${table.name}" is missing FOREIGN KEY on column "${ref.fieldName}" referencing "${ref.table.name}"`,
					{
						table: table.name,
						drift: `missing fk:${ref.fieldName}->${ref.table.name}`,
						suggestion: `Run db.ensureConstraints(${table.name}) to apply constraints`,
					},
				);
			}
		}

		// Check for missing compound foreign keys
		for (const ref of table.compoundReferences) {
			const refFields = ref.referencedFields ?? ref.fields;
			const hasFk = existingConstraints.some((c) => {
				if (c.type !== "foreign_key") return false;
				if (c.columns.length !== ref.fields.length) return false;
				if (c.referencedTable !== ref.table.name) return false;
				// Check all local columns match (order matters)
				if (!ref.fields.every((field, i) => c.columns[i] === field)) return false;
				// Check all referenced columns match (order matters)
				return refFields.every((field, i) => c.referencedColumns?.[i] === field);
			});
			if (!hasFk) {
				throw new SchemaDriftError(
					`Table "${table.name}" is missing compound FOREIGN KEY on columns (${ref.fields.join(", ")}) referencing "${ref.table.name}"`,
					{
						table: table.name,
						drift: `missing fk:(${ref.fields.join(",")}) ->${ref.table.name}`,
						suggestion: `Run db.ensureConstraints(${table.name}) to apply constraints`,
					},
				);
			}
		}
	}

	async #ensureUniqueConstraints<T extends Table<any>>(
		table: T,
		existingConstraints: {type: string; columns: string[]}[],
	): Promise<boolean> {
		const meta = table.meta;
		const fields = Object.keys(meta.fields);
		let applied = false;

		for (const fieldName of fields) {
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
					const indexName = `uniq_${table.name}_${fieldName}`;
					await this.#createIndex(table.name, indexName, [fieldName], true);
					applied = true;
				}
			}
		}

		return applied;
	}

	async #ensureForeignKeys<T extends Table<any>>(
		table: T,
		existingConstraints: {
			type: string;
			columns: string[];
			referencedTable?: string;
		}[],
	): Promise<boolean> {
		const meta = table.meta;
		let applied = false;

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
				// Preflight: check for orphans
				await this.#preflightForeignKey(
					table.name,
					ref.fieldName,
					ref.table.name,
					ref.referencedField,
				);


			// Add FK constraint (SQLite requires table rebuild, not supported here)
			if (this.#dialect === "sqlite") {
				throw new Error(
					`Adding foreign key constraints to existing SQLite tables requires table rebuild. ` +
						`Table "${table.name}" column "${ref.fieldName}" -> "${ref.table.name}"."${ref.referencedField}". ` +
						`Please use a manual migration.`,
				);
			}

			// PostgreSQL and MySQL support ALTER TABLE ADD CONSTRAINT
			const constraintName = `${table.name}_${ref.fieldName}_fkey`;
			const onDelete = ref.onDelete
				? ` ON DELETE ${ref.onDelete.toUpperCase().replace(" ", " ")}`
				: "";

			const sql =
				`ALTER TABLE ${quoteIdent(table.name, this.#dialect)} ` +
				`ADD CONSTRAINT ${quoteIdent(constraintName, this.#dialect)} ` +
				`FOREIGN KEY (${quoteIdent(ref.fieldName, this.#dialect)}) ` +
				`REFERENCES ${quoteIdent(ref.table.name, this.#dialect)}(${quoteIdent(ref.referencedField, this.#dialect)})${onDelete}`;

			await this.#sql.unsafe(sql, []);
			applied = true;
			}
		}

		// Handle compound foreign keys
		for (const ref of table.compoundReferences) {
			const refFields = ref.referencedFields ?? ref.fields;
			const hasFk = existingConstraints.some((c) => {
				if (c.type !== "foreign_key") return false;
				if (c.columns.length !== ref.fields.length) return false;
				if (c.referencedTable !== ref.table.name) return false;
				// Check all local columns match (order matters)
				if (!ref.fields.every((field, i) => c.columns[i] === field)) return false;
				// Check all referenced columns match (order matters)
				return refFields.every((field, i) => c.referencedColumns?.[i] === field);
			});

			if (!hasFk) {
				// Preflight: check for orphans
				await this.#preflightCompoundForeignKey(
					table.name,
					ref.fields,
					ref.table.name,
					refFields,
				);

				// Add compound FK constraint (SQLite requires table rebuild, not supported here)
				if (this.#dialect === "sqlite") {
					throw new Error(
						`Adding foreign key constraints to existing SQLite tables requires table rebuild. ` +
							`Table "${table.name}" columns (${ref.fields.join(", ")}) -> "${ref.table.name}".(${refFields.join(", ")}). ` +
							`Please use a manual migration.`,
					);
				}

				// PostgreSQL and MySQL support ALTER TABLE ADD CONSTRAINT
				const constraintName = `${table.name}_${ref.fields.join("_")}_fkey`;
				const onDelete = ref.onDelete
					? ` ON DELETE ${ref.onDelete.toUpperCase().replace(" ", " ")}`
					: "";

				const localCols = ref.fields
					.map((f) => quoteIdent(f, this.#dialect))
					.join(", ");
				const refCols = refFields
					.map((f) => quoteIdent(f, this.#dialect))
					.join(", ");

				const sql =
					`ALTER TABLE ${quoteIdent(table.name, this.#dialect)} ` +
					`ADD CONSTRAINT ${quoteIdent(constraintName, this.#dialect)} ` +
					`FOREIGN KEY (${localCols}) ` +
					`REFERENCES ${quoteIdent(ref.table.name, this.#dialect)}(${refCols})${onDelete}`;

				await this.#sql.unsafe(sql, []);
				applied = true;
			}
		}

		return applied;
	}

	async #preflightUnique(tableName: string, columns: string[]): Promise<void> {
		const colList = columns.map((c) => quoteIdent(c, this.#dialect)).join(", ");
		// Use COUNT(*) > 1 directly in HAVING (PostgreSQL doesn't allow aliases in HAVING)
		const sql = `SELECT ${colList}, COUNT(*) as cnt FROM ${quoteIdent(tableName, this.#dialect)} GROUP BY ${colList} HAVING COUNT(*) > 1 LIMIT 1`;

		const result = await this.#sql.unsafe(sql, []);

		if (result.length > 0) {
			const diagQuery = `SELECT ${columns.join(", ")}, COUNT(*) as cnt FROM ${tableName} GROUP BY ${columns.join(", ")} HAVING COUNT(*) > 1`;

			// Count total duplicates
			const countSql = `SELECT COUNT(*) as total FROM (${sql.replace(" LIMIT 1", "")}) t`;
			const countResult = await this.#sql.unsafe(countSql, []);
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

	async #preflightForeignKey(
		tableName: string,
		column: string,
		refTable: string,
		refColumn: string,
	): Promise<void> {
		const sql = `SELECT 1 FROM ${quoteIdent(tableName, this.#dialect)} t
			WHERE t.${quoteIdent(column, this.#dialect)} IS NOT NULL
			AND NOT EXISTS (
				SELECT 1 FROM ${quoteIdent(refTable, this.#dialect)} r
				WHERE r.${quoteIdent(refColumn, this.#dialect)} = t.${quoteIdent(column, this.#dialect)}
			) LIMIT 1`;

		const result = await this.#sql.unsafe(sql, []);

		if (result.length > 0) {
			const diagQuery = `SELECT t.* FROM ${quoteIdent(tableName, this.#dialect)} t WHERE t.${quoteIdent(column, this.#dialect)} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ${quoteIdent(refTable, this.#dialect)} r WHERE r.${quoteIdent(refColumn, this.#dialect)} = t.${quoteIdent(column, this.#dialect)})`;

			// Count orphans
			const countSql = sql
				.replace("SELECT 1", "SELECT COUNT(*)")
				.replace(" LIMIT 1", "");
			const countResult = await this.#sql.unsafe(countSql, []);
			const violationCount =
				(countResult[0] as any)?.["COUNT(*)"] ??
				(countResult[0] as any)?.count ??
				1;

			throw new ConstraintPreflightError(
				`Cannot add FOREIGN KEY on "${tableName}"."${column}" -> "${refTable}"."${refColumn}": orphan records exist`,
				{
					table: tableName,
					constraint: `fk:${column}->${refTable}.${refColumn}`,
					violationCount,
					query: diagQuery,
				},
			);
		}
	}

	async #preflightCompoundForeignKey(
		tableName: string,
		columns: string[],
		refTable: string,
		refColumns: string[],
	): Promise<void> {
		// Build WHERE conditions for matching
		const joinConditions = columns
			.map(
				(col, i) =>
					`r.${quoteIdent(refColumns[i], this.#dialect)} = t.${quoteIdent(col, this.#dialect)}`,
			)
			.join(" AND ");

		// Check for NULL in any of the columns (skip those rows)
		const nullChecks = columns
			.map((col) => `t.${quoteIdent(col, this.#dialect)} IS NOT NULL`)
			.join(" AND ");

		const sql = `SELECT 1 FROM ${quoteIdent(tableName, this.#dialect)} t
			WHERE ${nullChecks}
			AND NOT EXISTS (
				SELECT 1 FROM ${quoteIdent(refTable, this.#dialect)} r
				WHERE ${joinConditions}
			) LIMIT 1`;

		const result = await this.#sql.unsafe(sql, []);

		if (result.length > 0) {
			const quotedCols = columns.map((c) => quoteIdent(c, this.#dialect)).join(", ");
			const diagQuery = `SELECT t.* FROM ${quoteIdent(tableName, this.#dialect)} t WHERE ${nullChecks} AND NOT EXISTS (SELECT 1 FROM ${quoteIdent(refTable, this.#dialect)} r WHERE ${joinConditions})`;

			// Count orphans
			const countSql = sql
				.replace("SELECT 1", "SELECT COUNT(*)")
				.replace(" LIMIT 1", "");
			const countResult = await this.#sql.unsafe(countSql, []);
			const violationCount =
				(countResult[0] as any)?.["COUNT(*)"] ??
				(countResult[0] as any)?.count ??
				1;

			throw new ConstraintPreflightError(
				`Cannot add compound FOREIGN KEY on "${tableName}".(${columns.join(", ")}) -> "${refTable}".(${refColumns.join(", ")}): orphan records exist`,
				{
					table: tableName,
					constraint: `fk:(${columns.join(",")}) ->${refTable}.(${refColumns.join(",")})`,
					violationCount,
					query: diagQuery,
				},
			);
		}
	}
}
