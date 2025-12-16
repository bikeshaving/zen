/**
 * Bun.SQL adapter for @b9g/zealot
 *
 * Unified driver supporting PostgreSQL, MySQL, and SQLite via Bun's built-in SQL.
 * Zero dependencies - uses native Bun implementation.
 */

import {SQL} from "bun";
import type {Driver} from "./zealot.js";
import {ConstraintViolationError} from "./zealot.js";

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
 * Build SQL from template parts using the appropriate placeholder style.
 * SQLite/MySQL use ?, PostgreSQL uses $1, $2, etc.
 */
function buildSQL(
	strings: TemplateStringsArray,
	values: unknown[],
	dialect: SQLDialect,
): string {
	let sql = strings[0];
	for (let i = 1; i < strings.length; i++) {
		const placeholder = dialect === "postgresql" ? `$${i}` : "?";
		sql += placeholder + strings[i];
	}
	return sql;
}

/**
 * Bun driver using Bun's built-in SQL.
 * Supports PostgreSQL, MySQL, and SQLite with automatic dialect detection.
 *
 * @example
 * import BunDriver from "@b9g/zealot/bun";
 * import {Database} from "@b9g/zealot";
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
			const sql = buildSQL(strings, values, this.#dialect);
			const result = await this.#sql.unsafe(sql, values as any[]);
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
			const sql = buildSQL(strings, values, this.#dialect);
			const result = await this.#sql.unsafe(sql, values as any[]);
			return (result[0] as T) ?? null;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async run(strings: TemplateStringsArray, values: unknown[]): Promise<number> {
		try {
			const sql = buildSQL(strings, values, this.#dialect);
			const result = await this.#sql.unsafe(sql, values as any[]);
			// Bun.SQL: .count for postgres/sqlite, .affectedRows for mysql
			return (
				(result as any).count ?? (result as any).affectedRows ?? result.length
			);
		} catch (error) {
			this.#handleError(error);
		}
	}

	async val<T>(
		strings: TemplateStringsArray,
		values: unknown[],
	): Promise<T | null> {
		try {
			const sql = buildSQL(strings, values, this.#dialect);
			const result = await this.#sql.unsafe(sql, values as any[]);
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
						const sql = buildSQL(strings, values, dialect);
						const result = await txSql.unsafe(sql, values as any[]);
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
						const sql = buildSQL(strings, values, dialect);
						const result = await txSql.unsafe(sql, values as any[]);
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
						const sql = buildSQL(strings, values, dialect);
						const result = await txSql.unsafe(sql, values as any[]);
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
						const sql = buildSQL(strings, values, dialect);
						const result = await txSql.unsafe(sql, values as any[]);
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
			const LOCK_NAME = "zealot_migration";
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
}
