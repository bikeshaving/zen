/**
 * Bun.SQL adapter for @b9g/zealot
 *
 * Unified driver supporting PostgreSQL, MySQL, and SQLite via Bun's built-in SQL.
 * Zero dependencies - uses native Bun implementation.
 */

import {SQL} from "bun";
import type {Driver, SQLDialect} from "./zealot.js";
import {ConstraintViolationError} from "./zealot.js";

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
	readonly dialect: SQLDialect;
	#sql: SQL;

	constructor(url: string, options?: Record<string, unknown>) {
		this.dialect = detectDialect(url);
		this.#sql = new SQL(url, options as any);
	}

	/**
	 * Convert database errors to Zealot errors.
	 */
	#handleError(error: unknown): never {
		if (error && typeof error === "object" && "code" in error) {
			const code = (error as any).code;
			const message = (error as any).message || String(error);

			// Handle constraint violations based on dialect
			if (this.dialect === "sqlite") {
				// SQLite errors
				if (code === "SQLITE_CONSTRAINT" || code === "SQLITE_CONSTRAINT_UNIQUE") {
					// Extract table.column from message
					// Example: "UNIQUE constraint failed: users.email"
					const match = message.match(/constraint failed: (\w+)\.(\w+)/i);
					const table = match ? match[1] : undefined;
					const column = match ? match[2] : undefined;
					const constraint = match ? `${table}.${column}` : undefined;

					// Determine kind from error code
					let kind: "unique" | "foreign_key" | "check" | "not_null" | "unknown" = "unknown";
					if (code === "SQLITE_CONSTRAINT_UNIQUE") kind = "unique";
					else if (message.includes("UNIQUE")) kind = "unique";
					else if (message.includes("FOREIGN KEY")) kind = "foreign_key";
					else if (message.includes("NOT NULL")) kind = "not_null";
					else if (message.includes("CHECK")) kind = "check";

					throw new ConstraintViolationError(message, {
						kind,
						constraint,
						table,
						column,
					}, {
						cause: error,
					});
				}
			} else if (this.dialect === "postgresql") {
				// PostgreSQL errors (23xxx = integrity constraint violation)
				if (code === "23505" || code === "23503" || code === "23514" || code === "23502") {
					const constraint = (error as any).constraint_name || (error as any).constraint;
					const table = (error as any).table_name || (error as any).table;
					const column = (error as any).column_name || (error as any).column;

					// PostgreSQL constraint violations (23505 = unique, 23503 = fk, 23514 = check, 23502 = not null)
					let kind: "unique" | "foreign_key" | "check" | "not_null" | "unknown" = "unknown";
					if (code === "23505") kind = "unique";
					else if (code === "23503") kind = "foreign_key";
					else if (code === "23514") kind = "check";
					else if (code === "23502") kind = "not_null";

					throw new ConstraintViolationError(message, {
						kind,
						constraint,
						table,
						column,
					}, {
						cause: error,
					});
				}
			} else if (this.dialect === "mysql") {
				// MySQL errors
				if (code === "ER_DUP_ENTRY" || code === "ER_NO_REFERENCED_ROW_2" || code === "ER_ROW_IS_REFERENCED_2") {
					let kind: "unique" | "foreign_key" | "check" | "not_null" | "unknown" = "unknown";
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
					} else if (code === "ER_NO_REFERENCED_ROW_2" || code === "ER_ROW_IS_REFERENCED_2") {
						kind = "foreign_key";
						// Example: "Cannot add or update a child row: a foreign key constraint fails (`db`.`table`, CONSTRAINT `fk_name` ...)"
						const constraintMatch = message.match(/CONSTRAINT `([^`]+)`/i);
						constraint = constraintMatch ? constraintMatch[1] : undefined;
						const tableMatch = message.match(/`([^`]+)`\.`([^`]+)`/);
						if (tableMatch) {
							table = tableMatch[2]; // Second match is table name
						}
					}

					throw new ConstraintViolationError(message, {
						kind,
						constraint,
						table,
						column,
					}, {
						cause: error,
					});
				}
			}
		}
		throw error;
	}

	async all<T>(query: string, params: unknown[]): Promise<T[]> {
		try {
			const result = await this.#sql.unsafe(query, params as any[]);
			return result as T[];
		} catch (error) {
			this.#handleError(error);
		}
	}

	async get<T>(query: string, params: unknown[]): Promise<T | null> {
		try {
			const result = await this.#sql.unsafe(query, params as any[]);
			return (result[0] as T) ?? null;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async run(query: string, params: unknown[]): Promise<number> {
		try {
			const result = await this.#sql.unsafe(query, params as any[]);
			// Bun.SQL: .count for postgres/sqlite, .affectedRows for mysql
			return (
				(result as any).count ?? (result as any).affectedRows ?? result.length
			);
		} catch (error) {
			this.#handleError(error);
		}
	}

	async val<T>(query: string, params: unknown[]): Promise<T> {
		try {
			const result = await this.#sql.unsafe(query, params as any[]);
			if (result.length === 0) return null as T;
			const row = result[0] as Record<string, unknown>;
			const firstKey = Object.keys(row)[0];
			return row[firstKey] as T;
		} catch (error) {
			this.#handleError(error);
		}
	}

	escapeIdentifier(name: string): string {
		if (this.dialect === "mysql") {
			// MySQL: backticks, doubled to escape
			return `\`${name.replace(/`/g, "``")}\``;
		}
		// PostgreSQL and SQLite: double quotes, doubled to escape
		return `"${name.replace(/"/g, '""')}"`;
	}

	async close(): Promise<void> {
		await this.#sql.close();
	}

	async transaction<T>(fn: () => Promise<T>): Promise<T> {
		// Bun.SQL: use appropriate BEGIN syntax for dialect
		const beginSql = this.dialect === "mysql" ? "START TRANSACTION" : "BEGIN";
		await this.#sql.unsafe(beginSql, []);
		try {
			const result = await fn();
			await this.#sql.unsafe("COMMIT", []);
			return result;
		} catch (error) {
			await this.#sql.unsafe("ROLLBACK", []);
			throw error;
		}
	}

	async insert(
		tableName: string,
		data: Record<string, unknown>,
	): Promise<Record<string, unknown>> {
		try {
			const columns = Object.keys(data);
			const values = Object.values(data);
			const columnList = columns.map((c) => this.escapeIdentifier(c)).join(", ");

			if (this.dialect === "mysql") {
				// MySQL: no RETURNING support
				const placeholders = columns.map(() => "?").join(", ");
				const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columnList}) VALUES (${placeholders})`;
				await this.#sql.unsafe(sql, values as any[]);
				return data;
			} else if (this.dialect === "postgresql") {
				// PostgreSQL: use $1, $2, etc. with RETURNING
				const placeholders = columns.map((_, i) => `$${i + 1}`).join(", ");
				const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columnList}) VALUES (${placeholders}) RETURNING *`;
				const result = await this.#sql.unsafe(sql, values as any[]);
				return result[0] as Record<string, unknown>;
			} else {
				// SQLite: use ? with RETURNING
				const placeholders = columns.map(() => "?").join(", ");
				const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columnList}) VALUES (${placeholders}) RETURNING *`;
				const result = await this.#sql.unsafe(sql, values as any[]);
				return result[0] as Record<string, unknown>;
			}
		} catch (error) {
			this.#handleError(error);
		}
	}

	async update(
		tableName: string,
		primaryKey: string,
		id: unknown,
		data: Record<string, unknown>,
	): Promise<Record<string, unknown> | null> {
		try {
			const columns = Object.keys(data);
			const values = Object.values(data);

			if (this.dialect === "mysql") {
				// MySQL: no RETURNING support
				const setClause = columns
					.map((c) => `${this.escapeIdentifier(c)} = ?`)
					.join(", ");
				const updateSql = `UPDATE ${this.escapeIdentifier(tableName)} SET ${setClause} WHERE ${this.escapeIdentifier(primaryKey)} = ?`;
				const result = await this.#sql.unsafe(updateSql, [
					...values,
					id,
				] as any[]);

				if ((result as any).affectedRows === 0) {
					return null;
				}

				// SELECT the updated row
				const selectSql = `SELECT * FROM ${this.escapeIdentifier(tableName)} WHERE ${this.escapeIdentifier(primaryKey)} = ?`;
				const rows = await this.#sql.unsafe(selectSql, [id] as any[]);
				return rows[0] as Record<string, unknown>;
			} else if (this.dialect === "postgresql") {
				// PostgreSQL: use $1, $2, etc. with RETURNING
				const setClause = columns
					.map((c, i) => `${this.escapeIdentifier(c)} = $${i + 1}`)
					.join(", ");
				const sql = `UPDATE ${this.escapeIdentifier(tableName)} SET ${setClause} WHERE ${this.escapeIdentifier(primaryKey)} = $${columns.length + 1} RETURNING *`;
				const result = await this.#sql.unsafe(sql, [...values, id] as any[]);
				return result[0] ?? null;
			} else {
				// SQLite: use ? with RETURNING
				const setClause = columns
					.map((c) => `${this.escapeIdentifier(c)} = ?`)
					.join(", ");
				const sql = `UPDATE ${this.escapeIdentifier(tableName)} SET ${setClause} WHERE ${this.escapeIdentifier(primaryKey)} = ? RETURNING *`;
				const result = await this.#sql.unsafe(sql, [...values, id] as any[]);
				return result[0] ?? null;
			}
		} catch (error) {
			this.#handleError(error);
		}
	}

	async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
		if (this.dialect === "postgresql") {
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
		} else if (this.dialect === "mysql") {
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
