/**
 * mysql2 adapter for @b9g/zealot
 *
 * Provides a Driver implementation for mysql2.
 * Uses connection pooling - call close() when done to end all connections.
 *
 * Requires: mysql2
 */

import type {Driver} from "./zealot.js";
import {ConstraintViolationError} from "./zealot.js";
import mysql from "mysql2/promise";

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
 * import MySQLDriver from "@b9g/zealot/mysql";
 * import {Database} from "@b9g/zealot";
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
	readonly dialect = "mysql" as const;
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

			// MySQL constraint violations
			// ER_DUP_ENTRY = duplicate key/unique constraint
			// ER_NO_REFERENCED_ROW_2 = foreign key constraint (insert)
			// ER_ROW_IS_REFERENCED_2 = foreign key constraint (delete)
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

			if (code === "ER_DUP_ENTRY" || code === "ER_NO_REFERENCED_ROW_2" || code === "ER_ROW_IS_REFERENCED_2") {
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
		throw error;
	}

	async all<T>(sql: string, params: unknown[]): Promise<T[]> {
		try {
			const [rows] = await this.#pool.execute(sql, params);
			return rows as T[];
		} catch (error) {
			this.#handleError(error);
		}
	}

	async get<T>(sql: string, params: unknown[]): Promise<T | null> {
		try {
			const [rows] = await this.#pool.execute(sql, params);
			return ((rows as unknown[])[0] as T) ?? null;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async run(sql: string, params: unknown[]): Promise<number> {
		try {
			const [result] = await this.#pool.execute(sql, params);
			return (result as mysql.ResultSetHeader).affectedRows ?? 0;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async val<T>(sql: string, params: unknown[]): Promise<T> {
		try {
			const [rows] = await this.#pool.execute(sql, params);
			const row = (rows as unknown[])[0];
			if (!row) return null as T;
			const values = Object.values(row as object);
			return values[0] as T;
		} catch (error) {
			this.#handleError(error);
		}
	}

	escapeIdentifier(name: string): string {
		// MySQL: wrap in backticks, double any embedded backticks
		return `\`${name.replace(/`/g, "``")}\``;
	}

	async close(): Promise<void> {
		await this.#pool.end();
	}

	async transaction<T>(fn: () => Promise<T>): Promise<T> {
		// mysql2: get a dedicated connection from pool for transaction
		const connection = await this.#pool.getConnection();
		try {
			await connection.execute("START TRANSACTION", []);
			const result = await fn();
			await connection.execute("COMMIT", []);
			return result;
		} catch (error) {
			await connection.execute("ROLLBACK", []);
			throw error;
		} finally {
			connection.release();
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
			const placeholders = columns.map(() => "?").join(", ");

			// MySQL doesn't support RETURNING - need to INSERT then SELECT
			const insertSql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columnList}) VALUES (${placeholders})`;
			await this.#pool.execute(insertSql, values);

			// Get the inserted row using LAST_INSERT_ID() if there's an auto-increment
			// For now, just return the data as-is (caller should handle defaults)
			// TODO: This is a limitation - MySQL doesn't give us DB defaults easily
			return data;
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
			const setClause = columns
				.map((c) => `${this.escapeIdentifier(c)} = ?`)
				.join(", ");

			// MySQL doesn't support RETURNING - need to UPDATE then SELECT
			const updateSql = `UPDATE ${this.escapeIdentifier(tableName)} SET ${setClause} WHERE ${this.escapeIdentifier(primaryKey)} = ?`;
			const [result] = await this.#pool.execute(updateSql, [...values, id]);

			// Check if row was updated
			if ((result as mysql.ResultSetHeader).affectedRows === 0) {
				return null;
			}

			// SELECT the updated row
			const selectSql = `SELECT * FROM ${this.escapeIdentifier(tableName)} WHERE ${this.escapeIdentifier(primaryKey)} = ?`;
			const [rows] = await this.#pool.execute(selectSql, [id]);
			return (rows as unknown[])[0] as Record<string, unknown>;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
		// Use MySQL named lock for migrations
		// Timeout of 10 seconds to acquire lock
		const LOCK_NAME = "zealot_migration";
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
}
