/**
 * mysql2 adapter for @b9g/zen
 *
 * Provides a Driver implementation for mysql2.
 * Uses connection pooling - call close() when done to end all connections.
 *
 * Requires: mysql2
 */

import type {Driver} from "./zen.js";
import {
	ConstraintViolationError,
	isSQLSymbol,
	isSQLIdentifier,
	NOW,
} from "./zen.js";
import mysql from "mysql2/promise";

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
 * Quote an identifier (table name, column name) using MySQL backticks.
 * Backticks inside the name are doubled to escape.
 */
function quoteIdent(name: string): string {
	return `\`${name.replace(/`/g, "``")}\``;
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
		if (isSQLSymbol(value)) {
			// Inline the symbol's SQL directly
			sql += resolveSQLSymbol(value) + strings[i + 1];
		} else if (isSQLIdentifier(value)) {
			// Quote identifier with MySQL backticks
			sql += quoteIdent(value.name) + strings[i + 1];
		} else {
			// Add placeholder and keep value
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
}
