/**
 * postgres.js adapter for @b9g/zen
 *
 * Provides a Driver implementation for postgres.js.
 * Uses connection pooling - call close() when done to end all connections.
 *
 * Requires: postgres
 */

import type {Driver} from "./zen.js";
import {
	ConstraintViolationError,
	isSQLBuiltin,
	isSQLIdentifier,
} from "./zen.js";
import postgres from "postgres";

/**
 * Resolve SQL builtin to dialect-specific SQL.
 */
function resolveSQLBuiltin(sym: symbol): string {
	const key = Symbol.keyFor(sym);
	if (!key?.startsWith("@b9g/zen:")) {
		throw new Error(`Unknown SQL builtin: ${String(sym)}`);
	}
	// Strip the prefix and return the SQL keyword
	return key.slice("@b9g/zen:".length);
}

/**
 * Quote an identifier (table name, column name) using PostgreSQL double quotes.
 * Double quotes inside the name are doubled to escape.
 */
function quoteIdent(name: string): string {
	return `"${name.replace(/"/g, '""')}"`;
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
			// Inline the symbol's SQL directly
			sql += resolveSQLBuiltin(value) + strings[i + 1];
		} else if (isSQLIdentifier(value)) {
			// Quote identifier with PostgreSQL double quotes
			sql += quoteIdent(value.name) + strings[i + 1];
		} else {
			// Add placeholder and keep value
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

	async transaction<T>(fn: (txDriver: Driver) => Promise<T>): Promise<T> {
		const handleError = this.#handleError.bind(this);

		const result = await this.#sql.begin(async (txSql) => {
			const txDriver: Driver = {
				supportsReturning: true,
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
}
