/**
 * better-sqlite3 adapter for @b9g/zealot
 *
 * Provides a Driver implementation for better-sqlite3 (Node.js).
 * The connection is persistent - call close() when done.
 *
 * Requires: better-sqlite3
 */

import type {Driver} from "./zealot.js";
import {ConstraintViolationError} from "./zealot.js";
import Database from "better-sqlite3";

/**
 * Build SQL from template parts using ? placeholders.
 */
function buildSQL(strings: TemplateStringsArray, _values: unknown[]): string {
	let sql = strings[0];
	for (let i = 1; i < strings.length; i++) {
		sql += "?" + strings[i];
	}
	return sql;
}

/**
 * SQLite driver using better-sqlite3.
 *
 * @example
 * import SQLiteDriver from "@b9g/zealot/sqlite";
 * import {Database} from "@b9g/zealot";
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
			const sql = buildSQL(strings, values);
			return this.#db.prepare(sql).all(...values) as T[];
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async get<T>(
		strings: TemplateStringsArray,
		values: unknown[],
	): Promise<T | null> {
		try {
			const sql = buildSQL(strings, values);
			return (this.#db.prepare(sql).get(...values) as T) ?? null;
		} catch (error) {
			return this.#handleError(error);
		}
	}

	async run(strings: TemplateStringsArray, values: unknown[]): Promise<number> {
		try {
			const sql = buildSQL(strings, values);
			const result = this.#db.prepare(sql).run(...values);
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
			const sql = buildSQL(strings, values);
			return this.#db
				.prepare(sql)
				.pluck()
				.get(...values) as T;
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
}
