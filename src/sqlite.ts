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
	readonly dialect = "sqlite" as const;
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
		}
		throw error;
	}

	async all<T>(sql: string, params: unknown[]): Promise<T[]> {
		try {
			return this.#db.prepare(sql).all(...params) as T[];
		} catch (error) {
			this.#handleError(error);
		}
	}

	async get<T>(sql: string, params: unknown[]): Promise<T | null> {
		try {
			return (this.#db.prepare(sql).get(...params) as T) ?? null;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async run(sql: string, params: unknown[]): Promise<number> {
		try {
			const result = this.#db.prepare(sql).run(...params);
			return result.changes;
		} catch (error) {
			this.#handleError(error);
		}
	}

	async val<T>(sql: string, params: unknown[]): Promise<T> {
		try {
			return this.#db
				.prepare(sql)
				.pluck()
				.get(...params) as T;
		} catch (error) {
			this.#handleError(error);
		}
	}

	escapeIdentifier(name: string): string {
		// SQLite: wrap in double quotes, double any embedded quotes
		return `"${name.replace(/"/g, '""')}"`;
	}

	async close(): Promise<void> {
		this.#db.close();
	}

	async transaction<T>(fn: () => Promise<T>): Promise<T> {
		// better-sqlite3 doesn't support async in transactions by default
		// Use BEGIN/COMMIT with error handling
		this.#db.exec("BEGIN");
		try {
			const result = await fn();
			this.#db.exec("COMMIT");
			return result;
		} catch (error) {
			this.#db.exec("ROLLBACK");
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
			const placeholders = columns.map(() => "?").join(", ");

			// SQLite supports RETURNING
			const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columnList}) VALUES (${placeholders}) RETURNING *`;
			const row = this.#db.prepare(sql).get(...values) as Record<string, unknown>;
			return row;
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

			// SQLite supports RETURNING
			const sql = `UPDATE ${this.escapeIdentifier(tableName)} SET ${setClause} WHERE ${this.escapeIdentifier(primaryKey)} = ? RETURNING *`;
			const row = this.#db.prepare(sql).get(...values, id) as
				| Record<string, unknown>
				| undefined;
			return row ?? null;
		} catch (error) {
			this.#handleError(error);
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
