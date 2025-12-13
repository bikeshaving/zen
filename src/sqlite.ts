/**
 * better-sqlite3 adapter for @b9g/zealot
 *
 * Creates a DatabaseDriver from better-sqlite3 (Node.js).
 * The connection is persistent - call close() when done.
 *
 * Requires: better-sqlite3
 */

import type {DatabaseAdapter, DatabaseDriver} from "./database.js";
import type {SQLDialect} from "./query.js";
import Database from "better-sqlite3";

export type {DatabaseAdapter};

/**
 * SQL dialect for this adapter.
 */
export const dialect: SQLDialect = "sqlite";

/**
 * Create a DatabaseDriver from a better-sqlite3 connection.
 *
 * @param url - Database URL (e.g., "file:data/app.db" or ":memory:")
 * @returns DatabaseAdapter with driver and close function
 *
 * @example
 * import { createDriver, dialect } from "@b9g/zealot/sqlite";
 * import { Database } from "@b9g/zealot";
 *
 * const { driver, close } = createDriver("file:app.db");
 * const db = new Database(driver, { dialect });
 *
 * db.addEventListener("upgradeneeded", (e) => {
 *   e.waitUntil(runMigrations(e));
 * });
 *
 * await db.open(1);
 *
 * // When done:
 * await close();
 */
export function createDriver(url: string): DatabaseAdapter {
	// Handle file: prefix
	const path = url.startsWith("file:") ? url.slice(5) : url;
	const sqlite = new Database(path);

	// Enable WAL mode for better concurrency
	sqlite.pragma("journal_mode = WAL");

	const driver: DatabaseDriver = {
		async all<T>(sql: string, params: unknown[]): Promise<T[]> {
			return sqlite.prepare(sql).all(...params) as T[];
		},

		async get<T>(sql: string, params: unknown[]): Promise<T | null> {
			return (sqlite.prepare(sql).get(...params) as T) ?? null;
		},

		async run(sql: string, params: unknown[]): Promise<number> {
			const result = sqlite.prepare(sql).run(...params);
			return result.changes;
		},

		async val<T>(sql: string, params: unknown[]): Promise<T> {
			return sqlite
				.prepare(sql)
				.pluck()
				.get(...params) as T;
		},

		escapeIdentifier(name: string): string {
			// SQLite: wrap in double quotes, double any embedded quotes
			return `"${name.replace(/"/g, '""')}"`;
		},

		async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
			// SQLite: BEGIN EXCLUSIVE acquires database-level write lock
			// This prevents all other connections from reading or writing
			sqlite.exec("BEGIN EXCLUSIVE");
			try {
				const result = await fn();
				sqlite.exec("COMMIT");
				return result;
			} catch (error) {
				sqlite.exec("ROLLBACK");
				throw error;
			}
		},
	};

	const close = async (): Promise<void> => {
		sqlite.close();
	};

	return {driver, close};
}
