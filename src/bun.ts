/**
 * Bun.SQL adapter for @b9g/zealot
 *
 * Unified driver supporting PostgreSQL, MySQL, and SQLite via Bun's built-in SQL.
 * Zero dependencies - uses native Bun implementation.
 */

import {SQL} from "bun";
import type {DatabaseAdapter, DatabaseDriver} from "./database.js";
import type {SQLDialect} from "./query.js";

export type {DatabaseAdapter};

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
 * Create a DatabaseDriver from a Bun.SQL connection.
 *
 * @param url - Database URL:
 *   - PostgreSQL: "postgres://user:pass@localhost:5432/db"
 *   - MySQL: "mysql://user:pass@localhost:3306/db"
 *   - SQLite: "sqlite://path.db", ":memory:", or "file:path.db"
 * @param options - Additional SQL options
 * @returns DatabaseAdapter with driver, close function, and detected dialect
 *
 * @example
 * import { createDriver } from "@b9g/zealot/bun";
 * import { Database } from "@b9g/zealot";
 *
 * const { driver, close, dialect } = createDriver("postgres://localhost/mydb");
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
export function createDriver(
	url: string,
	options?: Record<string, unknown>,
): DatabaseAdapter & {dialect: SQLDialect} {
	const dialect = detectDialect(url);
	const sql = new SQL(url, options as any);

	const driver: DatabaseDriver = {
		async all<T>(query: string, params: unknown[]): Promise<T[]> {
			const result = await sql.unsafe(query, params as any[]);
			return result as T[];
		},

		async get<T>(query: string, params: unknown[]): Promise<T | null> {
			const result = await sql.unsafe(query, params as any[]);
			return (result[0] as T) ?? null;
		},

		async run(query: string, params: unknown[]): Promise<number> {
			const result = await sql.unsafe(query, params as any[]);
			// Bun.SQL: .count for postgres/sqlite, .affectedRows for mysql
			return (
				(result as any).count ?? (result as any).affectedRows ?? result.length
			);
		},

		async val<T>(query: string, params: unknown[]): Promise<T> {
			const result = await sql.unsafe(query, params as any[]);
			if (result.length === 0) return null as T;
			const row = result[0] as Record<string, unknown>;
			const firstKey = Object.keys(row)[0];
			return row[firstKey] as T;
		},

		escapeIdentifier(name: string): string {
			if (dialect === "mysql") {
				// MySQL: backticks, doubled to escape
				return `\`${name.replace(/`/g, "``")}\``;
			}
			// PostgreSQL and SQLite: double quotes, doubled to escape
			return `"${name.replace(/"/g, '""')}"`;
		},

		async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
			if (dialect === "postgresql") {
				// PostgreSQL: advisory lock
				const MIGRATION_LOCK_ID = 1952393421;
				await sql.unsafe(`SELECT pg_advisory_lock($1)`, [MIGRATION_LOCK_ID]);
				try {
					return await fn();
				} finally {
					await sql.unsafe(`SELECT pg_advisory_unlock($1)`, [
						MIGRATION_LOCK_ID,
					]);
				}
			} else if (dialect === "mysql") {
				// MySQL: named lock
				const LOCK_NAME = "zealot_migration";
				const LOCK_TIMEOUT = 10;
				const result = await sql.unsafe(`SELECT GET_LOCK(?, ?)`, [
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
					await sql.unsafe(`SELECT RELEASE_LOCK(?)`, [LOCK_NAME]);
				}
			} else {
				// SQLite: exclusive transaction
				await sql.unsafe("BEGIN EXCLUSIVE", []);
				try {
					const result = await fn();
					await sql.unsafe("COMMIT", []);
					return result;
				} catch (error) {
					await sql.unsafe("ROLLBACK", []);
					throw error;
				}
			}
		},
	};

	const close = async (): Promise<void> => {
		await sql.close();
	};

	return {driver, close, dialect};
}
