/**
 * mysql2 adapter for @b9g/zealot
 *
 * Creates a DatabaseDriver from mysql2 connection pool.
 * Uses connection pooling - call close() when done to end all connections.
 *
 * Requires: mysql2
 */

import type {DatabaseAdapter, DatabaseDriver} from "./database.js";
import type {SQLDialect} from "./query.js";
import mysql from "mysql2/promise";

export type {DatabaseAdapter};

/**
 * SQL dialect for this adapter.
 */
export const dialect: SQLDialect = "mysql";

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
 * Create a DatabaseDriver from a mysql2 connection pool.
 *
 * @param url - MySQL connection URL
 * @param options - Connection pool options
 * @returns DatabaseAdapter with driver and close function
 *
 * @example
 * import { createDriver, dialect } from "@b9g/zealot/mysql";
 * import { Database } from "@b9g/zealot";
 *
 * const { driver, close } = createDriver("mysql://localhost/mydb");
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
	options: MySQLOptions = {},
): DatabaseAdapter {
	const pool = mysql.createPool({
		uri: url,
		connectionLimit: options.connectionLimit ?? 10,
		idleTimeout: options.idleTimeout ?? 60000,
		connectTimeout: options.connectTimeout ?? 10000,
	});

	const driver: DatabaseDriver = {
		async all<T>(sql: string, params: unknown[]): Promise<T[]> {
			const [rows] = await pool.execute(sql, params);
			return rows as T[];
		},

		async get<T>(sql: string, params: unknown[]): Promise<T | null> {
			const [rows] = await pool.execute(sql, params);
			return ((rows as unknown[])[0] as T) ?? null;
		},

		async run(sql: string, params: unknown[]): Promise<number> {
			const [result] = await pool.execute(sql, params);
			return (result as mysql.ResultSetHeader).affectedRows ?? 0;
		},

		async val<T>(sql: string, params: unknown[]): Promise<T> {
			const [rows] = await pool.execute(sql, params);
			const row = (rows as unknown[])[0];
			if (!row) return null as T;
			const values = Object.values(row as object);
			return values[0] as T;
		},

		escapeIdentifier(name: string): string {
			// MySQL: wrap in backticks, double any embedded backticks
			return `\`${name.replace(/`/g, "``")}\``;
		},

		async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
			// Use MySQL named lock for migrations
			// Timeout of 10 seconds to acquire lock
			const LOCK_NAME = "zealot_migration";
			const LOCK_TIMEOUT = 10;

			const [lockResult] = await pool.execute(`SELECT GET_LOCK(?, ?)`, [
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
				await pool.execute(`SELECT RELEASE_LOCK(?)`, [LOCK_NAME]);
			}
		},
	};

	const close = async (): Promise<void> => {
		await pool.end();
	};

	return {driver, close};
}
