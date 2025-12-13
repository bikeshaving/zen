/**
 * postgres.js adapter for @b9g/zealot
 *
 * Provides a Driver implementation for postgres.js.
 * Uses connection pooling - call close() when done to end all connections.
 *
 * Requires: postgres
 */

import type {Driver} from "./zealot.js";
import postgres from "postgres";

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
 * import PostgresDriver from "@b9g/zealot/postgres";
 * import {Database} from "@b9g/zealot";
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
	readonly dialect = "postgresql" as const;
	#sql: ReturnType<typeof postgres>;

	constructor(url: string, options: PostgresOptions = {}) {
		this.#sql = postgres(url, {
			max: options.max ?? 10,
			idle_timeout: options.idleTimeout ?? 30,
			connect_timeout: options.connectTimeout ?? 30,
		});
	}

	async all<T>(query: string, params: unknown[]): Promise<T[]> {
		const result = await this.#sql.unsafe<T[]>(query, params as any[]);
		return result;
	}

	async get<T>(query: string, params: unknown[]): Promise<T | null> {
		const result = await this.#sql.unsafe<T[]>(query, params as any[]);
		return result[0] ?? null;
	}

	async run(query: string, params: unknown[]): Promise<number> {
		const result = await this.#sql.unsafe(query, params as any[]);
		return result.count;
	}

	async val<T>(query: string, params: unknown[]): Promise<T> {
		const result = await this.#sql.unsafe(query, params as any[]);
		const row = result[0];
		if (!row) return null as T;
		const values = Object.values(row as object);
		return values[0] as T;
	}

	escapeIdentifier(name: string): string {
		// PostgreSQL: wrap in double quotes, double any embedded quotes
		return `"${name.replace(/"/g, '""')}"`;
	}

	async close(): Promise<void> {
		await this.#sql.end();
	}

	async transaction<T>(fn: () => Promise<T>): Promise<T> {
		// postgres.js has native transaction support with sql.begin()
		const result = await this.#sql.begin(async (sql) => {
			// Temporarily replace #sql with transaction-bound sql
			const originalSql = this.#sql;
			this.#sql = sql as any;
			try {
				return await fn();
			} finally {
				this.#sql = originalSql;
			}
		});
		return result as T;
	}

	async insert(
		tableName: string,
		data: Record<string, unknown>,
	): Promise<Record<string, unknown>> {
		const columns = Object.keys(data);
		const values = Object.values(data);
		const columnList = columns.map((c) => this.escapeIdentifier(c)).join(", ");
		const placeholders = columns.map((_, i) => `$${i + 1}`).join(", ");

		// PostgreSQL supports RETURNING
		const sql = `INSERT INTO ${this.escapeIdentifier(tableName)} (${columnList}) VALUES (${placeholders}) RETURNING *`;
		const result = await this.#sql.unsafe(sql, values as any[]);
		return result[0] as Record<string, unknown>;
	}

	async update(
		tableName: string,
		primaryKey: string,
		id: unknown,
		data: Record<string, unknown>,
	): Promise<Record<string, unknown> | null> {
		const columns = Object.keys(data);
		const values = Object.values(data);
		const setClause = columns
			.map((c, i) => `${this.escapeIdentifier(c)} = $${i + 1}`)
			.join(", ");

		// PostgreSQL supports RETURNING
		const sql = `UPDATE ${this.escapeIdentifier(tableName)} SET ${setClause} WHERE ${this.escapeIdentifier(primaryKey)} = $${columns.length + 1} RETURNING *`;
		const result = await this.#sql.unsafe(sql, [...values, id] as any[]);
		return result[0] ?? null;
	}

	async withMigrationLock<T>(fn: () => Promise<T>): Promise<T> {
		// Use PostgreSQL advisory lock with a fixed lock ID for migrations
		// 1952393421 = crc32("zealot_migration") for uniqueness
		const MIGRATION_LOCK_ID = 1952393421;

		await this.#sql`SELECT pg_advisory_lock(${MIGRATION_LOCK_ID})`;
		try {
			return await fn();
		} finally {
			await this.#sql`SELECT pg_advisory_unlock(${MIGRATION_LOCK_ID})`;
		}
	}
}
