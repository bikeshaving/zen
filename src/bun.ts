/**
 * Bun.SQL adapter for @b9g/zealot
 *
 * Unified driver supporting PostgreSQL, MySQL, and SQLite via Bun's built-in SQL.
 * Zero dependencies - uses native Bun implementation.
 */

import {SQL} from "bun";
import type {Driver, SQLDialect} from "./zealot.js";

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

	async all<T>(query: string, params: unknown[]): Promise<T[]> {
		const result = await this.#sql.unsafe(query, params as any[]);
		return result as T[];
	}

	async get<T>(query: string, params: unknown[]): Promise<T | null> {
		const result = await this.#sql.unsafe(query, params as any[]);
		return (result[0] as T) ?? null;
	}

	async run(query: string, params: unknown[]): Promise<number> {
		const result = await this.#sql.unsafe(query, params as any[]);
		// Bun.SQL: .count for postgres/sqlite, .affectedRows for mysql
		return (
			(result as any).count ?? (result as any).affectedRows ?? result.length
		);
	}

	async val<T>(query: string, params: unknown[]): Promise<T> {
		const result = await this.#sql.unsafe(query, params as any[]);
		if (result.length === 0) return null as T;
		const row = result[0] as Record<string, unknown>;
		const firstKey = Object.keys(row)[0];
		return row[firstKey] as T;
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
	}

	async update(
		tableName: string,
		primaryKey: string,
		id: unknown,
		data: Record<string, unknown>,
	): Promise<Record<string, unknown> | null> {
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
