/**
 * Database wrapper - the main API for schema-driven SQL.
 *
 * Provides typed queries with entity normalization and reference resolution.
 * Extends EventTarget for IndexedDB-style migration events.
 */

import type {Table, Infer, Insert} from "./table.js";
import {validateWithStandardSchema, getDBMeta} from "./table.js";
import {z} from "zod";
import {
	createQuery,
	parseTemplate,
	normalize,
	normalizeOne,
	type SQLDialect,
} from "./query.js";

// ============================================================================
// Encoding/Decoding
// ============================================================================

/**
 * Encode data for database insert/update operations.
 * Converts app values → DB values using .db.encode() functions.
 * Automatically encodes objects/arrays as JSON unless custom encoding is specified.
 */
function encodeData<T extends Table<any>>(
	table: T,
	data: Record<string, unknown>,
): Record<string, unknown> {
	const encoded: Record<string, unknown> = {};
	const shape = table.schema._def.schema?.shape ?? table.schema.shape;

	for (const [key, value] of Object.entries(data)) {
		const fieldSchema = shape?.[key];
		if (!fieldSchema) {
			encoded[key] = value;
			continue;
		}

		const dbMeta = getDBMeta(fieldSchema);
		if (dbMeta.encode && typeof dbMeta.encode === "function") {
			// Custom encoding specified - use it
			encoded[key] = dbMeta.encode(value);
		} else {
			// Check if field is an object or array type - auto-encode as JSON
			let core = fieldSchema;
			while (typeof (core as any).unwrap === "function") {
				// Stop unwrapping if we hit an array or object (they have unwrap() but it returns the element/shape)
				if (core instanceof z.ZodArray || core instanceof z.ZodObject) {
					break;
				}
				core = (core as any).unwrap();
			}

			if ((core instanceof z.ZodObject || core instanceof z.ZodArray) && value !== null && value !== undefined) {
				// Automatic JSON encoding for objects and arrays
				encoded[key] = JSON.stringify(value);
			} else {
				encoded[key] = value;
			}
		}
	}

	return encoded;
}

/**
 * Decode data from database read operations.
 * Converts DB values → app values using .db.decode() functions.
 * Automatically decodes JSON strings to objects/arrays unless custom decoding is specified.
 */
function decodeData<T extends Table<any>>(
	table: T,
	data: Record<string, unknown> | null,
): Record<string, unknown> | null {
	if (!data) return data;

	const decoded: Record<string, unknown> = {};
	const shape = table.schema._def.schema?.shape ?? table.schema.shape;

	for (const [key, value] of Object.entries(data)) {
		const fieldSchema = shape?.[key];
		if (!fieldSchema) {
			decoded[key] = value;
			continue;
		}

		const dbMeta = getDBMeta(fieldSchema);
		if (dbMeta.decode && typeof dbMeta.decode === "function") {
			// Custom decoding specified - use it
			decoded[key] = dbMeta.decode(value);
		} else {
			// Check if field is an object or array type - auto-decode from JSON
			let core = fieldSchema;
			while (typeof (core as any).unwrap === "function") {
				// Stop unwrapping if we hit an array or object (they have unwrap() but it returns the element/shape)
				if (core instanceof z.ZodArray || core instanceof z.ZodObject) {
					break;
				}
				core = (core as any).unwrap();
			}

			if (core instanceof z.ZodObject || core instanceof z.ZodArray) {
				// Automatic JSON decoding for objects and arrays
				if (typeof value === "string") {
					try {
						decoded[key] = JSON.parse(value);
					} catch {
						// If parse fails, keep as-is
						decoded[key] = value;
					}
				} else {
					// Already an object (e.g., from PostgreSQL JSONB)
					decoded[key] = value;
				}
			} else {
				decoded[key] = value;
			}
		}
	}

	return decoded;
}

// ============================================================================
// Driver Interface
// ============================================================================

/**
 * Database driver interface.
 *
 * Each driver implements this interface as a class with a specific name
 * (e.g., SQLiteDriver, PostgresDriver) and is exported as the default export.
 *
 * Drivers own all SQL generation and dialect-specific behavior.
 */
export interface Driver {
	/**
	 * SQL dialect for this driver.
	 */
	readonly dialect: SQLDialect;

	/**
	 * Execute a query and return all rows.
	 */
	all<T = Record<string, unknown>>(
		sql: string,
		params: unknown[],
	): Promise<T[]>;

	/**
	 * Execute a query and return the first row.
	 */
	get<T = Record<string, unknown>>(
		sql: string,
		params: unknown[],
	): Promise<T | null>;

	/**
	 * Execute a statement and return the number of affected rows.
	 */
	run(sql: string, params: unknown[]): Promise<number>;

	/**
	 * Execute a query and return a single value.
	 */
	val<T = unknown>(sql: string, params: unknown[]): Promise<T>;

	/**
	 * Escape an identifier (table name, column name) for safe SQL interpolation.
	 */
	escapeIdentifier(name: string): string;

	/**
	 * Close the database connection.
	 */
	close(): Promise<void>;

	/**
	 * Execute a function within a database transaction.
	 *
	 * Drivers implement this using their native transaction API to ensure
	 * all operations use the same connection.
	 *
	 * If the function completes successfully, the transaction is committed.
	 * If the function throws an error, the transaction is rolled back.
	 */
	transaction<T>(fn: () => Promise<T>): Promise<T>;

	/**
	 * Insert a row and return the full row (including DB defaults/generated values).
	 *
	 * @param tableName - The table name
	 * @param data - Validated column-value pairs to insert
	 * @returns The inserted row with all columns (including defaults)
	 */
	insert(
		tableName: string,
		data: Record<string, unknown>,
	): Promise<Record<string, unknown>>;

	/**
	 * Update a row and return the updated row.
	 *
	 * @param tableName - The table name
	 * @param primaryKey - The primary key column name
	 * @param id - The primary key value
	 * @param data - Validated column-value pairs to update
	 * @returns The updated row, or null if not found
	 */
	update(
		tableName: string,
		primaryKey: string,
		id: unknown,
		data: Record<string, unknown>,
	): Promise<Record<string, unknown> | null>;

	/**
	 * Execute a function while holding an exclusive migration lock.
	 *
	 * Optional — implement this using dialect-appropriate locking:
	 * - PostgreSQL: pg_advisory_lock
	 * - MySQL: GET_LOCK
	 * - SQLite: BEGIN EXCLUSIVE
	 */
	withMigrationLock?<T>(fn: () => Promise<T>): Promise<T>;

	/**
	 * Run EXPLAIN on a query to see the database's query plan.
	 *
	 * Optional — implement using dialect-appropriate EXPLAIN syntax:
	 * - SQLite: EXPLAIN QUERY PLAN
	 * - PostgreSQL/MySQL: EXPLAIN
	 *
	 * @param query - The SQL query to explain
	 * @param params - Query parameters
	 * @returns Array of query plan rows (structure varies by database)
	 */
	explain?(query: string, params: unknown[]): Promise<Record<string, unknown>[]>;
}

// ============================================================================
// Database Upgrade Event
// ============================================================================

/**
 * Event fired when database version increases during open().
 *
 * Similar to IndexedDB's IDBVersionChangeEvent combined with
 * ServiceWorker's ExtendableEvent (for waitUntil support).
 *
 * **Migration model**: Zealot uses monotonic, forward-only versioning:
 * - Versions are integers that only increase: 1 → 2 → 3 → ...
 * - Downgrading (e.g., 3 → 2) is NOT supported
 * - Branching version histories are NOT supported
 * - Each version should be deployed once and never modified
 *
 * **Best practices**:
 * - Use conditional checks: `if (e.oldVersion < 2) { ... }`
 * - Prefer additive changes (new columns, indexes) over destructive ones
 * - Never modify past migrations - add new versions instead
 * - Keep migrations idempotent when possible (use ensureColumn, ensureIndex)
 */
export class DatabaseUpgradeEvent extends Event {
	readonly oldVersion: number;
	readonly newVersion: number;
	#promises: Promise<void>[] = [];

	constructor(type: string, init: {oldVersion: number; newVersion: number}) {
		super(type);
		this.oldVersion = init.oldVersion;
		this.newVersion = init.newVersion;
	}

	/**
	 * Extend the event lifetime until the promise settles.
	 * Like ExtendableEvent.waitUntil() from ServiceWorker.
	 */
	waitUntil(promise: Promise<void>): void {
		this.#promises.push(promise);
	}

	/**
	 * @internal Wait for all waitUntil promises to settle.
	 */
	async _settle(): Promise<void> {
		await Promise.all(this.#promises);
	}
}

// ============================================================================
// Transaction
// ============================================================================

/**
 * Tagged template query function that returns normalized entities.
 */
export type TaggedQuery<T> = (
	strings: TemplateStringsArray,
	...values: unknown[]
) => Promise<T>;

/**
 * Transaction context with query methods.
 *
 * Provides the same query interface as Database, but bound to a single
 * connection for the duration of the transaction.
 */
export class Transaction {
	#driver: Driver;

	constructor(driver: Driver) {
		this.#driver = driver;
	}

	// ==========================================================================
	// Queries - Return Normalized Entities
	// ==========================================================================

	all<T extends Table<any>>(tables: T | T[]): TaggedQuery<Infer<T>[]> {
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const query = createQuery(
				tableArray as Table<any>[],
				this.#driver.dialect,
			);
			const {sql, params} = query(strings, ...values);
			const rows = await this.#driver.all<Record<string, unknown>>(sql, params);
			return normalize<Infer<T>>(rows, tableArray as Table<any>[]);
		};
	}

	get<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<Infer<T> | null>;
	get<T extends Table<any>>(tables: T | T[]): TaggedQuery<Infer<T> | null>;
	get<T extends Table<any>>(
		tables: T | T[],
		id?: string | number,
	): Promise<Infer<T> | null> | TaggedQuery<Infer<T> | null> {
		// Convenience overload: get by primary key
		if (id !== undefined) {
			const table = tables as T;
			const pk = table._meta.primary;
			if (!pk) {
				return Promise.reject(
					new Error(`Table ${table.name} has no primary key defined`),
				);
			}
			const tableName = this.#quoteIdent(table.name);
			const whereClause = `${this.#quoteIdent(pk)} = ${this.#placeholder(1)}`;
			return this.#driver
				.get<
					Record<string, unknown>
				>(`SELECT * FROM ${tableName} WHERE ${whereClause}`, [id])
				.then((row) =>
					row ? (validateWithStandardSchema<Infer<T>>(table.schema, row) as Infer<T>) : null,
				);
		}

		// Tagged template query
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const query = createQuery(
				tableArray as Table<any>[],
				this.#driver.dialect,
			);
			const {sql, params} = query(strings, ...values);
			const row = await this.#driver.get<Record<string, unknown>>(sql, params);
			return normalizeOne<Infer<T>>(row, tableArray as Table<any>[]);
		};
	}

	// ==========================================================================
	// Mutations - Validate Through Zod
	// ==========================================================================

	async insert<T extends Table<any>>(
		table: T,
		data: Insert<T>,
	): Promise<Infer<T>> {
		if (table._meta.isPartial) {
			throw new Error(
				`Cannot insert into partial table "${table.name}". Use the full table definition instead.`,
			);
		}

		const validated = validateWithStandardSchema<Record<string, unknown>>(
			table.schema,
			data,
		);
		const encoded = encodeData(table, validated);
		const row = await this.#driver.insert(table.name, encoded);
		const decoded = decodeData(table, row);
		return validateWithStandardSchema<Infer<T>>(table.schema, decoded) as Infer<T>;
	}

	async update<T extends Table<any>>(
		table: T,
		id: string | number | Record<string, unknown>,
		data: Partial<Insert<T>>,
	): Promise<Infer<T> | null> {
		const pk = table._meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const partialSchema = table.schema.partial();
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			partialSchema,
			data,
		);

		const columns = Object.keys(validated);
		if (columns.length === 0) {
			throw new Error("No fields to update");
		}

		const encoded = encodeData(table, validated);
		const row = await this.#driver.update(table.name, pk, id, encoded);
		if (!row) return null;
		const decoded = decodeData(table, row);
		return validateWithStandardSchema<Infer<T>>(table.schema, decoded) as Infer<T>;
	}

	async delete<T extends Table<any>>(
		table: T,
		id: string | number | Record<string, unknown>,
	): Promise<boolean> {
		const pk = table._meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const tableName = this.#quoteIdent(table.name);
		const whereClause = `${this.#quoteIdent(pk)} = ${this.#placeholder(1)}`;

		const sql = `DELETE FROM ${tableName} WHERE ${whereClause}`;
		const affected = await this.#driver.run(sql, [id]);

		return affected > 0;
	}

	async softDelete<T extends Table<any>>(
		table: T,
		id: string | number | Record<string, unknown>,
	): Promise<boolean> {
		const pk = table._meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const softDeleteField = table._meta.softDeleteField;
		if (!softDeleteField) {
			throw new Error(
				`Table ${table.name} does not have a soft delete field. Use softDelete() wrapper to mark a field.`,
			);
		}

		const tableName = this.#quoteIdent(table.name);
		const whereClause = `${this.#quoteIdent(pk)} = ${this.#placeholder(1)}`;
		const setClause = `${this.#quoteIdent(softDeleteField)} = ${this.#placeholder(2)}`;

		const sql = `UPDATE ${tableName} SET ${setClause} WHERE ${whereClause}`;
		const affected = await this.#driver.run(sql, [id, new Date()]);

		return affected > 0;
	}

	// ==========================================================================
	// Raw - No Normalization
	// ==========================================================================

	async query<T = Record<string, unknown>>(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<T[]> {
		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return this.#driver.all<T>(sql, params);
	}

	async exec(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<number> {
		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return this.#driver.run(sql, params);
	}

	async val<T>(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<T> {
		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return this.#driver.val<T>(sql, params);
	}

	// ==========================================================================
	// Debugging
	// ==========================================================================

	/**
	 * Print the generated SQL and parameters without executing.
	 * Useful for debugging query composition and fragment expansion.
	 */
	print(
		strings: TemplateStringsArray,
		...values: unknown[]
	): {sql: string; params: unknown[]} {
		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return {sql, params};
	}

	/**
	 * Run EXPLAIN on a query to see the database's query plan.
	 * Helps optimize queries by showing index usage, join order, etc.
	 *
	 * @throws Error if driver doesn't implement explain()
	 */
	async explain(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<Record<string, unknown>[]> {
		if (!this.#driver.explain) {
			throw new Error(`Driver ${this.#driver.dialect} does not support EXPLAIN`);
		}

		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return this.#driver.explain(sql, params);
	}

	// ==========================================================================
	// Helpers
	// ==========================================================================

	#quoteIdent(name: string): string {
		return this.#driver.escapeIdentifier(name);
	}

	#placeholder(index: number): string {
		if (this.#driver.dialect === "postgresql") {
			return `$${index}`;
		}
		return "?";
	}
}

// ============================================================================
// Database
// ============================================================================

/**
 * Database wrapper with typed queries and entity normalization.
 * Extends EventTarget for IndexedDB-style "upgradeneeded" events.
 *
 * @example
 * const db = new Database(driver);
 *
 * db.addEventListener("upgradeneeded", (e) => {
 *   e.waitUntil(runMigrations(e));
 * });
 *
 * await db.open(2);
 */
export class Database extends EventTarget {
	#driver: Driver;
	#version: number = 0;
	#opened: boolean = false;

	constructor(driver: Driver) {
		super();
		this.#driver = driver;
	}

	/**
	 * Current database schema version.
	 * Returns 0 if database has never been opened.
	 */
	get version(): number {
		return this.#version;
	}

	/**
	 * Open the database at a specific version.
	 *
	 * If the requested version is higher than the current version,
	 * fires an "upgradeneeded" event and waits for all waitUntil()
	 * promises before completing.
	 *
	 * Migration safety: Uses exclusive locking to prevent race conditions
	 * when multiple processes attempt migrations simultaneously.
	 *
	 * @example
	 * db.addEventListener("upgradeneeded", (e) => {
	 *   e.waitUntil(runMigrations(e));
	 * });
	 * await db.open(2);
	 */
	async open(version: number): Promise<void> {
		if (this.#opened) {
			throw new Error("Database already opened");
		}

		// Create table outside lock (idempotent)
		await this.#ensureMigrationsTable();

		// Run migration logic inside lock
		const runMigration = async (): Promise<void> => {
			const currentVersion = await this.#getCurrentVersionLocked();

			if (version > currentVersion) {
				const event = new DatabaseUpgradeEvent("upgradeneeded", {
					oldVersion: currentVersion,
					newVersion: version,
				});
				this.dispatchEvent(event);
				await event._settle();

				await this.#setVersion(version);
			}
		};

		// Use driver's migration lock if available, otherwise fall back to SQL-based locking
		if (this.#driver.withMigrationLock) {
			await this.#driver.withMigrationLock(runMigration);
		} else {
			// Fallback: SQL-based transaction locking
			// SQLite: BEGIN IMMEDIATE acquires write lock upfront
			// PostgreSQL/MySQL: relies on SELECT FOR UPDATE in #getCurrentVersionLocked
			const beginSQL =
				this.#driver.dialect === "sqlite"
					? "BEGIN IMMEDIATE"
					: this.#driver.dialect === "mysql"
						? "START TRANSACTION"
						: "BEGIN";

			await this.#driver.run(beginSQL, []);

			try {
				await runMigration();
				await this.#driver.run("COMMIT", []);
			} catch (error) {
				await this.#driver.run("ROLLBACK", []);
				throw error;
			}
		}

		this.#version = version;
		this.#opened = true;
	}

	// ==========================================================================
	// Migration Table Helpers
	// ==========================================================================

	async #ensureMigrationsTable(): Promise<void> {
		const timestampCol =
			this.#driver.dialect === "mysql"
				? "applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
				: "applied_at TEXT DEFAULT CURRENT_TIMESTAMP";

		await this.#driver.run(
			`CREATE TABLE IF NOT EXISTS _migrations (
				version INTEGER PRIMARY KEY,
				${timestampCol}
			)`,
			[],
		);
	}

	async #getCurrentVersionLocked(): Promise<number> {
		// Locking is handled by withMigrationLock() (advisory locks) or
		// the transaction wrapper (BEGIN IMMEDIATE for SQLite, or transaction for others)
		const row = await this.#driver.get<{version: number}>(
			`SELECT MAX(version) as version FROM _migrations`,
			[],
		);
		return row?.version ?? 0;
	}

	async #setVersion(version: number): Promise<void> {
		await this.#driver.run(
			`INSERT INTO _migrations (version) VALUES (${this.#placeholder(1)})`,
			[version],
		);
	}

	// ==========================================================================
	// Queries - Return Normalized Entities
	// ==========================================================================

	/**
	 * Query multiple entities with joins and reference resolution.
	 *
	 * @example
	 * // Single table
	 * const posts = await db.all(Posts)`WHERE published = ${true}`;
	 *
	 * // Multi-table with joins
	 * const posts = await db.all([Posts, Users])`
	 *   JOIN users ON users.id = posts.author_id
	 *   WHERE published = ${true}
	 * `;
	 * posts[0].author.name  // "Alice"
	 */
	all<T extends Table<any>>(tables: T | T[]): TaggedQuery<Infer<T>[]> {
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const query = createQuery(
				tableArray as Table<any>[],
				this.#driver.dialect,
			);
			const {sql, params} = query(strings, ...values);
			const rows = await this.#driver.all<Record<string, unknown>>(sql, params);
			return normalize<Infer<T>>(rows, tableArray as Table<any>[]);
		};
	}

	/**
	 * Query a single entity.
	 *
	 * @example
	 * // By primary key
	 * const post = await db.get(Posts, postId);
	 *
	 * // With query
	 * const post = await db.get(Posts)`WHERE slug = ${slug}`;
	 *
	 * // Multi-table
	 * const post = await db.get([Posts, Users])`
	 *   JOIN users ON users.id = posts.author_id
	 *   WHERE posts.id = ${postId}
	 * `;
	 */
	get<T extends Table<any>>(
		table: T,
		id: string | number,
	): Promise<Infer<T> | null>;
	get<T extends Table<any>>(tables: T | T[]): TaggedQuery<Infer<T> | null>;
	get<T extends Table<any>>(
		tables: T | T[],
		id?: string | number,
	): Promise<Infer<T> | null> | TaggedQuery<Infer<T> | null> {
		// Convenience overload: get by primary key
		if (id !== undefined) {
			const table = tables as T;
			const pk = table._meta.primary;
			if (!pk) {
				return Promise.reject(
					new Error(`Table ${table.name} has no primary key defined`),
				);
			}
			const tableName = this.#quoteIdent(table.name);
			const whereClause = `${this.#quoteIdent(pk)} = ${this.#placeholder(1)}`;
			return this.#driver
				.get<
					Record<string, unknown>
				>(`SELECT * FROM ${tableName} WHERE ${whereClause}`, [id])
				.then((row) =>
					row ? (validateWithStandardSchema<Infer<T>>(table.schema, row) as Infer<T>) : null,
				);
		}

		// Tagged template query
		const tableArray = Array.isArray(tables) ? tables : [tables];
		return async (strings: TemplateStringsArray, ...values: unknown[]) => {
			const query = createQuery(
				tableArray as Table<any>[],
				this.#driver.dialect,
			);
			const {sql, params} = query(strings, ...values);
			const row = await this.#driver.get<Record<string, unknown>>(sql, params);
			return normalizeOne<Infer<T>>(row, tableArray as Table<any>[]);
		};
	}

	// ==========================================================================
	// Mutations - Validate Through Zod
	// ==========================================================================

	/**
	 * Insert a new entity.
	 *
	 * Uses RETURNING to get the actual inserted row (with DB defaults).
	 *
	 * @example
	 * const user = await db.insert(users, {
	 *   id: crypto.randomUUID(),
	 *   email: "alice@example.com",
	 *   name: "Alice",
	 * });
	 */
	async insert<T extends Table<any>>(
		table: T,
		data: Insert<T>,
	): Promise<Infer<T>> {
		if (table._meta.isPartial) {
			throw new Error(
				`Cannot insert into partial table "${table.name}". Use the full table definition instead.`,
			);
		}

		const validated = validateWithStandardSchema<Record<string, unknown>>(
			table.schema,
			data,
		);
		const encoded = encodeData(table, validated);

		const columns = Object.keys(encoded);
		const values = Object.values(encoded);
		const tableName = this.#quoteIdent(table.name);
		const columnList = columns.map((c) => this.#quoteIdent(c)).join(", ");
		const placeholders = columns
			.map((_, i) => this.#placeholder(i + 1))
			.join(", ");

		// Use RETURNING for SQLite/PostgreSQL to get actual row (with DB defaults)
		if (this.#driver.dialect !== "mysql") {
			const sql = `INSERT INTO ${tableName} (${columnList}) VALUES (${placeholders}) RETURNING *`;
			const row = await this.#driver.get<Record<string, unknown>>(sql, values);
			const decoded = decodeData(table, row);
			return validateWithStandardSchema<Infer<T>>(table.schema, decoded) as Infer<T>;
		}

		// MySQL fallback: INSERT then SELECT
		const sql = `INSERT INTO ${tableName} (${columnList}) VALUES (${placeholders})`;
		await this.#driver.run(sql, values);
		return validated as Infer<T>;
	}

	/**
	 * Update an entity by primary key.
	 *
	 * Uses RETURNING to get the updated row in a single query.
	 *
	 * @example
	 * const user = await db.update(users, userId, { name: "Bob" });
	 */
	async update<T extends Table<any>>(
		table: T,
		id: string | number | Record<string, unknown>,
		data: Partial<Insert<T>>,
	): Promise<Infer<T> | null> {
		const pk = table._meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const partialSchema = table.schema.partial();
		const validated = validateWithStandardSchema<Record<string, unknown>>(
			partialSchema,
			data,
		);

		const columns = Object.keys(validated);
		if (columns.length === 0) {
			throw new Error("No fields to update");
		}

		const encoded = encodeData(table, validated);
		const values = Object.values(encoded);
		const tableName = this.#quoteIdent(table.name);
		const setClause = columns
			.map((c, i) => `${this.#quoteIdent(c)} = ${this.#placeholder(i + 1)}`)
			.join(", ");

		const whereClause = `${this.#quoteIdent(pk)} = ${this.#placeholder(values.length + 1)}`;

		// Use RETURNING for SQLite/PostgreSQL
		if (this.#driver.dialect !== "mysql") {
			const sql = `UPDATE ${tableName} SET ${setClause} WHERE ${whereClause} RETURNING *`;
			const row = await this.#driver.get<Record<string, unknown>>(sql, [
				...values,
				id,
			]);
			if (!row) return null;
			const decoded = decodeData(table, row);
			return validateWithStandardSchema<Infer<T>>(table.schema, decoded) as Infer<T>;
		}

		// MySQL fallback: UPDATE then SELECT
		const sql = `UPDATE ${tableName} SET ${setClause} WHERE ${whereClause}`;
		await this.#driver.run(sql, [...values, id]);

		const selectSql = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
		const row = await this.#driver.get<Record<string, unknown>>(selectSql, [
			id,
		]);
		if (!row) return null;
		const decoded = decodeData(table, row);
		return validateWithStandardSchema<Infer<T>>(table.schema, decoded) as Infer<T>;
	}

	/**
	 * Delete an entity by primary key.
	 *
	 * @example
	 * const deleted = await db.delete(users, userId);
	 */
	async delete<T extends Table<any>>(
		table: T,
		id: string | number | Record<string, unknown>,
	): Promise<boolean> {
		const pk = table._meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const tableName = this.#quoteIdent(table.name);
		const whereClause = `${this.#quoteIdent(pk)} = ${this.#placeholder(1)}`;

		const sql = `DELETE FROM ${tableName} WHERE ${whereClause}`;
		const affected = await this.#driver.run(sql, [id]);

		return affected > 0;
	}

	/**
	 * Soft delete by marking the soft delete field (e.g., deletedAt) with the current timestamp.
	 *
	 * @example
	 * const deleted = await db.softDelete(Users, userId);
	 */
	async softDelete<T extends Table<any>>(
		table: T,
		id: string | number | Record<string, unknown>,
	): Promise<boolean> {
		const pk = table._meta.primary;
		if (!pk) {
			throw new Error(`Table ${table.name} has no primary key defined`);
		}

		const softDeleteField = table._meta.softDeleteField;
		if (!softDeleteField) {
			throw new Error(
				`Table ${table.name} does not have a soft delete field. Use softDelete() wrapper to mark a field.`,
			);
		}

		const tableName = this.#quoteIdent(table.name);
		const whereClause = `${this.#quoteIdent(pk)} = ${this.#placeholder(1)}`;
		const setClause = `${this.#quoteIdent(softDeleteField)} = ${this.#placeholder(2)}`;

		const sql = `UPDATE ${tableName} SET ${setClause} WHERE ${whereClause}`;
		const affected = await this.#driver.run(sql, [id, new Date()]);

		return affected > 0;
	}

	// ==========================================================================
	// Raw - No Normalization
	// ==========================================================================

	/**
	 * Execute a raw query and return rows.
	 *
	 * @example
	 * const counts = await db.query<{ count: number }>`
	 *   SELECT COUNT(*) as count FROM posts WHERE author_id = ${userId}
	 * `;
	 */
	async query<T = Record<string, unknown>>(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<T[]> {
		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return this.#driver.all<T>(sql, params);
	}

	/**
	 * Execute a statement (INSERT, UPDATE, DELETE, DDL).
	 *
	 * @example
	 * await db.exec`CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY)`;
	 */
	async exec(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<number> {
		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return this.#driver.run(sql, params);
	}

	/**
	 * Execute a query and return a single value.
	 *
	 * @example
	 * const count = await db.val<number>`SELECT COUNT(*) FROM posts`;
	 */
	async val<T>(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<T> {
		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return this.#driver.val<T>(sql, params);
	}

	// ==========================================================================
	// Debugging
	// ==========================================================================

	/**
	 * Print the generated SQL and parameters without executing.
	 * Useful for debugging query composition and fragment expansion.
	 *
	 * @returns Object with sql string and params array
	 *
	 * @example
	 * const query = db.print`
	 *   SELECT * FROM ${Posts}
	 *   WHERE ${Posts.where({ published: true })}
	 * `;
	 * console.log(query.sql);     // SELECT * FROM "posts" WHERE "posts"."published" = $1
	 * console.log(query.params);  // [true]
	 *
	 * @example
	 * // Inspect DDL generation
	 * const ddl = db.print`${Posts.ddl()}`;
	 * console.log(ddl.sql);  // CREATE TABLE IF NOT EXISTS "posts" (...)
	 */
	print(
		strings: TemplateStringsArray,
		...values: unknown[]
	): {sql: string; params: unknown[]} {
		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return {sql, params};
	}

	/**
	 * Run EXPLAIN on a query to see the database's query plan.
	 * Helps optimize queries by showing index usage, join order, etc.
	 *
	 * **Dialect differences**:
	 * - SQLite: Uses `EXPLAIN QUERY PLAN`
	 * - PostgreSQL/MySQL: Uses `EXPLAIN`
	 *
	 * @returns Array of query plan rows (structure varies by database)
	 * @throws Error if driver doesn't implement explain()
	 *
	 * @example
	 * const plan = await db.explain`
	 *   SELECT * FROM ${Posts}
	 *   WHERE ${Posts.where({ authorId: userId })}
	 * `;
	 * console.log(plan);
	 * // SQLite: [{ detail: "SEARCH posts USING INDEX idx_posts_authorId (authorId=?)" }]
	 * // PostgreSQL: [{ "QUERY PLAN": "Index Scan using idx_posts_authorId on posts" }]
	 */
	async explain(
		strings: TemplateStringsArray,
		...values: unknown[]
	): Promise<Record<string, unknown>[]> {
		if (!this.#driver.explain) {
			throw new Error(`Driver ${this.#driver.dialect} does not support EXPLAIN`);
		}

		const {sql, params} = parseTemplate(strings, values, this.#driver.dialect);
		return this.#driver.explain(sql, params);
	}

	// ==========================================================================
	// Transactions
	// ==========================================================================

	/**
	 * Execute a function within a database transaction.
	 *
	 * If the function completes successfully, the transaction is committed.
	 * If the function throws an error, the transaction is rolled back.
	 *
	 * For connection-pooled drivers that implement `beginTransaction()`,
	 * all operations are guaranteed to use the same connection.
	 *
	 * @example
	 * await db.transaction(async (tx) => {
	 *   const user = await tx.insert(users, { id: "1", name: "Alice" });
	 *   await tx.insert(posts, { id: "1", authorId: user.id, title: "Hello" });
	 *   // If any insert fails, both are rolled back
	 * });
	 */
	async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
		// Delegate to driver.transaction() which handles dialect-specific behavior
		return await this.#driver.transaction(async () => {
			const tx = new Transaction(this.#driver);
			return await fn(tx);
		});
	}

	// ==========================================================================
	// Helpers
	// ==========================================================================

	#quoteIdent(name: string): string {
		return this.#driver.escapeIdentifier(name);
	}

	#placeholder(index: number): string {
		if (this.#driver.dialect === "postgresql") {
			return `$${index}`;
		}
		return "?";
	}
}
