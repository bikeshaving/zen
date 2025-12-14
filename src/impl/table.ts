/**
 * Table definition with wrapper-based field extensions.
 *
 * Uses wrapper functions instead of .pipe() to avoid Zod internals.
 * Metadata is extracted once at table() call time.
 */

import {z, ZodTypeAny, ZodObject, ZodRawShape} from "zod";
import {TableDefinitionError} from "./errors.js";
import {createFragment, type SQLFragment} from "./query.js";
import {ValidationError} from "./errors.js";

// ============================================================================
// Standard Schema Support
// ============================================================================

/**
 * Validate data using Standard Schema.
 * All Zod schemas (v3.23+) implement the Standard Schema interface.
 *
 * @internal Used by table methods and database operations
 */
export function validateWithStandardSchema<T = unknown>(
	schema: ZodObject<any>,
	data: unknown,
): T {
	const standard = (schema as any)["~standard"];

	// Ensure Standard Schema is available
	if (!standard?.validate) {
		throw new Error(
			"Schema does not implement Standard Schema (~standard.validate). " +
				"Ensure you're using Zod v3.23+ or another Standard Schema-compliant library.",
		);
	}

	const result = standard.validate(data);

	// Guard against async validation (Standard Schema allows it, but Zod is sync)
	if (result && typeof (result as any).then === "function") {
		throw new Error(
			"Async validation is not supported. Standard Schema validate() must be synchronous.",
		);
	}

	if (result.issues) {
		// Convert Standard Schema issues to ValidationError
		// Join full path for nested fields: ["nested", "email"] -> "nested.email"
		throw new ValidationError(
			"Validation failed",
			result.issues.reduce(
				(acc: Record<string, string[]>, issue: any) => {
					const path =
						issue.path && issue.path.length > 0
							? issue.path.map(String).join(".")
							: "_root";
					if (!acc[path]) acc[path] = [];
					acc[path].push(issue.message);
					return acc;
				},
				{} as Record<string, string[]>,
			),
		);
	}

	return result.value;
}

// ============================================================================
// Metadata Keys
// ============================================================================

/**
 * Namespace for database metadata to avoid collisions with user metadata.
 */
const DB_META_NAMESPACE = "db" as const;

/**
 * Get database metadata from a schema.
 *
 * Metadata is stored under a namespaced key to avoid collisions with user metadata.
 *
 * @param schema - Zod schema to read metadata from
 * @returns Database metadata object (empty object if none exists)
 *
 * @example
 * const emailMeta = getDBMeta(emailSchema);
 * if (emailMeta.unique) {
 *   console.log("This field has a unique constraint");
 * }
 */
export function getDBMeta(schema: ZodTypeAny): Record<string, any> {
	try {
		const meta = typeof (schema as any).meta === "function" ? (schema as any).meta() : {};
		return meta?.[DB_META_NAMESPACE] ?? {};
	} catch {
		// Fallback to empty object if .meta() fails
		return {};
	}
}

/**
 * Set database metadata on a schema, preserving both user metadata and existing DB metadata.
 *
 * **Declarative only**: Metadata is read once at `table()` construction time.
 * Mutating metadata after a table is defined will NOT affect behavior.
 *
 * **Precedence**: Later calls to setDBMeta() override earlier ones (last write wins).
 * User metadata in the root object is preserved separately from DB metadata.
 *
 * @param schema - Zod schema to attach metadata to
 * @param dbMeta - Database metadata to set (merges with existing DB metadata)
 * @returns New schema with metadata attached
 *
 * @example
 * // Define custom field wrapper
 * function hashed<T extends z.ZodString>(schema: T) {
 *   return setDBMeta(
 *     schema.transform(password => bcrypt.hashSync(password, 10)),
 *     { hashed: true }
 *   );
 * }
 *
 * // User metadata is preserved separately
 * const email = z.string()
 *   .email()
 *   .meta({ label: "Email Address" }); // User metadata
 * const uniqueEmail = setDBMeta(email, { unique: true }); // DB metadata
 *
 * // Both are accessible:
 * uniqueEmail.meta(); // { label: "Email Address", db: { unique: true } }
 */
export function setDBMeta<T extends ZodTypeAny>(
	schema: T,
	dbMeta: Record<string, any>,
): T {
	const existing = (typeof (schema as any).meta === "function" ? (schema as any).meta() : undefined) ?? {};
	return schema.meta({
		...existing,
		[DB_META_NAMESPACE]: {
			...(existing[DB_META_NAMESPACE] ?? {}),
			...dbMeta,
		},
	}) as T;
}

// ============================================================================
// Field Metadata
// ============================================================================

export interface FieldDbMeta {
	primaryKey?: boolean;
	unique?: boolean;
	indexed?: boolean;
	softDelete?: boolean;
	reference?: {
		table: Table<any>;
		field?: string; // defaults to primary key
		as: string;
		onDelete?: "cascade" | "set null" | "restrict";
	};
}

// ============================================================================
// Field Wrappers
// ============================================================================

/**
 * Mark a field as the primary key.
 *
 * @example
 * id: primary(z.string().uuid())
 */
export function primary<T extends ZodTypeAny>(schema: T): T {
	return setDBMeta(schema, {primary: true});
}

/**
 * Mark a field as unique.
 *
 * @example
 * email: unique(z.string().email())
 */
export function unique<T extends ZodTypeAny>(schema: T): T {
	return setDBMeta(schema, {unique: true});
}

/**
 * Mark a field for indexing.
 *
 * @example
 * createdAt: index(z.date())
 */
export function index<T extends ZodTypeAny>(schema: T): T {
	return setDBMeta(schema, {indexed: true});
}

/**
 * Mark a field as the soft delete timestamp.
 * Enables the Table.deleted() helper for filtering soft-deleted records.
 *
 * @example
 * deletedAt: softDelete(z.date().nullable())
 */
export function softDelete<T extends ZodTypeAny>(schema: T): T {
	return setDBMeta(schema, {softDelete: true});
}

/**
 * Define a foreign key reference.
 *
 * @example
 * authorId: references(z.string().uuid(), users, { as: "author" })
 * authorId: references(z.string().uuid(), users, { field: "id", as: "author" })
 */
export function references<T extends ZodTypeAny>(
	schema: T,
	table: Table<any>,
	options: {
		field?: string;
		as: string;
		onDelete?: "cascade" | "set null" | "restrict";
	},
): T {
	return setDBMeta(schema, {
		reference: {
			table,
			field: options.field,
			as: options.as,
			onDelete: options.onDelete,
		},
	});
}

// ============================================================================
// Field Metadata Types (for forms/admin)
// ============================================================================

export type FieldType =
	| "text"
	| "textarea"
	| "email"
	| "url"
	| "tel"
	| "password"
	| "number"
	| "integer"
	| "checkbox"
	| "select"
	| "date"
	| "datetime"
	| "time"
	| "json"
	| "hidden";

export interface FieldMeta {
	name: string;
	type: FieldType;
	required: boolean;
	primaryKey?: boolean;
	unique?: boolean;
	indexed?: boolean;
	default?: unknown;
	maxLength?: number;
	minLength?: number;
	min?: number;
	max?: number;
	options?: readonly string[];
	reference?: {
		table: string;
		field: string;
		as: string;
	};
	/** Additional user-defined metadata from Zod's .meta() (label, helpText, widget, etc.) */
	[key: string]: unknown;
}

// ============================================================================
// Table
// ============================================================================

export interface TableOptions {
	indexes?: string[][];
}

// Symbol to identify Table objects
const TABLE_MARKER = Symbol.for("@b9g/zealot:table");

// Symbol for SQL fragments (same as query.ts - Symbol.for ensures identity)
const SQL_FRAGMENT = Symbol.for("@b9g/zealot:fragment");

interface ColumnFragment {
	readonly [SQL_FRAGMENT]: true;
	readonly sql: string;
	readonly params: readonly unknown[];
}

/**
 * Check if a value is a Table object.
 */
export function isTable(value: unknown): value is Table<any> {
	return (
		value !== null &&
		typeof value === "object" &&
		TABLE_MARKER in value &&
		(value as any)[TABLE_MARKER] === true
	);
}

export interface ReferenceInfo {
	fieldName: string;
	table: Table<any>;
	referencedField: string;
	as: string;
	onDelete?: "cascade" | "set null" | "restrict";
}

// ============================================================================
// Fragment Method Types
// ============================================================================

/**
 * Condition operators for where/having clauses.
 */
export type ConditionOperators<T> = {
	$eq?: T;
	$lt?: T;
	$gt?: T;
	$lte?: T;
	$gte?: T;
	$like?: string;
	$in?: T[];
	$neq?: T;
	$isNull?: boolean;
};

/**
 * A condition value can be a plain value (shorthand for $eq) or an operator object.
 */
export type ConditionValue<T> = T | ConditionOperators<T>;

/**
 * Where conditions for a table - keys must exist in table schema.
 */
export type WhereConditions<T extends Table<any>> = {
	[K in keyof Infer<T>]?: ConditionValue<Infer<T>[K]>;
};

/**
 * Set values for updates - plain values only (no operators).
 */
export type SetValues<T extends Table<any>> = {
	[K in keyof Infer<T>]?: Infer<T>[K];
};

/**
 * A partial view of a table created via pick().
 * Can be used for queries but not for insert().
 */
export interface PartialTable<T extends ZodRawShape = ZodRawShape> extends Omit<
	Table<T>,
	"_meta"
> {
	readonly _meta: Table<T>["_meta"] & {isPartial: true};
}

export interface Table<T extends ZodRawShape = ZodRawShape> {
	readonly [TABLE_MARKER]: true;
	readonly name: string;
	readonly schema: ZodObject<T>;
	readonly indexes: string[][];

	// Pre-extracted metadata (no Zod walking needed)
	readonly _meta: {
		primary: string | null;
		unique: string[];
		indexed: string[];
		softDeleteField: string | null;
		references: ReferenceInfo[];
		fields: Record<string, FieldDbMeta>;
		/** True if this is a partial table created via pick() */
		isPartial?: boolean;
	};

	/** Get field metadata for forms/admin */
	fields(): Record<string, FieldMeta>;

	/**
	 * Fully qualified primary key column as SQL fragment.
	 *
	 * @example
	 * db.all(Posts)`GROUP BY ${Posts.primary}`
	 * // → GROUP BY "posts"."id"
	 */
	readonly primary: ColumnFragment | null;

	/** Get all foreign key references */
	references(): ReferenceInfo[];

	/**
	 * Generate SQL fragment to check if a row is soft-deleted.
	 *
	 * Returns `"table"."deleted_at" IS NOT NULL` where deleted_at is the field
	 * marked with softDelete().
	 *
	 * @throws Error if table doesn't have a soft delete field
	 *
	 * @example
	 * // Exclude soft-deleted records
	 * db.all(Posts)`WHERE NOT (${Posts.deleted()}) AND published = ${true}`
	 *
	 * // Show only soft-deleted records
	 * db.all(Posts)`WHERE ${Posts.deleted()}`
	 */
	deleted(): SQLFragment;

	/**
	 * Generate safe IN clause with proper parameterization.
	 *
	 * Prevents SQL injection and handles empty arrays correctly.
	 *
	 * @throws Error if field doesn't exist in table
	 *
	 * @example
	 * db.all(Posts)`WHERE ${Posts.in("id", postIds)}`
	 * // Generates: "posts"."id" IN (?, ?, ?)
	 *
	 * // Empty array returns FALSE
	 * db.all(Posts)`WHERE ${Posts.in("id", [])}`
	 * // Generates: 1 = 0
	 */
	in<K extends keyof z.infer<ZodObject<T>>>(
		field: K,
		values: unknown[],
	): SQLFragment;

	/**
	 * Create a partial view of this table with only the specified fields.
	 *
	 * Useful for partial selects - the returned table-like object can be
	 * passed to all(), get(), where(), etc. Cannot be used with insert().
	 *
	 * @example
	 * const PostSummary = Posts.pick('id', 'title', 'authorId');
	 * db.all(PostSummary, Users.pick('id', 'name'))`...`
	 */
	pick<K extends keyof z.infer<ZodObject<T>>>(
		...fields: K[]
	): PartialTable<Pick<T, K & keyof T>>;

	/**
	 * Access qualified column names as SQL fragments.
	 *
	 * Each property returns a fragment with the fully qualified, quoted column name.
	 * Useful for JOINs, ORDER BY, GROUP BY, or disambiguating columns.
	 *
	 * @example
	 * db.all(Posts, Users)`
	 *   JOIN users ON ${Users.cols.id} = ${Posts.cols.authorId}
	 *   WHERE ${Posts.cols.published} = ${true}
	 *   ORDER BY ${Posts.cols.createdAt} DESC
	 * `
	 * // → JOIN users ON "users"."id" = "posts"."authorId" WHERE "posts"."published" = ? ORDER BY "posts"."createdAt" DESC
	 */
	readonly cols: {
		[K in keyof z.infer<ZodObject<T>>]: ColumnFragment;
	};

	/**
	 * Generate an AND-joined conditional fragment for WHERE clauses.
	 *
	 * Emits fully qualified column names to avoid ambiguity in JOINs.
	 *
	 * @example
	 * db.all(Posts)`
	 *   WHERE ${Posts.where({ published: true, viewCount: { $gte: 100 } })}
	 * `
	 * // → "posts"."published" = ? AND "posts"."viewCount" >= ?
	 */
	where(conditions: WhereConditions<Table<T>>): SQLFragment;

	/**
	 * Generate assignment fragment for UPDATE SET clauses.
	 *
	 * Column names are quoted but not table-qualified (SQL UPDATE syntax).
	 *
	 * @example
	 * db.exec`
	 *   UPDATE posts SET ${Posts.set({ title: "New Title" })} WHERE id = ${id}
	 * `
	 * // → "title" = ?
	 */
	set(values: SetValues<Table<T>>): SQLFragment;

	/**
	 * Generate foreign-key equality fragment for JOIN ON clauses.
	 *
	 * Emits fully qualified, quoted column names.
	 *
	 * @example
	 * db.all(Posts, Users)`
	 *   JOIN users ON ${Posts.on("authorId")}
	 * `
	 * // → JOIN users ON "users"."id" = "posts"."authorId"
	 */
	on(field: keyof z.infer<ZodObject<T>> & string): SQLFragment;

	/**
	 * Generate CREATE TABLE DDL for this table.
	 *
	 * **Pure function**: Generates SQL from table schema definition only, without
	 * inspecting the actual database. Does not check if table exists or validate
	 * against current database state.
	 *
	 * **Dialect-dependent**: Output varies by SQL dialect. Specify the target
	 * dialect explicitly or use the driver's default. The same table definition
	 * will produce different SQL for SQLite vs PostgreSQL vs MySQL.
	 *
	 * @param options - DDL generation options (dialect, ifNotExists)
	 * @returns SQL CREATE TABLE statement (dialect-specific)
	 *
	 * @example
	 * // In migrations
	 * await db.exec`${Posts.ddl()}`;
	 *
	 * // With options
	 * await db.exec`${Posts.ddl({ dialect: "postgresql", ifNotExists: true })}`;
	 */
	ddl(options?: {dialect?: "sqlite" | "postgresql" | "mysql"; ifNotExists?: boolean}): string;

	/**
	 * Generate idempotent ALTER TABLE statement to add a column if it doesn't exist.
	 * Reads column definition from table schema.
	 *
	 * **Idempotent**: Safe to run multiple times - uses IF NOT EXISTS to avoid errors.
	 *
	 * **Type compatibility**: If the column already exists with a different type,
	 * behavior depends on the database (may fail or be silently ignored). For type
	 * changes, use a multi-step migration: add new column, copy data, drop old column.
	 *
	 * **Pure function**: Generates SQL from schema without inspecting the database.
	 *
	 * @param fieldName - Name of field from table schema
	 * @param options - Dialect for SQL generation
	 * @returns SQL ALTER TABLE ADD COLUMN statement
	 *
	 * @example
	 * // Add new field to schema:
	 * const Posts = table("posts", {
	 *   id: primary(z.string()),
	 *   title: z.string(),
	 *   views: z.number().default(0), // NEW
	 * });
	 *
	 * // In migration:
	 * if (e.oldVersion < 2) {
	 *   await db.exec`${Posts.ensureColumn("views")}`;
	 * }
	 * // → ALTER TABLE posts ADD COLUMN IF NOT EXISTS views INTEGER DEFAULT 0
	 */
	ensureColumn(fieldName: keyof z.infer<ZodObject<T>> & string, options?: {dialect?: "sqlite" | "postgresql" | "mysql"}): string;

	/**
	 * Generate idempotent CREATE INDEX statement.
	 *
	 * **Idempotent**: Safe to run multiple times - uses IF NOT EXISTS to avoid errors.
	 *
	 * **Index naming**: Auto-generates index name as `idx_{table}_{field1}_{field2}...`
	 * unless explicitly provided via `options.name`. Explicit names recommended for
	 * important indexes to avoid name conflicts.
	 *
	 * **Non-unique**: Creates a regular (non-unique) index. For unique constraints,
	 * use the `unique()` field wrapper in your schema instead.
	 *
	 * **Compatibility**: If an index with the same name exists but differs (different
	 * columns, different uniqueness), behavior is database-dependent (may fail or be
	 * silently ignored).
	 *
	 * @param fields - Array of field names to index
	 * @param options - Index name and dialect
	 * @returns SQL CREATE INDEX statement
	 *
	 * @example
	 * if (e.oldVersion < 3) {
	 *   await db.exec`${Posts.ensureIndex(["authorId", "createdAt"])}`;
	 * }
	 * // → CREATE INDEX IF NOT EXISTS idx_posts_authorId_createdAt ON posts(authorId, createdAt)
	 *
	 * @example
	 * // With explicit name
	 * await db.exec`${Posts.ensureIndex(["authorId"], { name: "posts_author_idx" })}`;
	 */
	ensureIndex(fields: (keyof z.infer<ZodObject<T>> & string)[], options?: {name?: string; dialect?: "sqlite" | "postgresql" | "mysql"}): string;

	/**
	 * Generate UPDATE statement to copy data from one column to another.
	 *
	 * **Idempotent**: Safe to run multiple times - only copies WHERE destination IS NULL.
	 *
	 * **Type compatibility**: Does NOT verify types match. Ensure source and destination
	 * columns have compatible types or the database will error. For type conversions,
	 * write custom migration SQL with explicit casting.
	 *
	 * **Data safety**: Only updates rows where destination is NULL. Will NOT overwrite
	 * existing non-NULL values. If destination has non-NULL data, those rows are skipped.
	 *
	 * **Pure function**: Generates SQL from schema without inspecting the database.
	 *
	 * Useful for safe column renames in migrations:
	 * 1. Add new column to schema
	 * 2. copyColumn() to migrate data
	 * 3. Drop old column in future migration (manual)
	 *
	 * @param fromField - Source column name (may not exist in current schema)
	 * @param toField - Destination field from table schema
	 * @returns SQL UPDATE statement
	 *
	 * @example
	 * // Schema now has emailAddress instead of email
	 * const Users = table("users", {
	 *   id: primary(z.string()),
	 *   emailAddress: z.string().email(), // renamed from "email"
	 * });
	 *
	 * if (e.oldVersion < 4) {
	 *   await db.exec`${Users.ensureColumn("emailAddress")}`;
	 *   await db.exec`${Users.copyColumn("email", "emailAddress")}`;
	 *   // Keep "email" for backwards compat, drop in later migration if needed
	 * }
	 * // → UPDATE users SET emailAddress = email WHERE emailAddress IS NULL
	 */
	copyColumn(fromField: string, toField: keyof z.infer<ZodObject<T>> & string): string;

	/**
	 * Generate column list and value tuples for INSERT statements.
	 *
	 * Columns are inferred from the first row's keys. All rows must have
	 * the same columns. Each row is validated against the table schema.
	 *
	 * @example
	 * db.exec`
	 *   INSERT INTO posts ${Posts.values([
	 *     {id: "1", title: "First"},
	 *     {id: "2", title: "Second"},
	 *   ])}
	 * `
	 * // → INSERT INTO posts (id, title) VALUES (?, ?), (?, ?)
	 */
	values(rows: Partial<z.infer<ZodObject<T>>>[]): SQLFragment;
}

/**
 * Define a database table with a Zod schema.
 *
 * @example
 * const users = table("users", {
 *   id: primary(z.string().uuid()),
 *   email: unique(z.string().email()),
 *   name: z.string().max(100),
 *   role: z.enum(["user", "admin"]).default("user"),
 * });
 */
export function table<T extends Record<string, ZodTypeAny>>(
	name: string,
	shape: T,
	options: TableOptions = {},
): Table<any> {
	// Validate table name doesn't contain dots (would break normalization)
	if (name.includes(".")) {
		throw new TableDefinitionError(
			`Invalid table name "${name}": table names cannot contain "." as it conflicts with normalization prefixes`,
			name,
		);
	}

	// Extract Zod schemas and metadata from .meta()
	const zodShape: Record<string, ZodTypeAny> = {};
	const meta = {
		primary: null as string | null,
		unique: [] as string[],
		indexed: [] as string[],
		softDeleteField: null as string | null,
		references: [] as ReferenceInfo[],
		fields: {} as Record<string, FieldDbMeta>,
	};

	for (const [key, value] of Object.entries(shape)) {
		// Validate field names don't contain dots (would break normalization)
		if (key.includes(".")) {
			throw new TableDefinitionError(
				`Invalid field name "${key}" in table "${name}": field names cannot contain "." as it conflicts with normalization prefixes`,
				name,
				key,
			);
		}

		const fieldSchema = value as ZodTypeAny;
		zodShape[key] = fieldSchema;

		// Read database metadata from namespaced .meta()
		const fieldDbMeta = getDBMeta(fieldSchema);
		const dbMeta: FieldDbMeta = {};

		if (fieldDbMeta.primary) {
			if (meta.primary !== null) {
				throw new TableDefinitionError(
					`Table "${name}" has multiple primary keys: "${meta.primary}" and "${key}". Only one primary key is allowed.`,
					name,
				);
			}
			meta.primary = key;
			dbMeta.primaryKey = true;
		}
		if (fieldDbMeta.unique) {
			meta.unique.push(key);
			dbMeta.unique = true;
		}
		if (fieldDbMeta.indexed) {
			meta.indexed.push(key);
			dbMeta.indexed = true;
		}
		if (fieldDbMeta.softDelete) {
			if (meta.softDeleteField !== null) {
				throw new TableDefinitionError(
					`Table "${name}" has multiple soft delete fields: "${meta.softDeleteField}" and "${key}". Only one soft delete field is allowed.`,
					name,
				);
			}
			meta.softDeleteField = key;
			dbMeta.softDelete = true;
		}
		if (fieldDbMeta.reference) {
			const ref = fieldDbMeta.reference;
			meta.references.push({
				fieldName: key,
				table: ref.table,
				referencedField: ref.field ?? ref.table._meta.primary ?? "id",
				as: ref.as,
				onDelete: ref.onDelete,
			});
			dbMeta.reference = ref;
		}

		meta.fields[key] = dbMeta;
	}

	const schema = z.object(zodShape as any);

	return createTableObject(name, schema, zodShape, meta, options.indexes ?? []);
}

/**
 * Create a Table object with all methods. Shared between table() and pick().
 */
/**
 * Quote an identifier with double quotes (ANSI SQL standard).
 * MySQL users should enable ANSI_QUOTES mode.
 */
function quoteIdentifier(id: string): string {
	return `"${id.replace(/"/g, '""')}"`;
}

/**
 * Create a fully qualified column name: "table"."column"
 */
function qualifiedColumn(tableName: string, fieldName: string): string {
	return `${quoteIdentifier(tableName)}.${quoteIdentifier(fieldName)}`;
}

/**
 * Check if a value is an operator object.
 */
function isOperatorObject(
	value: unknown,
): value is ConditionOperators<unknown> {
	if (value === null || typeof value !== "object") return false;
	const keys = Object.keys(value);
	return keys.length > 0 && keys.every((k) => k.startsWith("$"));
}

/**
 * Build a condition fragment for a single field.
 */
function buildCondition(
	column: string,
	value: ConditionValue<unknown>,
): {sql: string; params: unknown[]} {
	if (isOperatorObject(value)) {
		const parts: string[] = [];
		const params: unknown[] = [];

		if (value.$eq !== undefined) {
			parts.push(`${column} = ?`);
			params.push(value.$eq);
		}
		if (value.$neq !== undefined) {
			parts.push(`${column} != ?`);
			params.push(value.$neq);
		}
		if (value.$lt !== undefined) {
			parts.push(`${column} < ?`);
			params.push(value.$lt);
		}
		if (value.$gt !== undefined) {
			parts.push(`${column} > ?`);
			params.push(value.$gt);
		}
		if (value.$gte !== undefined) {
			parts.push(`${column} >= ?`);
			params.push(value.$gte);
		}
		if (value.$lte !== undefined) {
			parts.push(`${column} <= ?`);
			params.push(value.$lte);
		}
		if (value.$like !== undefined) {
			parts.push(`${column} LIKE ?`);
			params.push(value.$like);
		}
		if (value.$in !== undefined && Array.isArray(value.$in)) {
			const placeholders = value.$in.map(() => "?").join(", ");
			parts.push(`${column} IN (${placeholders})`);
			params.push(...value.$in);
		}
		if (value.$isNull !== undefined) {
			parts.push(value.$isNull ? `${column} IS NULL` : `${column} IS NOT NULL`);
		}

		return {
			sql: parts.join(" AND "),
			params,
		};
	}

	// Plain value = $eq shorthand
	return {
		sql: `${column} = ?`,
		params: [value],
	};
}

/**
 * Create a column fragment proxy for Table.cols access.
 */
function createColsProxy(
	tableName: string,
	zodShape: Record<string, ZodTypeAny>,
): Record<string, ColumnFragment> {
	return new Proxy({} as Record<string, ColumnFragment>, {
		get(_target, prop: string): ColumnFragment | undefined {
			if (prop in zodShape) {
				return {
					[SQL_FRAGMENT]: true,
					sql: `${quoteIdentifier(tableName)}.${quoteIdentifier(prop)}`,
					params: [],
				};
			}
			return undefined;
		},
		has(_target, prop: string): boolean {
			return prop in zodShape;
		},
		ownKeys(): string[] {
			return Object.keys(zodShape);
		},
		getOwnPropertyDescriptor(_target, prop: string) {
			if (prop in zodShape) {
				return {enumerable: true, configurable: true};
			}
			return undefined;
		},
	});
}

function createTableObject(
	name: string,
	schema: ZodObject<any>,
	zodShape: Record<string, ZodTypeAny>,
	meta: {
		primary: string | null;
		unique: string[];
		indexed: string[];
		softDeleteField: string | null;
		references: ReferenceInfo[];
		fields: Record<string, FieldDbMeta>;
	},
	indexes: string[][],
): Table<any> {
	const cols = createColsProxy(name, zodShape);

	// Create primary key fragment
	const primary: ColumnFragment | null = meta.primary
		? {
				[SQL_FRAGMENT]: true,
				sql: `${quoteIdentifier(name)}.${quoteIdentifier(meta.primary)}`,
				params: [],
			}
		: null;

	return {
		[TABLE_MARKER]: true,
		name,
		schema,
		indexes,
		_meta: meta,
		cols,
		primary,

		fields(): Record<string, FieldMeta> {
			const result: Record<string, FieldMeta> = {};

			for (const [key, zodType] of Object.entries(zodShape)) {
				const dbMeta = meta.fields[key] || {};
				result[key] = extractFieldMeta(key, zodType, dbMeta);
			}

			return result;
		},

		references(): ReferenceInfo[] {
			return meta.references;
		},

		deleted(): SQLFragment {
			const softDeleteField = meta.softDeleteField;
			if (!softDeleteField) {
				throw new Error(
					`Table "${name}" does not have a soft delete field. Use softDelete() wrapper to mark a field.`,
				);
			}
			return createFragment(
				`${quoteIdentifier(name)}.${quoteIdentifier(softDeleteField)} IS NOT NULL`,
				[],
			);
		},

		in(field: string, values: unknown[]): SQLFragment {
			// Validate field exists
			if (!(field in zodShape)) {
				throw new Error(
					`Field "${field}" does not exist in table "${name}". Available fields: ${Object.keys(zodShape).join(", ")}`,
				);
			}

			// Handle empty array - return always-false condition
			if (values.length === 0) {
				return createFragment("1 = 0", []);
			}

			// Generate IN clause with placeholders
			const placeholders = values.map(() => "?").join(", ");
			const sql = `${quoteIdentifier(name)}.${quoteIdentifier(field)} IN (${placeholders})`;

			return createFragment(sql, values);
		},

		pick(...fields: any[]): PartialTable<any> {
			const fieldSet = new Set(fields as string[]);

			// Pick the schema fields
			const pickObj: Record<string, true> = {};
			for (const f of fields as string[]) {
				pickObj[f] = true;
			}
			const pickedSchema = schema.pick(pickObj);

			// Filter zodShape to only picked fields
			const pickedZodShape: Record<string, ZodTypeAny> = {};
			for (const f of fields as string[]) {
				if (f in zodShape) {
					pickedZodShape[f] = zodShape[f];
				}
			}

			// Filter metadata
			const pickedMeta = {
				primary:
					meta.primary && fieldSet.has(meta.primary) ? meta.primary : null,
				unique: meta.unique.filter((f) => fieldSet.has(f)),
				indexed: meta.indexed.filter((f) => fieldSet.has(f)),
				softDeleteField:
					meta.softDeleteField && fieldSet.has(meta.softDeleteField)
						? meta.softDeleteField
						: null,
				references: meta.references.filter((r) => fieldSet.has(r.fieldName)),
				fields: Object.fromEntries(
					Object.entries(meta.fields).filter(([k]) => fieldSet.has(k)),
				),
				isPartial: true,
			};

			// Filter indexes to only those with all fields present
			const pickedIndexes = indexes.filter((idx) =>
				idx.every((f) => fieldSet.has(f)),
			);

			return createTableObject(
				name,
				pickedSchema,
				pickedZodShape,
				pickedMeta,
				pickedIndexes,
			) as PartialTable<any>;
		},

		where(conditions: Record<string, unknown>): SQLFragment {
			const entries = Object.entries(conditions);
			if (entries.length === 0) {
				return createFragment("1 = 1", []);
			}

			const parts: string[] = [];
			const params: unknown[] = [];

			for (const [field, value] of entries) {
				if (value === undefined) continue;

				const column = qualifiedColumn(name, field);
				const condition = buildCondition(column, value);
				parts.push(condition.sql);
				params.push(...condition.params);
			}

			if (parts.length === 0) {
				return createFragment("1 = 1", []);
			}

			return createFragment(parts.join(" AND "), params);
		},

		set(values: Record<string, unknown>): SQLFragment {
			const entries = Object.entries(values);
			if (entries.length === 0) {
				throw new Error("set() requires at least one field");
			}

			const parts: string[] = [];
			const params: unknown[] = [];

			for (const [field, value] of entries) {
				if (value === undefined) continue;

				parts.push(`${quoteIdentifier(field)} = ?`);
				params.push(value);
			}

			if (parts.length === 0) {
				throw new Error("set() requires at least one non-undefined field");
			}

			return createFragment(parts.join(", "), params);
		},

		on(field: string): SQLFragment {
			const ref = meta.references.find((r) => r.fieldName === field);

			if (!ref) {
				throw new Error(
					`Field "${field}" is not a foreign key reference in table "${name}"`,
				);
			}

			const refColumn = qualifiedColumn(ref.table.name, ref.referencedField);
			const fkColumn = qualifiedColumn(name, field);

			return createFragment(`${refColumn} = ${fkColumn}`, []);
		},

		ddl(options?: {dialect?: "sqlite" | "postgresql" | "mysql"; ifNotExists?: boolean}): string {
			// Dynamic import to avoid circular dependency (ddl.ts imports Table from table.ts)
			const {generateDDL} = require("./ddl.js");
			return generateDDL(this as Table<any>, options);
		},

		ensureColumn(fieldName: string, options?: {dialect?: "sqlite" | "postgresql" | "mysql"}): string {
			const {dialect = "sqlite"} = options || {};

			// Validate field exists in schema
			if (!(fieldName in zodShape)) {
				throw new Error(
					`Field "${fieldName}" does not exist in table "${name}". Available fields: ${Object.keys(zodShape).join(", ")}`
				);
			}

			// Use ddl.ts to generate the column definition
			const {generateColumnDDL} = require("./ddl.js");
			const columnDef = generateColumnDDL(fieldName, zodShape[fieldName], meta.fields[fieldName] || {}, dialect);

			// SQLite and PostgreSQL support IF NOT EXISTS
			const ifNotExists = dialect === "sqlite" || dialect === "postgresql" ? "IF NOT EXISTS " : "";
			const quote = dialect === "mysql" ? "`" : '"';

			return `ALTER TABLE ${quote}${name}${quote} ADD COLUMN ${ifNotExists}${columnDef}`;
		},

		ensureIndex(fields: string[], options?: {name?: string; dialect?: "sqlite" | "postgresql" | "mysql"}): string {
			const {dialect = "sqlite", name: indexName} = options || {};

			// Validate all fields exist
			for (const field of fields) {
				if (!(field in zodShape)) {
					throw new Error(
						`Field "${field}" does not exist in table "${name}". Available fields: ${Object.keys(zodShape).join(", ")}`
					);
				}
			}

			// Generate index name if not provided
			const finalIndexName = indexName || `idx_${name}_${fields.join("_")}`;
			const quote = dialect === "mysql" ? "`" : '"';
			const quotedColumns = fields.map(f => `${quote}${f}${quote}`).join(", ");

			return `CREATE INDEX IF NOT EXISTS ${quote}${finalIndexName}${quote} ON ${quote}${name}${quote}(${quotedColumns})`;
		},

		copyColumn(fromField: string, toField: string): string {
			// Validate toField exists in schema
			if (!(toField in zodShape)) {
				throw new Error(
					`Destination field "${toField}" does not exist in table "${name}". Available fields: ${Object.keys(zodShape).join(", ")}`
				);
			}

			// Note: fromField might not exist in current schema (it's the old column)
			// Generate UPDATE with WHERE IS NULL for idempotency
			return `UPDATE ${quoteIdentifier(name)} SET ${quoteIdentifier(toField)} = ${quoteIdentifier(fromField)} WHERE ${quoteIdentifier(toField)} IS NULL`;
		},

		values(rows: Record<string, unknown>[]): SQLFragment {
			if (rows.length === 0) {
				throw new Error("values() requires at least one row");
			}

			// Infer columns from first row
			const columns = Object.keys(rows[0]);
			if (columns.length === 0) {
				throw new Error("values() requires at least one column");
			}

			const partialSchema = schema.partial();
			const params: unknown[] = [];
			const tuples: string[] = [];

			for (const row of rows) {
				const validated = validateWithStandardSchema(partialSchema, row);
				const rowPlaceholders: string[] = [];

				for (const col of columns) {
					if (!(col in row)) {
						throw new Error(
							`All rows must have the same columns. Row is missing column "${col}"`,
						);
					}
					rowPlaceholders.push("?");
					params.push((validated as Record<string, unknown>)[col]);
				}

				tuples.push(`(${rowPlaceholders.join(", ")})`);
			}

			// Include column list and VALUES keyword
			const columnList = columns.map((c) => quoteIdentifier(c)).join(", ");
			return createFragment(
				`(${columnList}) VALUES ${tuples.join(", ")}`,
				params,
			);
		},
	};
}

// ============================================================================
// Field Metadata Extraction (using only public Zod APIs)
// ============================================================================

interface UnwrapResult {
	core: z.ZodType;
	isOptional: boolean;
	isNullable: boolean;
	hasDefault: boolean;
	defaultValue?: unknown;
	/** Collected .meta() from all layers, merged (outer overrides inner) */
	collectedMeta: Record<string, unknown>;
}

/**
 * Extract .meta() from a single schema layer.
 */
function getLayerMeta(schema: z.ZodType): Record<string, unknown> {
	if (typeof (schema as any).meta === "function") {
		return (schema as any).meta() ?? {};
	}
	return {};
}

/**
 * Unwrap wrapper types using public Zod APIs only.
 * No _def access - uses removeDefault(), unwrap(), innerType(), etc.
 * Collects .meta() from all layers during unwrapping.
 */
function unwrapType(schema: z.ZodType): UnwrapResult {
	let core: z.ZodType = schema;
	let hasDefault = false;
	let defaultValue: unknown = undefined;

	// Collect meta from all layers (will merge at the end, outer wins)
	const metaLayers: Record<string, unknown>[] = [];

	// Use public isOptional/isNullable
	const isOptional = schema.isOptional();
	const isNullable = schema.isNullable();

	// Unwrap layers using public methods, collecting meta at each step
	while (true) {
		// Collect meta from current layer
		metaLayers.push(getLayerMeta(core));

		// Check for ZodDefault (has removeDefault method)
		if (typeof (core as any).removeDefault === "function") {
			hasDefault = true;
			try {
				defaultValue = core.parse(undefined);
			} catch {
				// Default might be a function that throws
			}
			core = (core as any).removeDefault();
			continue;
		}

		// Check for ZodOptional/ZodNullable (has unwrap method)
		if (typeof (core as any).unwrap === "function") {
			core = (core as any).unwrap();
			continue;
		}

		// Check for ZodEffects (has innerType method)
		if (typeof (core as any).innerType === "function") {
			core = (core as any).innerType();
			continue;
		}

		// No more wrappers
		break;
	}

	// Merge meta: inner layers first, outer layers override
	// metaLayers[0] is outermost, metaLayers[n-1] is innermost
	// Spread in reverse so outer wins
	const collectedMeta: Record<string, unknown> = {};
	for (let i = metaLayers.length - 1; i >= 0; i--) {
		Object.assign(collectedMeta, metaLayers[i]);
	}

	return {
		core,
		isOptional,
		isNullable,
		hasDefault,
		defaultValue,
		collectedMeta,
	};
}

/**
 * Extract field metadata using instanceof checks and public properties.
 * No _def access. Merges Zod 4 .meta() for Shovel UI metadata.
 */
function extractFieldMeta(
	name: string,
	zodType: ZodTypeAny,
	dbMeta: FieldDbMeta,
): FieldMeta {
	const {
		core,
		isOptional,
		isNullable,
		hasDefault,
		defaultValue,
		collectedMeta,
	} = unwrapType(zodType);

	const meta: FieldMeta = {
		name,
		type: "text",
		required: !isOptional && !isNullable && !hasDefault,
		...collectedMeta, // Spread user-defined metadata (label, helpText, widget, etc.)
	};

	// Apply database metadata
	if (dbMeta.primaryKey) meta.primaryKey = true;
	if (dbMeta.unique) meta.unique = true;
	if (dbMeta.indexed) meta.indexed = true;
	if (dbMeta.reference) {
		meta.reference = {
			table: dbMeta.reference.table.name,
			field:
				dbMeta.reference.field ?? dbMeta.reference.table._meta.primary ?? "id",
			as: dbMeta.reference.as,
		};
	}

	if (defaultValue !== undefined) {
		meta.default = defaultValue;
	}

	// Determine field type using instanceof and public properties
	if (core instanceof z.ZodString) {
		meta.type = "text";

		// Use public properties for string checks
		const str = core as any;
		// Zod 4 uses .format, Zod 3 uses .isEmail/.isURL
		if (str.format === "email" || str.isEmail) meta.type = "email";
		if (str.format === "url" || str.isURL) meta.type = "url";
		if (str.maxLength !== undefined) {
			meta.maxLength = str.maxLength;
			if (str.maxLength > 500) meta.type = "textarea";
		}
		if (str.minLength !== undefined) {
			meta.minLength = str.minLength;
		}
	} else if (core instanceof z.ZodNumber) {
		meta.type = "number";

		// Use public properties for number checks
		const num = core as any;
		// Zod 4 uses .format for "int", Zod 3 uses .isInt
		if (num.format === "int" || num.isInt) meta.type = "integer";
		if (num.minValue !== undefined) meta.min = num.minValue;
		if (num.maxValue !== undefined) meta.max = num.maxValue;
	} else if (core instanceof z.ZodBoolean) {
		meta.type = "checkbox";
	} else if (core instanceof z.ZodDate) {
		meta.type = "datetime";
	} else if (core instanceof z.ZodEnum) {
		meta.type = "select";
		// Use public options property
		meta.options = (core as any).options;
	} else if (core instanceof z.ZodArray || core instanceof z.ZodObject) {
		meta.type = "json";
	}

	return meta;
}

// ============================================================================
// Type Inference
// ============================================================================

/**
 * Infer the TypeScript type from a table (full document after read).
 */
export type Infer<T extends Table<any>> = z.infer<T["schema"]>;

/**
 * Infer the insert type (respects defaults).
 */
export type Insert<T extends Table<any>> = z.input<T["schema"]>;
