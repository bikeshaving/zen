/**
 * Table definition with wrapper-based field extensions.
 *
 * Uses wrapper functions instead of .pipe() to avoid Zod internals.
 * Metadata is extracted once at table() call time.
 */

import {z, ZodTypeAny, ZodObject, ZodRawShape} from "zod";

import {TableDefinitionError} from "./errors.js";
import {createFragment, type SQLFragment, createDDLFragment, type DDLFragment} from "./query.js";
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
	encode?: (value: any) => any;
	decode?: (value: any) => any;
	/** Explicit column type override for DDL generation */
	columnType?: string;
	/** DB expression to apply on insert (e.g., db.now()) */
	inserted?: unknown;
	/** DB expression to apply on insert and update (e.g., db.now()) */
	updated?: unknown;
}

// ============================================================================
// .db namespace - Fluent API for database metadata
// ============================================================================

/**
 * Create the .db methods object that will be added to all Zod schemas.
 * This is shared across all types and bound to the schema instance.
 */
function createDbMethods(schema: ZodTypeAny) {
	return {
			/**
			 * Mark field as primary key.
			 * @example z.string().uuid().db.primary()
			 */
			primary() {
				return setDBMeta(schema, {primary: true});
			},

			/**
			 * Mark field as unique.
			 * @example z.string().email().db.unique()
			 */
			unique() {
				return setDBMeta(schema, {unique: true});
			},

			/**
			 * Create an index on this field.
			 * @example z.date().db.index()
			 */
			index() {
				return setDBMeta(schema, {indexed: true});
			},

			/**
			 * Mark field as soft delete timestamp.
			 * @example z.date().nullable().default(null).db.softDelete()
			 */
			softDelete() {
				return setDBMeta(schema, {softDelete: true});
			},

			/**
			 * Define a foreign key reference with optional reverse relationship.
			 *
			 * @example
			 * // Forward reference only
			 * authorId: z.string().uuid().db.references(Users, {as: "author"})
			 *
			 * @example
			 * // With reverse relationship
			 * authorId: z.string().uuid().db.references(Users, {
			 *   as: "author",      // post.author = User
			 *   reverseAs: "posts" // user.posts = Post[]
			 * })
			 */
			references(table: Table<any>, options: {
				field?: string;
				as: string;
				reverseAs?: string;
				onDelete?: "cascade" | "set null" | "restrict";
			}) {
				return setDBMeta(schema, {
					reference: {
						table,
						field: options.field,
						as: options.as,
						reverseAs: options.reverseAs,
						onDelete: options.onDelete,
					},
				});
			},

			/**
			 * Encode app values to DB values (for INSERT/UPDATE).
			 * One-way transformation is fine (e.g., password hashing).
			 *
			 * @example
			 * password: z.string().db.encode(hashPassword)
			 *
			 * @example
			 * // Bidirectional: pair with .db.decode()
			 * status: z.enum(["pending", "active"])
			 *   .db.encode(s => statusMap.indexOf(s))
			 *   .db.decode(i => statusMap[i])
			 */
			encode<TDB>(encodeFn: (app: any) => TDB) {
				// Validate: encode cannot be combined with inserted/updated
				const existing = getDBMeta(schema);
				if (existing.inserted || existing.updated) {
					throw new TableDefinitionError(
						`encode() cannot be combined with inserted() or updated(). ` +
						`DB expressions bypass encoding and are sent directly to the database.`,
					);
				}
				return setDBMeta(schema, {encode: encodeFn});
			},

			/**
			 * Decode DB values to app values (for SELECT).
			 * One-way transformation is fine.
			 *
			 * @example
			 * legacy: z.string().db.decode(deserializeLegacyFormat)
			 */
			decode<TApp>(decodeFn: (db: any) => TApp) {
				// Validate: decode cannot be combined with inserted/updated
				const existing = getDBMeta(schema);
				if (existing.inserted || existing.updated) {
					throw new TableDefinitionError(
						`decode() cannot be combined with inserted() or updated(). ` +
						`DB expressions bypass decoding and are sent directly to the database.`,
					);
				}
				return setDBMeta(schema, {decode: decodeFn});
			},

			/**
			 * Specify explicit column type for DDL generation.
			 * Required when using custom encode/decode on objects/arrays
			 * that transform to a different storage type.
			 *
			 * @example
			 * // Store array as CSV instead of JSON
			 * tags: z.array(z.string())
			 *   .db.encode((arr) => arr.join(","))
			 *   .db.decode((str) => str.split(","))
			 *   .db.type("TEXT")
			 */
			type(columnType: string) {
				return setDBMeta(schema, {columnType});
			},

			/**
			 * Auto-apply a DB expression on insert.
			 * Field becomes optional for insert - the expression provides the value.
			 *
			 * @example
			 * createdAt: z.date().db.inserted(db.now())
			 */
			inserted(expr: unknown) {
				// Validate at definition time that this is a DBExpression
				const DB_EXPR = Symbol.for("@b9g/zealot:db-expr");
				if (
					expr === null ||
					typeof expr !== "object" ||
					!(DB_EXPR in expr) ||
					(expr as any)[DB_EXPR] !== true
				) {
					throw new TableDefinitionError(
						`inserted() requires a DB expression (e.g., db.now()), got ${typeof expr}`,
					);
				}
				// Validate: inserted cannot be combined with encode/decode
				const existing = getDBMeta(schema);
				if (existing.encode || existing.decode) {
					throw new TableDefinitionError(
						`inserted() cannot be combined with encode() or decode(). ` +
						`DB expressions bypass encoding/decoding and are sent directly to the database.`,
					);
				}
				return setDBMeta(schema, {inserted: expr});
			},

			/**
			 * Auto-apply a DB expression on insert and update.
			 * Field becomes optional for insert/update - the expression provides the value.
			 *
			 * @example
			 * updatedAt: z.date().db.updated(db.now())
			 */
			updated(expr: unknown) {
				// Validate at definition time that this is a DBExpression
				const DB_EXPR = Symbol.for("@b9g/zealot:db-expr");
				if (
					expr === null ||
					typeof expr !== "object" ||
					!(DB_EXPR in expr) ||
					(expr as any)[DB_EXPR] !== true
				) {
					throw new TableDefinitionError(
						`updated() requires a DB expression (e.g., db.now()), got ${typeof expr}`,
					);
				}
				// Validate: updated cannot be combined with encode/decode
				const existing = getDBMeta(schema);
				if (existing.encode || existing.decode) {
					throw new TableDefinitionError(
						`updated() cannot be combined with encode() or decode(). ` +
						`DB expressions bypass encoding/decoding and are sent directly to the database.`,
					);
				}
				return setDBMeta(schema, {updated: expr});
			},
		};
}

/**
 * Extend Zod with .db namespace for database-specific methods.
 *
 * Call this once at application startup to add the .db namespace to all Zod types:
 *
 * @example
 * import {z} from "zod";
 * import {extendZod, table} from "@b9g/zealot";
 *
 * extendZod(z);
 *
 * const Users = table("users", {
 *   id: z.string().uuid().db.primary(),
 *   email: z.string().email().db.unique(),
 * });
 *
 * @param zodModule - The Zod module to extend (typically `z` from `import {z} from "zod"`)
 */
export function extendZod(zodModule: typeof z): void {
	// Programmatically extend ALL Zod type constructors
	// This future-proofs against new Zod types being added
	for (const key of Object.keys(zodModule)) {
		const value = (zodModule as any)[key];

		// Check if this is a Zod type constructor (has prototype and starts with "Zod")
		if (
			typeof value === 'function' &&
			value.prototype &&
			key.startsWith('Zod')
		) {
			// Skip if .db already exists (avoid double-extending)
			if (!('db' in value.prototype)) {
				Object.defineProperty(value.prototype, 'db', {
					get() {
						return createDbMethods(this as ZodTypeAny);
					},
					enumerable: false,
					configurable: true,
				});
			}
		}
	}
}

// Auto-extend the local z import so our own code works
extendZod(z);

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

/**
 * Compound foreign key reference defined at the table level.
 */
export interface CompoundReference {
	/** Local field names that form the foreign key */
	fields: string[];
	/** Referenced table */
	table: Table<any>;
	/** Referenced field names (must match fields length, defaults to referenced table's fields) */
	referencedFields?: string[];
	/** Property name for the resolved reference */
	as: string;
	/** Delete behavior */
	onDelete?: "cascade" | "set null" | "restrict";
}

export interface TableOptions {
	/** Compound indexes */
	indexes?: string[][];
	/** Compound unique constraints */
	unique?: string[][];
	/** Compound foreign key references */
	references?: CompoundReference[];
	/**
	 * Derived views - client-side transformations of already-fetched data.
	 *
	 * Derived properties:
	 * - Are NOT stored in the database
	 * - Are NOT part of TypeScript type inference
	 * - Are lazy getters (computed on access)
	 * - Are non-enumerable (don't appear in Object.keys() or JSON.stringify())
	 * - Must be pure functions (no I/O, no side effects)
	 * - Only transform data already in the entity
	 *
	 * @example
	 * table("posts", schema, {
	 *   derive: {
	 *     tags: (post) => post.postTags?.map(pt => pt.tag) ?? []
	 *   }
	 * });
	 *
	 * // Usage:
	 * post.tags  // ✅ Returns array of tags (lazy getter)
	 * JSON.stringify(post)  // ✅ Doesn't include tags (non-enumerable)
	 * {...post, tags: post.tags}  // ✅ Explicit when needed
	 */
	derive?: Record<string, (entity: any) => any>;
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
	reverseAs?: string;
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
	readonly unique: string[][];
	readonly compoundReferences: CompoundReference[];

	// Pre-extracted metadata (no Zod walking needed)
	readonly _meta: {
		primary: string | null;
		unique: string[];
		indexed: string[];
		softDeleteField: string | null;
		references: ReferenceInfo[];
		fields: Record<string, FieldDbMeta>;
		/** Derived property functions (non-enumerable getters on entities) */
		derive?: Record<string, (entity: any) => any>;
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
	 * **Dialect-aware**: Returns an abstract DDL fragment that gets transformed
	 * to dialect-specific SQL when passed through `db.exec()`. The driver's
	 * dialect is automatically used - no need to specify it manually.
	 *
	 * @param options - DDL generation options (ifNotExists)
	 * @returns DDL fragment (transformed to SQL based on driver dialect)
	 *
	 * @example
	 * // In migrations - dialect automatically detected from driver
	 * await db.exec`${Posts.ddl()}`;
	 *
	 * // With options
	 * await db.exec`${Posts.ddl({ ifNotExists: true })}`;
	 */
	ddl(options?: {ifNotExists?: boolean}): DDLFragment;

	/**
	 * Generate idempotent ALTER TABLE statement to add a column if it doesn't exist.
	 * Reads column definition from table schema.
	 *
	 * **Idempotent**: Safe to run multiple times on SQLite/PostgreSQL (uses IF NOT EXISTS).
	 *
	 * **MySQL limitation**: MySQL does NOT support IF NOT EXISTS for ALTER TABLE ADD COLUMN.
	 * On MySQL, this will error if the column already exists. Wrap in try/catch or check
	 * column existence first if re-running migrations.
	 *
	 * **Type compatibility**: If the column already exists with a different type,
	 * behavior depends on the database (may fail or be silently ignored). For type
	 * changes, use a multi-step migration: add new column, copy data, drop old column.
	 *
	 * **Pure function**: Generates SQL from schema without inspecting the database.
	 *
	 * **Dialect-aware**: Returns an abstract DDL fragment that gets transformed
	 * to dialect-specific SQL when passed through `db.exec()`. Dialect is resolved
	 * at execution time, not creation time.
	 *
	 * @param fieldName - Name of field from table schema
	 * @returns DDL fragment (transformed to SQL based on driver dialect)
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
	 * // → SQLite/PostgreSQL: ALTER TABLE posts ADD COLUMN IF NOT EXISTS views INTEGER DEFAULT 0
	 * // → MySQL: ALTER TABLE posts ADD COLUMN views INTEGER DEFAULT 0 (no IF NOT EXISTS)
	 */
	ensureColumn(fieldName: keyof z.infer<ZodObject<T>> & string): DDLFragment;

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
	 * **Dialect-aware**: Returns an abstract DDL fragment that gets transformed
	 * to dialect-specific SQL when passed through `db.exec()`.
	 *
	 * @param fields - Array of field names to index
	 * @param options - Index name
	 * @returns DDL fragment (transformed to SQL based on driver dialect)
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
	ensureIndex(fields: (keyof z.infer<ZodObject<T>> & string)[], options?: {name?: string}): DDLFragment;

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
	 * **Dialect-aware**: Returns an abstract DDL fragment that gets transformed
	 * to dialect-specific SQL when passed through `db.exec()`.
	 *
	 * Useful for safe column renames in migrations:
	 * 1. Add new column to schema
	 * 2. copyColumn() to migrate data
	 * 3. Drop old column in future migration (manual)
	 *
	 * @param fromField - Source column name (may not exist in current schema)
	 * @param toField - Destination field from table schema
	 * @returns DDL fragment (transformed to SQL based on driver dialect)
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
	copyColumn(fromField: string, toField: keyof z.infer<ZodObject<T>> & string): DDLFragment;

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

			// Validate 'as' doesn't collide with existing fields in THIS table
			if (ref.as in shape) {
				throw new TableDefinitionError(
					`Table "${name}": reference property "${ref.as}" (from field "${key}") collides with existing schema field. Choose a different 'as' name.`,
					name,
					key,
				);
			}

			// Validate 'reverseAs' doesn't collide with existing fields in TARGET table
			if (ref.reverseAs) {
				const targetShape = ref.table.schema.shape;
				if (ref.reverseAs in targetShape) {
					throw new TableDefinitionError(
						`Table "${name}": reverse reference property "${ref.reverseAs}" (from field "${key}") collides with existing field in target table "${ref.table.name}". Choose a different 'reverseAs' name.`,
						name,
						key,
					);
				}
			}

			meta.references.push({
				fieldName: key,
				table: ref.table,
				referencedField: ref.field ?? ref.table._meta.primary ?? "id",
				as: ref.as,
				reverseAs: ref.reverseAs,
				onDelete: ref.onDelete,
			});
			dbMeta.reference = ref;
		}
		if (fieldDbMeta.encode) {
			dbMeta.encode = fieldDbMeta.encode;
		}
		if (fieldDbMeta.decode) {
			dbMeta.decode = fieldDbMeta.decode;
		}
		if (fieldDbMeta.columnType) {
			dbMeta.columnType = fieldDbMeta.columnType;
		}

		meta.fields[key] = dbMeta;
	}

	const schema = z.object(zodShape as any);

	return createTableObject(name, schema, zodShape, meta, options);
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
	options: TableOptions,
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

	// Validate derived properties don't collide with schema fields
	if (options.derive) {
		for (const key of Object.keys(options.derive)) {
			if (key in zodShape) {
				throw new TableDefinitionError(
					`Table "${name}": derived property "${key}" collides with existing schema field. Choose a different name.`,
					name,
				);
			}
		}
	}

	const table: Table<any> = {
		[TABLE_MARKER]: true,
		name,
		schema,
		indexes: options.indexes ?? [],
		unique: options.unique ?? [],
		compoundReferences: options.references ?? [],
		_meta: {...meta, derive: options.derive},
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
			const pickedIndexes = (options.indexes ?? []).filter((idx) =>
				idx.every((f) => fieldSet.has(f)),
			);

			// Filter unique constraints to only those with all fields present
			const pickedUnique = (options.unique ?? []).filter((u) =>
				u.every((f) => fieldSet.has(f)),
			);

			// Filter compound references to only those with all fields present
			const pickedCompoundRefs = (options.references ?? []).filter((ref) =>
				ref.fields.every((f) => fieldSet.has(f)),
			);

			return createTableObject(
				name,
				pickedSchema,
				pickedZodShape,
				pickedMeta,
				{indexes: pickedIndexes, unique: pickedUnique, references: pickedCompoundRefs},
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

		ddl(options?: {ifNotExists?: boolean}): DDLFragment {
			return createDDLFragment("create-table", this as Table<any>, options);
		},

		ensureColumn(fieldName: string): DDLFragment {
			// Validate field exists in schema
			if (!(fieldName in zodShape)) {
				throw new Error(
					`Field "${fieldName}" does not exist in table "${name}". Available fields: ${Object.keys(zodShape).join(", ")}`
				);
			}

			return createDDLFragment("alter-table-add-column", this as Table<any>, {fieldName});
		},

		ensureIndex(fields: string[], options?: {name?: string}): DDLFragment {
			// Validate all fields exist
			for (const field of fields) {
				if (!(field in zodShape)) {
					throw new Error(
						`Field "${field}" does not exist in table "${name}". Available fields: ${Object.keys(zodShape).join(", ")}`
					);
				}
			}

			return createDDLFragment("create-index", this as Table<any>, {...options, fields});
		},

		copyColumn(fromField: string, toField: string): DDLFragment {
			// Validate toField exists in schema
			if (!(toField in zodShape)) {
				throw new Error(
					`Destination field "${toField}" does not exist in table "${name}". Available fields: ${Object.keys(zodShape).join(", ")}`
				);
			}

			// Note: fromField might not exist in current schema (it's the old column)
			return createDDLFragment("update", this as Table<any>, {fromField, toField});
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

	return table;
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

// ============================================================================
// TypeScript Declarations for .db namespace
// ============================================================================

declare module "zod" {
	interface ZodType {
		readonly db: {
			/**
			 * Mark field as primary key.
			 * @example z.string().uuid().db.primary()
			 */
			primary<T extends ZodTypeAny>(this: T): T;

			/**
			 * Mark field as unique.
			 * @example z.string().email().db.unique()
			 */
			unique<T extends ZodTypeAny>(this: T): T;

			/**
			 * Create an index on this field.
			 * @example z.date().db.index()
			 */
			index<T extends ZodTypeAny>(this: T): T;

			/**
			 * Mark field as soft delete timestamp.
			 * @example z.date().nullable().default(null).db.softDelete()
			 */
			softDelete<T extends ZodTypeAny>(this: T): T;

			/**
			 * Define a foreign key reference with optional reverse relationship.
			 *
			 * @example
			 * // Forward reference only
			 * authorId: z.string().uuid().db.references(Users, {as: "author"})
			 *
			 * @example
			 * // With reverse relationship
			 * authorId: z.string().uuid().db.references(Users, {
			 *   as: "author",      // post.author = User
			 *   reverseAs: "posts" // user.posts = Post[]
			 * })
			 */
			references<T extends ZodTypeAny>(this: T, table: Table<any>, options: {
				field?: string;
				as: string;
				reverseAs?: string;
				onDelete?: "cascade" | "set null" | "restrict";
			}): T;

			/**
			 * Encode app values to DB values (for INSERT/UPDATE).
			 * One-way transformation is fine (e.g., password hashing).
			 *
			 * @example
			 * password: z.string().db.encode(hashPassword)
			 *
			 * @example
			 * // Bidirectional: pair with .db.decode()
			 * status: z.enum(["pending", "active"])
			 *   .db.encode(s => statusMap.indexOf(s))
			 *   .db.decode(i => statusMap[i])
			 */
			serialize<T extends ZodTypeAny, TDB>(this: T, fn: (app: z.infer<T>) => TDB): T;

			/**
			 * Shorthand for JSON encoding/decoding.
			 * Equivalent to .transform(JSON.parse).db.serialize(JSON.stringify)
			 *
			 * @example
			 * metadata: z.object({theme: z.string()}).db.json()
			 */
			json<T extends ZodTypeAny>(this: T): ZodTypeAny;

			/**
			 * Shorthand for CSV encoding/decoding of string arrays.
			 * Equivalent to .transform(s => s.split(",")).db.serialize(arr => arr.join(","))
			 *
			 * @example
			 * tags: z.array(z.string()).db.csv()
			 */
			csv<T extends ZodTypeAny>(this: T): ZodTypeAny;

			/**
			 * Specify explicit column type for DDL generation.
			 * Required when using custom encode/decode on objects/arrays
			 * that transform to a different storage type.
			 *
			 * @example
			 * // Store array as CSV instead of JSON
			 * tags: z.array(z.string())
			 *   .db.encode((arr) => arr.join(","))
			 *   .db.decode((str) => str.split(","))
			 *   .db.type("TEXT")
			 */
			type<T extends ZodTypeAny>(this: T, columnType: string): T;
		};
	}
}
