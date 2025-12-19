/**
 * Table definition with wrapper-based field extensions.
 *
 * Uses wrapper functions instead of .pipe() to avoid Zod internals.
 * Metadata is extracted once at table() call time.
 */

import {z, ZodType, ZodObject, ZodRawShape} from "zod";

import {TableDefinitionError} from "./errors.js";
import {isSQLBuiltin, type SQLBuiltin} from "./database.js";
import {
	ident,
	makeTemplate,
	createTemplate,
	isSQLTemplate,
	type SQLTemplate,
} from "./template.js";
import {ValidationError} from "./errors.js";

// ============================================================================
// Identifier Validation
// ============================================================================

/**
 * Validate that an identifier (table or column name) is safe for SQL.
 * Throws an error if the identifier contains dangerous characters.
 *
 * @param name - The identifier to validate
 * @param type - "table" or "column" for error messages
 * @throws Error if identifier contains control characters, semicolons, etc.
 */
function validateIdentifier(name: string, type: "table" | "column"): void {
	// Check for control characters (ASCII 0-31 and 127)
	// These include: null byte (\x00), newline (\n), carriage return (\r), tab (\t), etc.
	// eslint-disable-next-line no-control-regex
	const controlCharRegex = /[\x00-\x1f\x7f]/;
	if (controlCharRegex.test(name)) {
		throw new TableDefinitionError(
			// eslint-disable-next-line no-control-regex
			`Invalid ${type} identifier "${name.replace(/[\x00-\x1f\x7f]/g, "\\x")}"` +
				`: ${type} names cannot contain control characters`,
		);
	}

	// Check for semicolons (SQL statement separator)
	if (name.includes(";")) {
		throw new TableDefinitionError(
			`Invalid ${type} identifier "${name}": ${type} names cannot contain semicolons`,
		);
	}

	// Check for backticks (could interfere with MySQL quoting)
	if (name.includes("`")) {
		throw new TableDefinitionError(
			`Invalid ${type} identifier "${name}": ${type} names cannot contain backticks`,
		);
	}
}

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
export function getDBMeta(schema: ZodType): Record<string, any> {
	try {
		const meta =
			typeof (schema as any).meta === "function" ? (schema as any).meta() : {};
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
export function setDBMeta<T extends ZodType>(
	schema: T,
	dbMeta: Record<string, any>,
): T {
	const existing =
		(typeof (schema as any).meta === "function"
			? (schema as any).meta()
			: undefined) ?? {};
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

export interface FieldDBMeta {
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
	/** Auto-increment flag */
	autoIncrement?: boolean;
	/** Value to apply on INSERT */
	inserted?: {
		type: "sql" | "symbol" | "function";
		/** SQL template (for type: "sql") */
		template?: SQLTemplate;
		symbol?: SQLBuiltin;
		fn?: () => unknown;
	};
	/** Value to apply on UPDATE only */
	updated?: {
		type: "sql" | "symbol" | "function";
		/** SQL template (for type: "sql") */
		template?: SQLTemplate;
		symbol?: SQLBuiltin;
		fn?: () => unknown;
	};
	/** Value to apply on both INSERT and UPDATE */
	upserted?: {
		type: "sql" | "symbol" | "function";
		/** SQL template (for type: "sql") */
		template?: SQLTemplate;
		symbol?: SQLBuiltin;
		fn?: () => unknown;
	};
}

// ============================================================================
// .db namespace - Fluent API for database metadata
// ============================================================================

/**
 * Check if a value is a TemplateStringsArray (for tagged template detection).
 */
function isTemplateStringsArray(value: unknown): value is TemplateStringsArray {
	return Array.isArray(value) && "raw" in value;
}

/**
 * Merge a SQLTemplate into strings/values arrays, maintaining the template invariant.
 * Fragment templates merge naturally without string parsing.
 *
 * @param strings - Mutable string array to append to
 * @param values - Mutable values array to append to
 * @param template - SQLTemplate tuple [strings, values]
 * @param suffix - String to append after the template
 */
function mergeFragment(
	strings: string[],
	values: unknown[],
	template: SQLTemplate,
	suffix: string,
): void {
	const templateStrings = template[0];
	const templateValues = template.slice(1);
	// Append templateStrings[0] to last string
	strings[strings.length - 1] += templateStrings[0];

	// Push remaining template strings and all template values
	for (let j = 1; j < templateStrings.length; j++) {
		strings.push(templateStrings[j]);
	}
	values.push(...templateValues);

	// Append suffix
	strings[strings.length - 1] += suffix;
}

/**
 * Check if a schema uses Zod's .default() wrapper.
 */
function hasZodDefault(schema: ZodType): boolean {
	return typeof (schema as any).removeDefault === "function";
}

/**
 * Create the .db methods object that will be added to all Zod schemas.
 * This is shared across all types and bound to the schema instance.
 */
function createDBMethods(schema: ZodType) {
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
		references(
			table: Table<any>,
			options: {
				field?: string;
				as: string;
				reverseAs?: string;
				onDelete?: "cascade" | "set null" | "restrict";
			},
		) {
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
		 * Set a value to apply on INSERT.
		 *
		 * Three forms:
		 * - Tagged template: .db.inserted`CURRENT_TIMESTAMP` → raw SQL
		 * - Symbol: .db.inserted(NOW) → dialect-aware SQL
		 * - Function: .db.inserted(() => "draft") → client-side per-insert
		 *
		 * Field becomes optional for insert.
		 *
		 * **Note:** SQL expressions (tagged templates and symbols) bypass encode/decode
		 * since they're executed by the database, not the application. Use function
		 * form if you need encoding applied.
		 *
		 * **Note:** Interpolated values in tagged templates are parameterized but not
		 * schema-validated. Ensure values are appropriate for the column type.
		 *
		 * @example
		 * createdAt: z.date().db.inserted(NOW)
		 * token: z.string().db.inserted(() => crypto.randomUUID())
		 * slug: z.string().db.inserted`LOWER(name)`
		 */
		inserted(
			stringsOrValue: TemplateStringsArray | SQLBuiltin | (() => unknown),
			...templateValues: unknown[]
		) {
			let insertedMeta: FieldDBMeta["inserted"];

			if (isTemplateStringsArray(stringsOrValue)) {
				// Tagged template → SQL expression
				// Build template by flattening any SQL fragments while preserving structure
				const strings: string[] = [];
				const values: unknown[] = [];

				for (let i = 0; i < stringsOrValue.length; i++) {
					if (i === 0) {
						strings.push(stringsOrValue[i]);
					}

					if (i < templateValues.length) {
						const value = templateValues[i];
						// Check if it's an SQL template (from cols proxy or other templates)
						if (isSQLTemplate(value)) {
							// Merge template to maintain invariant
							mergeFragment(strings, values, value, stringsOrValue[i + 1]);
						} else {
							// Regular value - add to values array
							values.push(value);
							strings.push(stringsOrValue[i + 1]);
						}
					}
				}

				insertedMeta = {
					type: "sql",
					template: createTemplate(makeTemplate(strings), values),
				};
			} else if (isSQLBuiltin(stringsOrValue)) {
				insertedMeta = {type: "symbol", symbol: stringsOrValue};
			} else if (typeof stringsOrValue === "function") {
				insertedMeta = {type: "function", fn: stringsOrValue};
			} else {
				throw new TableDefinitionError(
					`inserted() requires a tagged template, symbol (NOW), or function. Got: ${typeof stringsOrValue}`,
				);
			}

			// Validate: inserted cannot be combined with encode/decode
			const existing = getDBMeta(schema);
			if (existing.encode || existing.decode) {
				throw new TableDefinitionError(
					`inserted() cannot be combined with encode() or decode(). ` +
						`DB expressions and functions bypass encoding/decoding.`,
				);
			}

			// Make schema optional for input type, preserving existing db metadata
			const optionalSchema = schema.optional();
			return setDBMeta(optionalSchema, {...existing, inserted: insertedMeta});
		},

		/**
		 * Set a value to apply on UPDATE only.
		 *
		 * Same forms as inserted(). See inserted() for notes on codec bypass
		 * and template parameter validation.
		 *
		 * Field becomes optional for update operations.
		 *
		 * @example
		 * modifiedAt: z.date().db.updated(NOW)
		 * lastModified: z.date().db.updated(() => new Date())
		 */
		updated(
			stringsOrValue: TemplateStringsArray | SQLBuiltin | (() => unknown),
			...templateValues: unknown[]
		) {
			let updatedMeta: FieldDBMeta["updated"];

			if (isTemplateStringsArray(stringsOrValue)) {
				// Tagged template → SQL expression
				// Build template by flattening any SQL fragments while preserving structure
				const strings: string[] = [];
				const values: unknown[] = [];

				for (let i = 0; i < stringsOrValue.length; i++) {
					if (i === 0) {
						strings.push(stringsOrValue[i]);
					}

					if (i < templateValues.length) {
						const value = templateValues[i];
						// Check if it's an SQL template (from cols proxy or other templates)
						if (isSQLTemplate(value)) {
							// Merge template to maintain invariant
							mergeFragment(strings, values, value, stringsOrValue[i + 1]);
						} else {
							// Regular value - add to values array
							values.push(value);
							strings.push(stringsOrValue[i + 1]);
						}
					}
				}

				updatedMeta = {
					type: "sql",
					template: createTemplate(makeTemplate(strings), values),
				};
			} else if (isSQLBuiltin(stringsOrValue)) {
				updatedMeta = {type: "symbol", symbol: stringsOrValue};
			} else if (typeof stringsOrValue === "function") {
				updatedMeta = {type: "function", fn: stringsOrValue};
			} else {
				throw new TableDefinitionError(
					`updated() requires a tagged template, symbol (NOW), or function. Got: ${typeof stringsOrValue}`,
				);
			}

			// Validate: updated cannot be combined with encode/decode
			const existing = getDBMeta(schema);
			if (existing.encode || existing.decode) {
				throw new TableDefinitionError(
					`updated() cannot be combined with encode() or decode(). ` +
						`DB expressions and functions bypass encoding/decoding.`,
				);
			}

			// Make schema optional for input type, preserving existing db metadata
			const optionalSchema = schema.optional();
			return setDBMeta(optionalSchema, {...existing, updated: updatedMeta});
		},

		/**
		 * Set a value to apply on both INSERT and UPDATE.
		 *
		 * Same forms as inserted(). See inserted() for notes on codec bypass
		 * and template parameter validation.
		 *
		 * Field becomes optional for insert/update.
		 *
		 * @example
		 * updatedAt: z.date().db.upserted(NOW)
		 * lastModified: z.date().db.upserted(() => new Date())
		 */
		upserted(
			stringsOrValue: TemplateStringsArray | SQLBuiltin | (() => unknown),
			...templateValues: unknown[]
		) {
			let upsertedMeta: FieldDBMeta["upserted"];

			if (isTemplateStringsArray(stringsOrValue)) {
				// Tagged template → SQL expression
				// Build template by flattening any SQL fragments while preserving structure
				const strings: string[] = [];
				const values: unknown[] = [];

				for (let i = 0; i < stringsOrValue.length; i++) {
					if (i === 0) {
						strings.push(stringsOrValue[i]);
					}

					if (i < templateValues.length) {
						const value = templateValues[i];
						// Check if it's an SQL template (from cols proxy or other templates)
						if (isSQLTemplate(value)) {
							// Merge template to maintain invariant
							mergeFragment(strings, values, value, stringsOrValue[i + 1]);
						} else {
							// Regular value - add to values array
							values.push(value);
							strings.push(stringsOrValue[i + 1]);
						}
					}
				}

				upsertedMeta = {
					type: "sql",
					template: createTemplate(makeTemplate(strings), values),
				};
			} else if (isSQLBuiltin(stringsOrValue)) {
				upsertedMeta = {type: "symbol", symbol: stringsOrValue};
			} else if (typeof stringsOrValue === "function") {
				upsertedMeta = {type: "function", fn: stringsOrValue};
			} else {
				throw new TableDefinitionError(
					`upserted() requires a tagged template, symbol (NOW), or function. Got: ${typeof stringsOrValue}`,
				);
			}

			// Validate: upserted cannot be combined with encode/decode
			const existing = getDBMeta(schema);
			if (existing.encode || existing.decode) {
				throw new TableDefinitionError(
					`upserted() cannot be combined with encode() or decode(). ` +
						`DB expressions and functions bypass encoding/decoding.`,
				);
			}

			// Make schema optional for input type, preserving existing db metadata
			const optionalSchema = schema.optional();
			return setDBMeta(optionalSchema, {...existing, upserted: upsertedMeta});
		},

		/**
		 * Mark this field as auto-incrementing.
		 *
		 * Field becomes optional for insert - the database generates the value.
		 * Typically used for integer primary keys.
		 *
		 * @example
		 * id: z.number().db.primary().db.autoIncrement()
		 */
		autoIncrement() {
			// Preserve existing db metadata when wrapping with optional
			const existing = getDBMeta(schema);
			const optionalSchema = schema.optional();
			return setDBMeta(optionalSchema, {...existing, autoIncrement: true});
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
 * import {extendZod, table} from "@b9g/zen";
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
			typeof value === "function" &&
			value.prototype &&
			key.startsWith("Zod")
		) {
			// Skip if .db already exists (avoid double-extending)
			if (!("db" in value.prototype)) {
				Object.defineProperty(value.prototype, "db", {
					get() {
						return createDBMethods(this as ZodType);
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
const TABLE_MARKER = Symbol.for("@b9g/zen:table");

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

/**
 * Metadata for SQL-derived expressions created via Table.derive().
 */
export interface DerivedExpr {
	/** The field name for this derived column */
	fieldName: string;
	/** SQL template for the expression */
	template: SQLTemplate;
}

// ============================================================================
// Fragment Method Types
// ============================================================================

/**
 * Set values for updates - plain values only.
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
	"meta"
> {
	readonly meta: Table<T>["meta"] & {isPartial: true};
}

/**
 * A table with SQL-derived columns created via derive().
 * Can be used for queries but not for insert/update.
 */
export interface DerivedTable<T extends ZodRawShape = ZodRawShape> extends Omit<
	Table<T>,
	"meta"
> {
	readonly meta: Table<T>["meta"] & {
		isDerived: true;
		derivedExprs: DerivedExpr[];
		derivedFields: string[];
	};
}

export interface Table<T extends ZodRawShape = ZodRawShape> {
	readonly [TABLE_MARKER]: true;
	readonly name: string;
	readonly schema: ZodObject<T>;
	readonly indexes: string[][];
	readonly unique: string[][];
	readonly compoundReferences: CompoundReference[];

	// Pre-extracted metadata (no Zod walking needed)
	readonly meta: {
		primary: string | null;
		unique: string[];
		indexed: string[];
		softDeleteField: string | null;
		references: ReferenceInfo[];
		fields: Record<string, FieldDBMeta>;
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
	readonly primary: SQLTemplate | null;

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
	deleted(): SQLTemplate;

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
	): SQLTemplate;

	/**
	 * Create a partial view of this table with only the specified fields.
	 *
	 * Useful for partial selects - the returned table-like object can be
	 * passed to all(), get(), etc. Cannot be used with insert().
	 *
	 * @example
	 * const PostSummary = Posts.pick('id', 'title', 'authorId');
	 * db.all(PostSummary, Users.pick('id', 'name'))`...`
	 */
	pick<K extends keyof z.infer<ZodObject<T>>>(
		...fields: K[]
	): PartialTable<Pick<T, K & keyof T>>;

	/**
	 * Create a new table with SQL-computed derived columns.
	 *
	 * Returns a tagged template function that parses the SQL expression.
	 * Derived columns are computed in the SELECT clause by the database.
	 * The AS aliases must match the schema field names.
	 *
	 * The returned table cannot be used for insert/update - SELECT only.
	 *
	 * @param derivedSchema - Zod schema for the derived fields
	 * @returns Tagged template function that returns a DerivedTable
	 *
	 * @example
	 * const PostsWithStats = Posts
	 *   .derive('likeCount', z.number())`COUNT(DISTINCT ${Likes.cols.id})`
	 *   .derive('commentCount', z.number())`COUNT(DISTINCT ${Comments.cols.id})`;
	 *
	 * db.all(PostsWithStats, Likes, Comments)`
	 *   LEFT JOIN likes ON ${Likes.cols.postId} = ${Posts.cols.id}
	 *   LEFT JOIN comments ON ${Comments.cols.postId} = ${Posts.cols.id}
	 *   GROUP BY ${Posts.primary}
	 * `
	 */
	derive<N extends string, V extends z.ZodType>(
		fieldName: N,
		fieldType: V,
	): (
		strings: TemplateStringsArray,
		...values: unknown[]
	) => DerivedTable<T & {[K in N]: V}>;

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
		[K in keyof z.infer<ZodObject<T>>]: SQLTemplate;
	};

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
	set(values: SetValues<Table<T>>): SQLTemplate;

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
	on(field: keyof z.infer<ZodObject<T>> & string): SQLTemplate;

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
	values(rows: Partial<z.infer<ZodObject<T>>>[]): SQLTemplate;
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
export function table<T extends Record<string, ZodType>>(
	name: string,
	shape: T,
	options: TableOptions = {},
): Table<any> {
	// Validate table name for dangerous characters
	validateIdentifier(name, "table");

	// Validate table name doesn't contain dots (would break normalization)
	if (name.includes(".")) {
		throw new TableDefinitionError(
			`Invalid table name "${name}": table names cannot contain "." as it conflicts with normalization prefixes`,
			name,
		);
	}

	// Extract Zod schemas and metadata from .meta()
	const zodShape: Record<string, ZodType> = {};
	const meta = {
		primary: null as string | null,
		unique: [] as string[],
		indexed: [] as string[],
		softDeleteField: null as string | null,
		references: [] as ReferenceInfo[],
		fields: {} as Record<string, FieldDBMeta>,
	};

	for (const [key, value] of Object.entries(shape)) {
		// Validate field name for dangerous characters
		validateIdentifier(key, "column");

		// Validate field names don't contain dots (would break normalization)
		if (key.includes(".")) {
			throw new TableDefinitionError(
				`Invalid field name "${key}" in table "${name}": field names cannot contain "." as it conflicts with normalization prefixes`,
				name,
				key,
			);
		}

		const fieldSchema = value as ZodType;
		zodShape[key] = fieldSchema;

		// Check for Zod .default() - this is a footgun, should use .db.inserted()/.db.updated()
		if (hasZodDefault(fieldSchema)) {
			throw new TableDefinitionError(
				`Field "${key}" uses Zod .default() which applies at parse time, not write time. ` +
					`Use .db.inserted() or .db.updated() instead.`,
				name,
				key,
			);
		}

		// Read database metadata from namespaced .meta()
		const fieldDBMeta = getDBMeta(fieldSchema);
		const dbMeta: FieldDBMeta = {};

		if (fieldDBMeta.primary) {
			if (meta.primary !== null) {
				throw new TableDefinitionError(
					`Table "${name}" has multiple primary keys: "${meta.primary}" and "${key}". Only one primary key is allowed.`,
					name,
				);
			}
			meta.primary = key;
			dbMeta.primaryKey = true;
		}
		if (fieldDBMeta.unique) {
			meta.unique.push(key);
			dbMeta.unique = true;
		}
		if (fieldDBMeta.indexed) {
			meta.indexed.push(key);
			dbMeta.indexed = true;
		}
		if (fieldDBMeta.softDelete) {
			if (meta.softDeleteField !== null) {
				throw new TableDefinitionError(
					`Table "${name}" has multiple soft delete fields: "${meta.softDeleteField}" and "${key}". Only one soft delete field is allowed.`,
					name,
				);
			}
			meta.softDeleteField = key;
			dbMeta.softDelete = true;
		}
		if (fieldDBMeta.reference) {
			const ref = fieldDBMeta.reference;

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
				referencedField: ref.field ?? ref.table.meta.primary ?? "id",
				as: ref.as,
				reverseAs: ref.reverseAs,
				onDelete: ref.onDelete,
			});
			dbMeta.reference = ref;
		}
		if (fieldDBMeta.encode) {
			dbMeta.encode = fieldDBMeta.encode;
		}
		if (fieldDBMeta.decode) {
			dbMeta.decode = fieldDBMeta.decode;
		}
		if (fieldDBMeta.columnType) {
			dbMeta.columnType = fieldDBMeta.columnType;
		}
		if (fieldDBMeta.inserted) {
			dbMeta.inserted = fieldDBMeta.inserted;
		}
		if (fieldDBMeta.updated) {
			dbMeta.updated = fieldDBMeta.updated;
		}
		if (fieldDBMeta.upserted) {
			dbMeta.upserted = fieldDBMeta.upserted;
		}
		if (fieldDBMeta.autoIncrement) {
			dbMeta.autoIncrement = fieldDBMeta.autoIncrement;
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
 * Create a column fragment proxy for Table.cols access.
 */
function createColsProxy(
	tableName: string,
	zodShape: Record<string, ZodType>,
): Record<string, SQLTemplate> {
	return new Proxy({} as Record<string, SQLTemplate>, {
		get(_target, prop: string): SQLTemplate | undefined {
			if (prop in zodShape) {
				return createTemplate(makeTemplate(["", ".", ""]), [
					ident(tableName),
					ident(prop),
				]);
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
	zodShape: Record<string, ZodType>,
	meta: {
		primary: string | null;
		unique: string[];
		indexed: string[];
		softDeleteField: string | null;
		references: ReferenceInfo[];
		fields: Record<string, FieldDBMeta>;
	},
	options: TableOptions,
): Table<any> {
	const cols = createColsProxy(name, zodShape);

	// Create primary key fragment
	const primary: SQLTemplate | null = meta.primary
		? createTemplate(makeTemplate(["", ".", ""]), [
				ident(name),
				ident(meta.primary),
			])
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
		meta: {...meta, derive: options.derive},
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

		deleted(): SQLTemplate {
			const softDeleteField = meta.softDeleteField;
			if (!softDeleteField) {
				throw new Error(
					`Table "${name}" does not have a soft delete field. Use softDelete() wrapper to mark a field.`,
				);
			}
			return createTemplate(makeTemplate(["", ".", " IS NOT NULL"]), [
				ident(name),
				ident(softDeleteField),
			]);
		},

		in(field: string, values: unknown[]): SQLTemplate {
			// Validate field exists
			if (!(field in zodShape)) {
				throw new Error(
					`Field "${field}" does not exist in table "${name}". Available fields: ${Object.keys(zodShape).join(", ")}`,
				);
			}

			// Handle empty array - return always-false condition
			if (values.length === 0) {
				return createTemplate(makeTemplate(["1 = 0"]), []);
			}

			// PostgreSQL has a limit of 32767 parameters per query
			// Validate this limit to prevent runtime errors
			const POSTGRESQL_PARAM_LIMIT = 32767;
			if (values.length > POSTGRESQL_PARAM_LIMIT) {
				throw new Error(
					`Too many values in IN clause: ${values.length} exceeds PostgreSQL's parameter limit of ${POSTGRESQL_PARAM_LIMIT}. ` +
						`Consider using a temporary table or splitting the query.`,
				);
			}

			// Build template: table.field IN (val1, val2, ...)
			// strings: ["", ".", " IN (", ", ", ..., ")"]
			// values: [ident(name), ident(field), val1, val2, ...]
			const strings: string[] = ["", ".", " IN ("];
			const templateValues: unknown[] = [ident(name), ident(field)];

			for (let i = 0; i < values.length; i++) {
				templateValues.push(values[i]);
				strings.push(i < values.length - 1 ? ", " : ")");
			}

			return createTemplate(makeTemplate(strings), templateValues);
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
			const pickedZodShape: Record<string, ZodType> = {};
			for (const f of fields as string[]) {
				if (f in zodShape) {
					pickedZodShape[f] = zodShape[f];
				}
			}

			// Filter metadata
			const existingDerivedExprs: DerivedExpr[] =
				(meta as any).derivedExprs ?? [];
			const existingDerivedFields: string[] = (meta as any).derivedFields ?? [];

			// Filter derived expressions to only those for picked fields
			const pickedDerivedExprs = existingDerivedExprs.filter((expr) =>
				fieldSet.has(expr.fieldName),
			);
			const pickedDerivedFields = existingDerivedFields.filter((f) =>
				fieldSet.has(f),
			);

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
				) as Record<string, FieldDBMeta>,
				isPartial: true as const,
				isDerived: undefined as true | undefined,
				derivedExprs: undefined as DerivedExpr[] | undefined,
				derivedFields: undefined as string[] | undefined,
			};

			// Preserve derived metadata if any derived fields are picked
			if (pickedDerivedExprs.length > 0) {
				pickedMeta.isDerived = true;
				pickedMeta.derivedExprs = pickedDerivedExprs;
				pickedMeta.derivedFields = pickedDerivedFields;
			}

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

			return createTableObject(name, pickedSchema, pickedZodShape, pickedMeta, {
				indexes: pickedIndexes,
				unique: pickedUnique,
				references: pickedCompoundRefs,
			}) as PartialTable<any>;
		},

		derive<N extends string, V extends z.ZodType>(fieldName: N, fieldType: V) {
			return (
				stringsOrValue: TemplateStringsArray,
				...templateValues: unknown[]
			) => {
				// Build template by flattening any SQL fragments
				const strings: string[] = [];
				const values: unknown[] = [];

				for (let i = 0; i < stringsOrValue.length; i++) {
					if (i === 0) {
						strings.push(stringsOrValue[i]);
					}

					if (i < templateValues.length) {
						const value = templateValues[i];
						// Check if it's an SQL template (from cols proxy or other templates)
						if (isSQLTemplate(value)) {
							// Merge template to maintain invariant
							mergeFragment(strings, values, value, stringsOrValue[i + 1]);
						} else {
							// Regular value - add to values array
							values.push(value);
							strings.push(stringsOrValue[i + 1]);
						}
					}
				}

				// Trim leading/trailing whitespace from first and last strings
				if (strings.length > 0) {
					strings[0] = strings[0].trimStart();
					strings[strings.length - 1] = strings[strings.length - 1].trimEnd();
				}

				const derivedExpr: DerivedExpr = {
					fieldName,
					template: createTemplate(makeTemplate(strings), values),
				};

				// Extend schema with new field
				const mergedSchema = schema.extend({[fieldName]: fieldType} as any);
				const mergedZodShape = {...zodShape, [fieldName]: fieldType};

				// Accumulate expressions (supports composition: A.derive(...).derive(...))
				const existingExprs: DerivedExpr[] = (meta as any).derivedExprs ?? [];
				const existingDerivedFields: string[] =
					(meta as any).derivedFields ?? [];

				const derivedMeta = {
					...meta,
					isDerived: true as const,
					derivedExprs: [...existingExprs, derivedExpr],
					derivedFields: [...existingDerivedFields, fieldName],
					fields: {
						...meta.fields,
						[fieldName]: {},
					},
				};

				return createTableObject(
					name,
					mergedSchema,
					mergedZodShape,
					derivedMeta,
					options,
				) as DerivedTable<any>;
			};
		},

		set(values: Record<string, unknown>): SQLTemplate {
			const entries = Object.entries(values).filter(([, v]) => v !== undefined);
			if (entries.length === 0) {
				throw new Error("set() requires at least one non-undefined field");
			}

			// Build template: col1 = val1, col2 = val2, ...
			// strings: ["", " = ", ", ", " = ", ""]
			// values: [ident(col1), val1, ident(col2), val2]
			const strings: string[] = [""];
			const templateValues: unknown[] = [];

			for (let i = 0; i < entries.length; i++) {
				const [field, value] = entries[i];
				templateValues.push(ident(field));
				strings.push(" = ");
				templateValues.push(value);
				strings.push(i < entries.length - 1 ? ", " : "");
			}

			return createTemplate(makeTemplate(strings), templateValues);
		},

		on(field: string): SQLTemplate {
			const ref = meta.references.find((r) => r.fieldName === field);

			if (!ref) {
				throw new Error(
					`Field "${field}" is not a foreign key reference in table "${name}"`,
				);
			}

			// Build template: ref_table.ref_field = this_table.fk_field
			// strings: ["", ".", " = ", ".", ""]
			// values: [ident(ref.table.name), ident(ref.referencedField), ident(name), ident(field)]
			return createTemplate(makeTemplate(["", ".", " = ", ".", ""]), [
				ident(ref.table.name),
				ident(ref.referencedField),
				ident(name),
				ident(field),
			]);
		},

		values(rows: Record<string, unknown>[]): SQLTemplate {
			if (rows.length === 0) {
				throw new Error("values() requires at least one row");
			}

			// Infer columns from first row
			const columns = Object.keys(rows[0]);
			if (columns.length === 0) {
				throw new Error("values() requires at least one column");
			}

			// Validate that all columns exist in the schema
			const schemaKeys = Object.keys(schema.shape);
			for (const col of columns) {
				if (!schemaKeys.includes(col)) {
					throw new TableDefinitionError(
						`Column "${col}" does not exist in table schema`,
						name,
						col,
					);
				}
			}

			const partialSchema = schema.partial();
			const strings: string[] = ["("];
			const templateValues: unknown[] = [];

			// Add column identifiers: (col1, col2, col3) VALUES
			for (let i = 0; i < columns.length; i++) {
				templateValues.push(ident(columns[i]));
				strings.push(i < columns.length - 1 ? ", " : ") VALUES ");
			}

			// Add value tuples: (val1, val2, val3), (val4, val5, val6)
			for (let rowIdx = 0; rowIdx < rows.length; rowIdx++) {
				const row = rows[rowIdx];
				const validated = validateWithStandardSchema(partialSchema, row);

				strings[strings.length - 1] += "(";

				for (let colIdx = 0; colIdx < columns.length; colIdx++) {
					const col = columns[colIdx];
					if (!(col in row)) {
						throw new Error(
							`All rows must have the same columns. Row is missing column "${col}"`,
						);
					}
					templateValues.push((validated as Record<string, unknown>)[col]);
					strings.push(colIdx < columns.length - 1 ? ", " : ")");
				}

				// Add comma between rows, or empty string for last row
				if (rowIdx < rows.length - 1) {
					strings[strings.length - 1] += ", ";
				}
			}

			return createTemplate(makeTemplate(strings), templateValues);
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
	zodType: ZodType,
	dbMeta: FieldDBMeta,
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
				dbMeta.reference.field ?? dbMeta.reference.table.meta.primary ?? "id",
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
 * Type guard that evaluates to `never` for partial or derived tables.
 * Use this to prevent insert/update operations at compile time.
 *
 * @example
 * function insert<T extends Table<any>>(
 *   table: T & FullTableOnly<T>,
 *   data: Insert<T>
 * ): Promise<Infer<T>>
 */
export type FullTableOnly<T> = T extends {meta: {isPartial: true}}
	? never
	: T extends {meta: {isDerived: true}}
		? never
		: T;

/**
 * Infer the insert type (respects defaults).
 * Returns `never` for partial or derived tables to prevent insert at compile time.
 */
export type Insert<T extends Table<any>> = T extends {meta: {isPartial: true}}
	? never
	: T extends {meta: {isDerived: true}}
		? never
		: z.input<T["schema"]>;

// ============================================================================
// TypeScript Declarations for .db namespace
// ============================================================================

/**
 * Database metadata methods available on Zod schemas.
 * These methods are added via extendZod() and return the same schema type.
 */
export interface ZodDBMethods<Schema extends ZodType> {
	/**
	 * Mark field as primary key.
	 * @example z.string().uuid().db.primary()
	 */
	primary(): Schema;

	/**
	 * Mark field as unique.
	 * @example z.string().email().db.unique()
	 */
	unique(): Schema;

	/**
	 * Create an index on this field.
	 * @example z.date().db.index()
	 */
	index(): Schema;

	/**
	 * Mark field as soft delete timestamp.
	 * @example z.date().nullable().default(null).db.softDelete()
	 */
	softDelete(): Schema;

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
	references(
		table: Table<any>,
		options: {
			field?: string;
			as: string;
			reverseAs?: string;
			onDelete?: "cascade" | "set null" | "restrict";
		},
	): Schema;

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
	encode<TDB>(fn: (app: z.infer<Schema>) => TDB): Schema;

	/**
	 * Decode DB values to app values (for SELECT).
	 * One-way transformation is fine.
	 *
	 * @example
	 * legacy: z.string().db.decode(deserializeLegacyFormat)
	 */
	decode<TApp>(fn: (db: any) => TApp): Schema;

	/**
	 * Shorthand for JSON encoding/decoding.
	 * Stores the value as a JSON string in the database.
	 *
	 * @example
	 * metadata: z.object({theme: z.string()}).db.json()
	 */
	json(): Schema;

	/**
	 * Shorthand for CSV encoding/decoding of string arrays.
	 * Stores the array as a comma-separated string in the database.
	 *
	 * @example
	 * tags: z.array(z.string()).db.csv()
	 */
	csv(): Schema;

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
	type(columnType: string): Schema;

	/**
	 * Set a value to apply on INSERT.
	 *
	 * Three forms:
	 * - Tagged template: .db.inserted`CURRENT_TIMESTAMP`
	 * - Symbol: .db.inserted(NOW)
	 * - Function: .db.inserted(() => "draft")
	 *
	 * Field becomes optional for insert.
	 *
	 * **Note:** SQL expressions (tagged templates and symbols) bypass encode/decode
	 * since they're executed by the database, not the application. Use function
	 * form if you need encoding applied.
	 *
	 * **Note:** Interpolated values in tagged templates are parameterized but not
	 * schema-validated. Ensure values are appropriate for the column type.
	 *
	 * @example
	 * createdAt: z.date().db.inserted(NOW)
	 * token: z.string().db.inserted(() => crypto.randomUUID())
	 * slug: z.string().db.inserted`LOWER(name)`
	 */
	inserted(
		value: import("./database.js").SQLBuiltin | (() => z.infer<Schema>),
	): Schema;
	inserted(strings: TemplateStringsArray, ...values: unknown[]): Schema;

	/**
	 * Set a value to apply on UPDATE only.
	 *
	 * Same forms as inserted(). See inserted() for notes on codec bypass
	 * and template parameter validation.
	 *
	 * Field becomes optional for update operations.
	 *
	 * @example
	 * modifiedAt: z.date().db.updated(NOW)
	 * lastModified: z.date().db.updated(() => new Date())
	 */
	updated(
		value: import("./database.js").SQLBuiltin | (() => z.infer<Schema>),
	): Schema;
	updated(strings: TemplateStringsArray, ...values: unknown[]): Schema;

	/**
	 * Set a value to apply on both INSERT and UPDATE.
	 *
	 * Same forms as inserted(). See inserted() for notes on codec bypass
	 * and template parameter validation.
	 *
	 * Field becomes optional for insert/update.
	 *
	 * @example
	 * updatedAt: z.date().db.upserted(NOW)
	 * lastModified: z.date().db.upserted(() => new Date())
	 */
	upserted(
		value: import("./database.js").SQLBuiltin | (() => z.infer<Schema>),
	): Schema;
	upserted(strings: TemplateStringsArray, ...values: unknown[]): Schema;

	/**
	 * Mark this field as auto-incrementing.
	 *
	 * Field becomes optional for insert - the database generates the value.
	 * Typically used for integer primary keys.
	 *
	 * @example
	 * id: z.number().int().db.primary().db.autoIncrement()
	 */
	autoIncrement(): Schema;
}

declare module "zod" {
	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	interface ZodType<out Output, out Input, out Internals> {
		readonly db: ZodDBMethods<this>;
	}
}
