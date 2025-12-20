/**
 * Table definition with wrapper-based field extensions.
 *
 * Uses wrapper functions instead of .pipe() to avoid Zod internals.
 * Metadata is extracted once at table() call time.
 */

import {z, ZodType, ZodObject, ZodRawShape} from "zod";

import {TableDefinitionError} from "./errors.js";
import {isSQLBuiltin, NOW, type SQLBuiltin} from "./builtins.js";
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
 * Unwraps optional/nullable/default/catch wrappers to find the db metadata.
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
		const dbMeta = meta?.[DB_META_NAMESPACE];
		if (dbMeta && Object.keys(dbMeta).length > 0) {
			return dbMeta;
		}
		// Unwrap optional/nullable/default/catch using public Zod APIs
		if (schema instanceof z.ZodOptional || schema instanceof z.ZodNullable) {
			return getDBMeta(schema.unwrap() as ZodType);
		}
		if (schema instanceof z.ZodDefault) {
			return getDBMeta((schema as any).removeDefault() as ZodType);
		}
		if (schema instanceof z.ZodCatch) {
			return getDBMeta((schema as any).removeCatch() as ZodType);
		}
		return {};
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
 * Detect if a ZodString schema has .uuid() format.
 */
function isUuidSchema(schema: ZodType): boolean {
	return schema instanceof z.ZodString && (schema as any).format === "uuid";
}

/**
 * Detect if a ZodNumber schema has .int() modifier.
 */
function isIntSchema(schema: ZodType): boolean {
	if (!(schema instanceof z.ZodNumber)) return false;
	const checks = (schema as any)._def?.checks;
	if (!Array.isArray(checks)) return false;
	return checks.some((c: any) => c.isInt === true);
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
		 * authorId: z.string().uuid().db.references(Users, "author")
		 *
		 * @example
		 * // With options
		 * authorId: z.string().uuid().db.references(Users, "author", {
		 *   reverseAs: "posts",   // user.posts = Post[]
		 *   ondelete: "cascade",
		 * })
		 */
		references<
			RefTable extends Table<any, any>,
			As extends string,
			ReverseAs extends string | undefined = undefined,
		>(
			table: RefTable,
			as: As,
			options?: {
				field?: string;
				reverseAs?: ReverseAs;
				onDelete?: "cascade" | "set null" | "restrict";
			},
		): ZodType & {
			readonly __refTable: RefTable;
			readonly __refAs: As;
		} {
			return setDBMeta(schema, {
				reference: {
					table,
					as,
					...options,
				},
			}) as ZodType & {readonly __refTable: RefTable; readonly __refAs: As};
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
		 * Auto-generate value on insert based on field type.
		 *
		 * Type-aware behavior:
		 * - `z.string().uuid()` → generates UUID via `crypto.randomUUID()`
		 * - `z.number().int()` on primary key → auto-increment (database-side)
		 * - `z.date()` → current timestamp via NOW
		 *
		 * Field becomes optional for insert.
		 *
		 * @example
		 * id: z.string().uuid().db.primary().db.auto()
		 * // → crypto.randomUUID() on insert
		 *
		 * @example
		 * id: z.number().int().db.primary().db.auto()
		 * // → auto-increment
		 *
		 * @example
		 * createdAt: z.date().db.auto()
		 * // → NOW on insert
		 */
		auto() {
			const existing = getDBMeta(schema);
			const optionalSchema = schema.optional();

			// UUID string → crypto.randomUUID()
			if (isUuidSchema(schema)) {
				const insertedMeta = {
					type: "function" as const,
					fn: () => crypto.randomUUID(),
				};
				return setDBMeta(optionalSchema, {...existing, inserted: insertedMeta});
			}

			// Integer (typically primary key) → autoIncrement
			if (isIntSchema(schema)) {
				return setDBMeta(optionalSchema, {...existing, autoIncrement: true});
			}

			// Date → NOW
			if (schema instanceof z.ZodDate) {
				const insertedMeta = {
					type: "symbol" as const,
					symbol: NOW,
				};
				return setDBMeta(optionalSchema, {...existing, inserted: insertedMeta});
			}

			throw new Error(
				`.db.auto() is not supported for this type. ` +
					`Supported: z.string().uuid(), z.number().int(), z.date()`,
			);
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
	// Field identity
	name: string;
	type: FieldType;
	required: boolean;

	// From .db.*() methods
	primaryKey?: boolean;
	unique?: boolean;
	indexed?: boolean;
	softDelete?: boolean;
	reference?: {
		table: Table;
		field: string;
		as: string;
		onDelete?: "cascade" | "set null" | "restrict";
	};
	encode?: (value: any) => any;
	decode?: (value: any) => any;
	columnType?: string;
	autoIncrement?: boolean;
	inserted?: {
		type: "sql" | "symbol" | "function";
		template?: SQLTemplate;
		symbol?: SQLBuiltin;
		fn?: () => unknown;
	};
	updated?: {
		type: "sql" | "symbol" | "function";
		template?: SQLTemplate;
		symbol?: SQLBuiltin;
		fn?: () => unknown;
	};
	upserted?: {
		type: "sql" | "symbol" | "function";
		template?: SQLTemplate;
		symbol?: SQLBuiltin;
		fn?: () => unknown;
	};

	// From Zod schema
	default?: unknown;
	maxLength?: number;
	minLength?: number;
	min?: number;
	max?: number;
	options?: readonly string[];

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

// Symbol for internal table metadata (not part of public API)
const TABLE_META = Symbol.for("@b9g/zen:table-meta");

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

/** Internal table metadata type */
export interface TableMeta {
	primary: string | null;
	unique: string[];
	indexed: string[];
	softDeleteField: string | null;
	references: ReferenceInfo[];
	fields: Record<string, FieldDBMeta>;
	derive?: Record<string, (entity: any) => any>;
	isPartial?: boolean;
	isDerived?: boolean;
	derivedExprs?: DerivedExpr[];
	derivedFields?: string[];
}

/**
 * Get internal metadata from a Table.
 * For internal library use only - not part of public API.
 */
export function getTableMeta(table: Table<any>): TableMeta {
	return (table as any)[TABLE_META];
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

// ============================================================================
// Relationship Navigation Types
// ============================================================================

/**
 * Unwrap Zod wrapper types (optional, nullable, default, catch) at the type level.
 * Used to extract reference metadata from wrapped schemas.
 */
type UnwrapZod<T> =
	T extends z.ZodOptional<infer U>
		? UnwrapZod<U>
		: T extends z.ZodNullable<infer U>
			? UnwrapZod<U>
			: T extends z.ZodDefault<infer U>
				? UnwrapZod<U>
				: T extends z.ZodCatch<infer U>
					? UnwrapZod<U>
					: T;

/**
 * Extract relationship references from a table schema.
 * Maps relationship aliases (the "as" name) to their target tables.
 *
 * @example
 * const Posts = table("posts", {
 *   authorId: z.string().db.references(Users, "author"),
 * });
 * type PostRefs = InferRefs<typeof Posts["schema"]["shape"]>;
 * // { author: typeof Users }
 */
export type InferRefs<T extends ZodRawShape> = {
	[K in keyof T as UnwrapZod<T[K]> extends {readonly __refAs: infer As}
		? As extends string
			? As
			: never
		: never]: UnwrapZod<T[K]> extends {
		readonly __refTable: infer RefTab extends Table<any, any>;
	}
		? RefTab
		: never;
};

/**
 * Filter refs to only include those whose backing field is in the picked schema.
 * Used by pick() to correctly narrow relationship types.
 */
type FilterRefs<
	PickedShape extends ZodRawShape,
	OriginalRefs extends Record<string, Table<any, any>>,
> = {
	[Alias in keyof OriginalRefs as Alias extends keyof InferRefs<PickedShape>
		? Alias
		: never]: OriginalRefs[Alias];
};

/**
 * A relationship jump point for navigating to a referenced table's fields.
 */
export interface Relation<TargetTable extends Table<any, any>> {
	/** Navigate to the target table's fields */
	fields(): ReturnType<TargetTable["fields"]>;
	/** Direct access to the referenced table */
	readonly table: TargetTable;
}

/**
 * The return type of `table.fields()` - combines column fields with relationship navigators.
 */
export type TableFields<
	T extends ZodRawShape,
	Refs extends Record<string, Table<any, any>>,
> = {[K in keyof T]: FieldMeta} & {
	[K in keyof Refs]: Relation<Refs[K]>;
};

/**
 * A partial view of a table created via pick().
 * Can be used for queries but not for insert().
 * Check via table.meta.isPartial at runtime.
 */
export interface PartialTable<
	T extends ZodRawShape = ZodRawShape,
	Refs extends Record<string, Table<any, any>> = {},
> extends Table<T, Refs> {}

/**
 * A table with SQL-derived columns created via derive().
 * Can be used for queries but not for insert/update.
 * Check via table.meta.isDerived at runtime.
 */
export interface DerivedTable<
	T extends ZodRawShape = ZodRawShape,
	Refs extends Record<string, Table<any, any>> = {},
> extends Table<T, Refs> {}

export interface Table<
	T extends ZodRawShape = ZodRawShape,
	Refs extends Record<string, Table<any, any>> = {},
> {
	readonly [TABLE_MARKER]: true;
	/** @internal Symbol-keyed internal metadata */
	readonly [TABLE_META]: TableMeta;
	readonly name: string;
	readonly schema: ZodObject<T>;
	readonly indexes: string[][];
	readonly unique: string[][];
	readonly compoundReferences: CompoundReference[];

	/**
	 * @internal Internal table metadata. Use getTableMeta() or table methods instead.
	 */
	readonly meta: TableMeta;

	/** Get field metadata for forms/admin, with relationship navigators */
	fields(): TableFields<T, Refs>;

	/**
	 * Get the primary key field name.
	 *
	 * @returns The primary key field name, or null if no primary key is defined.
	 *
	 * @example
	 * const pk = Users.primaryKey(); // "id"
	 */
	primaryKey(): string | null;

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
	): PartialTable<Pick<T, K & keyof T>, FilterRefs<Pick<T, K & keyof T>, Refs>>;

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
	) => DerivedTable<T & {[K in N]: V}, Refs>;

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
	 * Generate the ON condition for a JOIN clause (FK equality).
	 *
	 * Called on the table being joined, with the referencing table as argument.
	 * Looks up foreign keys in the referencing table that point to this table.
	 * Returns just the equality condition; you write the JOIN clause.
	 *
	 * @param referencingTable - The table that has a foreign key to this table
	 * @param alias - Optional relationship alias (the "as" name) to disambiguate
	 *                when multiple FKs point to the same table
	 *
	 * @example
	 * // Single FK - no disambiguation needed
	 * db.all([Posts, Users])`
	 *   JOIN "users" ON ${Users.on(Posts)}
	 *   WHERE ${Posts.where({published: true})}
	 * `
	 * // → JOIN "users" ON "users"."id" = "posts"."authorId" WHERE ...
	 *
	 * @example
	 * // Multiple FKs to same table - use alias to disambiguate
	 * const Posts = table("posts", {
	 *   authorId: z.string().db.references(Users, "author"),
	 *   editorId: z.string().db.references(Users, "editor"),
	 * });
	 * db.all([Posts, Users])`
	 *   JOIN "users" AS "author" ON ${Users.on(Posts, "author")}
	 *   JOIN "users" AS "editor" ON ${Users.on(Posts, "editor")}
	 *   WHERE ...
	 * `
	 * // → JOIN "users" AS "author" ON "users"."id" = "posts"."authorId"
	 * //   JOIN "users" AS "editor" ON "users"."id" = "posts"."editorId" WHERE ...
	 */
	on(referencingTable: Table<any>, alias?: string): SQLTemplate;

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
): Table<T, InferRefs<T>> {
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

			// Validate 'as' doesn't collide with derived properties
			if (options.derive && ref.as in options.derive) {
				throw new TableDefinitionError(
					`Table "${name}": reference property "${ref.as}" (from field "${key}") collides with derived property. Choose a different 'as' name.`,
					name,
					key,
				);
			}

			// Validate 'as' doesn't collide with other reference aliases
			const existingRef = meta.references.find((r) => r.as === ref.as);
			if (existingRef) {
				throw new TableDefinitionError(
					`Table "${name}": duplicate reference alias "${ref.as}" used by fields "${existingRef.fieldName}" and "${key}". Each reference must have a unique 'as' name.`,
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
				referencedField: ref.field ?? getTableMeta(ref.table).primary ?? "id",
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

	// Combine options.derive with meta for internal storage
	const internalMeta = {...meta, derive: options.derive};

	const table: Table<any, any> = {
		[TABLE_MARKER]: true,
		[TABLE_META]: internalMeta,
		name,
		schema,
		indexes: options.indexes ?? [],
		unique: options.unique ?? [],
		compoundReferences: options.references ?? [],
		cols,
		primary,
		// Internal getter for backward compatibility - use getTableMeta() instead
		get meta() {
			return internalMeta;
		},

		fields(): TableFields<any, any> {
			const result: Record<string, FieldMeta | Relation<any>> = {};

			for (const [key, zodType] of Object.entries(zodShape)) {
				const dbMeta = meta.fields[key] || {};
				result[key] = extractFieldMeta(key, zodType, dbMeta);
			}

			// Add relationship navigators for each reference
			for (const ref of meta.references) {
				result[ref.as] = {
					fields: () => ref.table.fields(),
					table: ref.table,
				};
			}

			return result as TableFields<any, any>;
		},

		primaryKey(): string | null {
			return meta.primary;
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
			return createTemplate(makeTemplate(["(", ".", " IS NOT NULL)"]), [
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

		on(referencingTable: Table<any>, alias?: string): SQLTemplate {
			// Find FKs in the referencing table that point to this table
			const refs = getTableMeta(referencingTable).references.filter(
				(r) => r.table.name === name,
			);

			if (refs.length === 0) {
				throw new Error(
					`Table "${referencingTable.name}" has no foreign key references to "${name}"`,
				);
			}

			let ref: ReferenceInfo;
			if (refs.length === 1) {
				ref = refs[0];
			} else if (alias) {
				// Disambiguate by "as" alias
				const found = refs.find((r) => r.as === alias);
				if (!found) {
					const availableAliases = refs.map((r) => `"${r.as}"`).join(", ");
					throw new Error(
						`No foreign key with alias "${alias}" found. Available aliases: ${availableAliases}`,
					);
				}
				ref = found;
			} else {
				const availableAliases = refs.map((r) => `"${r.as}"`).join(", ");
				throw new Error(
					`Multiple foreign keys from "${referencingTable.name}" to "${name}". ` +
						`Specify an alias: ${availableAliases}`,
				);
			}

			// Build template: table_or_alias.pk = ref_table.fk_field
			// Without alias: "users"."id" = "posts"."authorId"
			// With alias:    "author"."id" = "posts"."authorId"
			// Users write the JOIN clause: JOIN "users" ON ${Users.on(Posts)}
			// For self-joins with aliases: JOIN "users" AS "author" ON ${Users.on(Posts, "author")}
			const tableRef = alias ?? name;
			return createTemplate(makeTemplate(["", ".", " = ", ".", ""]), [
				ident(tableRef),
				ident(ref.referencedField),
				ident(referencingTable.name),
				ident(ref.fieldName),
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

	// Strip 'db' from collectedMeta - we flatten those properties directly
	const {db: _db, ...userMeta} = collectedMeta;

	const meta: FieldMeta = {
		name,
		type: "text",
		required: !isOptional && !isNullable && !hasDefault,
		...userMeta, // Spread user-defined metadata (label, helpText, widget, etc.)
	};

	// Apply database metadata - merge all FieldDBMeta properties
	if (dbMeta.primaryKey) meta.primaryKey = true;
	if (dbMeta.unique) meta.unique = true;
	if (dbMeta.indexed) meta.indexed = true;
	if (dbMeta.softDelete) meta.softDelete = true;
	if (dbMeta.reference) {
		meta.reference = {
			table: dbMeta.reference.table,
			field:
				dbMeta.reference.field ??
				getTableMeta(dbMeta.reference.table).primary ??
				"id",
			as: dbMeta.reference.as,
			onDelete: dbMeta.reference.onDelete,
		};
	}
	if (dbMeta.encode) meta.encode = dbMeta.encode;
	if (dbMeta.decode) meta.decode = dbMeta.decode;
	if (dbMeta.columnType) meta.columnType = dbMeta.columnType;
	if (dbMeta.autoIncrement) meta.autoIncrement = true;
	if (dbMeta.inserted) meta.inserted = dbMeta.inserted;
	if (dbMeta.updated) meta.updated = dbMeta.updated;
	if (dbMeta.upserted) meta.upserted = dbMeta.upserted;

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
 * Infer the row type from a table (full entity after read).
 *
 * @example
 * const Users = table("users", {...});
 * type User = Row<typeof Users>;
 */
export type Row<T extends Table<any>> = z.infer<T["schema"]>;

/**
 * @deprecated Use `Row<T>` instead.
 */
export type Infer<T extends Table<any>> = Row<T>;

// ============================================================================
// Join Result Types (Magic Types for Multi-Table Queries)
// ============================================================================

/**
 * Extract the Refs type parameter from a Table.
 */
type GetRefs<T extends Table<any, any>> =
	T extends Table<any, infer R> ? R : {};

/**
 * Given a primary table and a tuple of joined tables, build an object type
 * that maps each matching ref alias to Row<RefTable>.
 *
 * For example, if Posts has `{ author: typeof Users }` refs and Users is in JoinedTables,
 * this produces `{ author: Row<typeof Users> }`.
 */
type ResolveRefs<
	Refs extends Record<string, Table<any, any>>,
	JoinedTables extends readonly Table<any, any>[],
> = {
	[Alias in keyof Refs as Refs[Alias] extends JoinedTables[number]
		? Alias
		: never]: Refs[Alias] extends Table<any, any>
		? Row<Refs[Alias]> | null
		: never;
};

/**
 * A row type with resolved relationship properties from joined tables.
 *
 * When you query `db.all([Posts, Users])`, this type produces:
 * `Row<Posts> & { author?: Row<Users> }`
 *
 * @example
 * const posts = await db.all([Posts, Users])`JOIN users ON ...`;
 * posts[0].author?.name;  // typed as string | undefined
 */
export type WithRefs<
	PrimaryTable extends Table<any, any>,
	JoinedTables extends readonly Table<any, any>[],
> = Row<PrimaryTable> & ResolveRefs<GetRefs<PrimaryTable>, JoinedTables>;

/**
 * Type guard that evaluates to `never` for partial or derived tables.
 * Use this to prevent insert/update operations at compile time.
 *
 * @example
 * function insert<T extends Table<any>>(
 *   table: T & FullTableOnly<T>,
 *   data: Insert<T>
 * ): Promise<Row<T>>
 */
export type FullTableOnly<T> = T extends {meta: {isPartial: true}}
	? never
	: T extends {meta: {isDerived: true}}
		? never
		: T;

/**
 * Infer the insert type (respects defaults and .db.auto() fields).
 * Returns `never` for partial or derived tables to prevent insert at compile time.
 *
 * @example
 * const Users = table("users", {...});
 * type NewUser = Insert<typeof Users>;
 */
export type Insert<T extends Table<any>> = T extends {meta: {isPartial: true}}
	? never
	: T extends {meta: {isDerived: true}}
		? never
		: z.input<T["schema"]>;

/**
 * Infer the update type (all fields optional, excludes primary key and insert-only fields).
 * Returns `never` for partial or derived tables to prevent update at compile time.
 *
 * Note: This is a simplified version that makes all fields optional.
 * Primary key exclusion and insert-only field exclusion require runtime enforcement.
 *
 * @example
 * const Users = table("users", {...});
 * type EditUser = Update<typeof Users>;
 */
export type Update<T extends Table<any>> = T extends {meta: {isPartial: true}}
	? never
	: T extends {meta: {isDerived: true}}
		? never
		: Partial<z.input<T["schema"]>>;

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
	 * authorId: z.string().uuid().db.references(Users, "author")
	 *
	 * @example
	 * // With options
	 * authorId: z.string().uuid().db.references(Users, "author", {
	 *   reverseAs: "posts",   // user.posts = Post[]
	 *   ondelete: "cascade",
	 * })
	 */
	references<
		RefTable extends Table<any, any>,
		As extends string,
		ReverseAs extends string | undefined = undefined,
	>(
		table: RefTable,
		as: As,
		options?: {
			field?: string;
			reverseAs?: ReverseAs;
			onDelete?: "cascade" | "set null" | "restrict";
		},
	): Schema & {
		readonly __refTable: RefTable;
		readonly __refAs: As;
		readonly __refReverseAs: ReverseAs;
	};

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
	 * Auto-generate value on insert based on field type.
	 *
	 * Type-aware behavior:
	 * - `z.string().uuid()` → generates UUID via `crypto.randomUUID()`
	 * - `z.number().int()` → auto-increment (database-side)
	 * - `z.date()` → current timestamp via NOW
	 *
	 * Field becomes optional for insert.
	 *
	 * @example
	 * id: z.string().uuid().db.primary().db.auto()
	 * // → crypto.randomUUID() on insert
	 *
	 * @example
	 * id: z.number().int().db.primary().db.auto()
	 * // → auto-increment
	 *
	 * @example
	 * createdAt: z.date().db.auto()
	 * // → NOW on insert
	 */
	auto(): Schema;
}

declare module "zod" {
	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	interface ZodType<out Output, out Input, out Internals> {
		readonly db: ZodDBMethods<this>;
	}
}

// ============================================================================
// Data Decoding
// ============================================================================

/**
 * Decode database result data into proper JS types using table schema.
 *
 * Handles:
 * - Custom .db.decode() transformations
 * - Automatic JSON parsing for object/array fields
 * - Automatic Date parsing for date fields
 */
export function decodeData<T extends Table<any>>(
	table: T,
	data: Record<string, unknown> | null,
): Record<string, unknown> | null {
	if (!data) return data;

	const decoded: Record<string, unknown> = {};
	const shape = table.schema.shape;

	for (const [key, value] of Object.entries(data)) {
		const fieldMeta = getTableMeta(table).fields[key];
		const fieldSchema = shape?.[key];

		if (fieldMeta?.decode && typeof fieldMeta.decode === "function") {
			// Custom decoding specified - use it
			decoded[key] = fieldMeta.decode(value);
		} else if (fieldSchema) {
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
					} catch (e) {
						// Throw with helpful error message mentioning JSON and field
						throw new Error(
							`JSON parse error for field "${key}": ${e instanceof Error ? e.message : String(e)}. ` +
								`Value was: ${value.slice(0, 100)}${value.length > 100 ? "..." : ""}`,
						);
					}
				} else {
					// Already an object (e.g., from PostgreSQL JSONB)
					decoded[key] = value;
				}
			} else if (core instanceof z.ZodDate) {
				// Automatic date decoding from ISO string
				if (typeof value === "string") {
					const date = new Date(value);
					if (isNaN(date.getTime())) {
						throw new Error(
							`Invalid date value for field "${key}": "${value}" cannot be parsed as a valid date`,
						);
					}
					decoded[key] = date;
				} else if (value instanceof Date) {
					if (isNaN(value.getTime())) {
						throw new Error(
							`Invalid Date object for field "${key}": received an Invalid Date`,
						);
					}
					decoded[key] = value;
				} else {
					decoded[key] = value;
				}
			} else {
				decoded[key] = value;
			}
		} else {
			decoded[key] = value;
		}
	}

	return decoded;
}
