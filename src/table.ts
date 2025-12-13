/**
 * Table definition with wrapper-based field extensions.
 *
 * Uses wrapper functions instead of .pipe() to avoid Zod internals.
 * Metadata is extracted once at table() call time.
 */

import {z, ZodTypeAny, ZodObject, ZodRawShape} from "zod";
import {TableDefinitionError} from "./errors.js";
import {createFragment, type SQLFragment} from "./query.js";

// ============================================================================
// Wrapper Types
// ============================================================================

const DB_FIELD = Symbol.for("@b9g/zealot:field");

interface FieldWrapper<T extends ZodTypeAny = ZodTypeAny> {
	[DB_FIELD]: true;
	schema: T;
	meta: FieldDbMeta;
}

function isFieldWrapper(value: unknown): value is FieldWrapper {
	return (
		value !== null &&
		typeof value === "object" &&
		DB_FIELD in value &&
		(value as any)[DB_FIELD] === true
	);
}

function createWrapper<T extends ZodTypeAny>(
	schema: T,
	meta: FieldDbMeta,
): FieldWrapper<T> {
	return {
		[DB_FIELD]: true,
		schema,
		meta,
	};
}

// ============================================================================
// Field Metadata
// ============================================================================

export interface FieldDbMeta {
	primaryKey?: boolean;
	unique?: boolean;
	indexed?: boolean;
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
export function primary<T extends ZodTypeAny>(schema: T): FieldWrapper<T> {
	return createWrapper(schema, {primaryKey: true});
}

/**
 * Mark a field as unique.
 *
 * @example
 * email: unique(z.string().email())
 */
export function unique<T extends ZodTypeAny>(schema: T): FieldWrapper<T> {
	return createWrapper(schema, {unique: true});
}

/**
 * Mark a field for indexing.
 *
 * @example
 * createdAt: index(z.date())
 */
export function index<T extends ZodTypeAny>(schema: T): FieldWrapper<T> {
	return createWrapper(schema, {indexed: true});
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
): FieldWrapper<T> {
	return createWrapper(schema, {
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
		references: ReferenceInfo[];
		fields: Record<string, FieldDbMeta>;
		/** True if this is a partial table created via pick() */
		isPartial?: boolean;
	};

	/** Get field metadata for forms/admin */
	fields(): Record<string, FieldMeta>;

	/** Get primary key field name */
	primaryKey(): string | null;

	/** Get all foreign key references */
	references(): ReferenceInfo[];

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
	 * Generate value tuples fragment for INSERT statements.
	 *
	 * Each row is validated against the table schema. The columns array
	 * determines the order of values and must match the SQL column list.
	 *
	 * @example
	 * db.exec`
	 *   INSERT INTO posts (id, title) VALUES ${Posts.values(rows, ["id", "title"])}
	 * `
	 * // → (?, ?), (?, ?)
	 */
	values(
		rows: Partial<z.infer<ZodObject<T>>>[],
		columns: (keyof z.infer<ZodObject<T>> & string)[],
	): SQLFragment;
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
export function table<T extends Record<string, ZodTypeAny | FieldWrapper>>(
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

	// Extract Zod schemas and metadata
	const zodShape: Record<string, ZodTypeAny> = {};
	const meta = {
		primary: null as string | null,
		unique: [] as string[],
		indexed: [] as string[],
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
		if (isFieldWrapper(value)) {
			zodShape[key] = value.schema;
			meta.fields[key] = value.meta;

			if (value.meta.primaryKey) {
				meta.primary = key;
			}
			if (value.meta.unique) {
				meta.unique.push(key);
			}
			if (value.meta.indexed) {
				meta.indexed.push(key);
			}
			if (value.meta.reference) {
				const ref = value.meta.reference;
				meta.references.push({
					fieldName: key,
					table: ref.table,
					referencedField: ref.field ?? ref.table.primaryKey() ?? "id",
					as: ref.as,
					onDelete: ref.onDelete,
				});
			}
		} else {
			zodShape[key] = value as ZodTypeAny;
		}
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
		references: ReferenceInfo[];
		fields: Record<string, FieldDbMeta>;
	},
	indexes: string[][],
): Table<any> {
	const cols = createColsProxy(name, zodShape);

	return {
		[TABLE_MARKER]: true,
		name,
		schema,
		indexes,
		_meta: meta,
		cols,

		fields(): Record<string, FieldMeta> {
			const result: Record<string, FieldMeta> = {};

			for (const [key, zodType] of Object.entries(zodShape)) {
				const dbMeta = meta.fields[key] || {};
				result[key] = extractFieldMeta(key, zodType, dbMeta);
			}

			return result;
		},

		primaryKey(): string | null {
			return meta.primary;
		},

		references(): ReferenceInfo[] {
			return meta.references;
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

		values(rows: Record<string, unknown>[], columns: string[]): SQLFragment {
			if (rows.length === 0) {
				throw new Error("values() requires at least one row");
			}

			if (columns.length === 0) {
				throw new Error("values() requires at least one column");
			}

			const partialSchema = schema.partial();
			const params: unknown[] = [];
			const tuples: string[] = [];

			for (const row of rows) {
				const validated = partialSchema.parse(row);
				const rowPlaceholders: string[] = [];

				for (const col of columns) {
					rowPlaceholders.push("?");
					params.push((validated as Record<string, unknown>)[col]);
				}

				tuples.push(`(${rowPlaceholders.join(", ")})`);
			}

			return createFragment(tuples.join(", "), params);
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
				dbMeta.reference.field ?? dbMeta.reference.table.primaryKey() ?? "id",
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
