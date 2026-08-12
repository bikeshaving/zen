/**
 * DDL generation from table definitions.
 *
 * Generates CREATE TABLE statements for SQLite, PostgreSQL, and MySQL.
 * Uses only Zod's public APIs - no _def access.
 */

import {z} from "zod";
import type {Table, View} from "./table.js";
import {getTableMeta, getViewMeta} from "./table.js";
import {TableDefinitionError} from "./errors.js";
import {
	ident,
	makeTemplate,
	createTemplate,
	type SQLTemplate,
} from "./template.js";

export type SQLDialect = "sqlite" | "postgresql" | "mysql";

export interface DDLOptions {
	dialect?: SQLDialect;
	ifNotExists?: boolean;
}

// ============================================================================
// Type Mapping (using only public Zod APIs)
// ============================================================================

interface UnwrapResult {
	core: z.ZodType;
	isOptional: boolean;
	isNullable: boolean;
	hasDefault: boolean;
	defaultValue?: unknown;
}

/**
 * Unwrap wrapper types (Optional, Nullable, Default, etc.) using public APIs.
 */
function unwrapType(schema: z.ZodType): UnwrapResult {
	let core: z.ZodType = schema;
	let isOptional = false;
	let isNullable = false;
	let hasDefault = false;
	let defaultValue: unknown = undefined;

	// Use public isOptional/isNullable first
	isOptional = schema.isOptional();
	isNullable = schema.isNullable();

	// Unwrap layers using public methods
	while (true) {
		// Check for ZodDefault (has removeDefault method)
		if (typeof (core as any).removeDefault === "function") {
			hasDefault = true;
			// Get default value by parsing undefined
			try {
				defaultValue = core.parse(undefined);
			} catch {
				// If parse fails, default might be a function that throws
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

		// No more wrappers to unwrap
		break;
	}

	return {core, isOptional, isNullable, hasDefault, defaultValue};
}

/**
 * Map a Zod type to SQL type using instanceof checks and public properties.
 */
function mapZodToSQL(
	schema: z.ZodType,
	dialect: SQLDialect,
	fieldMeta?: Record<string, any>,
): {sqlType: string; defaultValue?: string} {
	const {core, hasDefault, defaultValue} = unwrapType(schema);

	let sqlType: string;
	let sqlDefault: string | undefined;

	// If explicit column type is specified via .db.type(), use it
	if (fieldMeta?.columnType) {
		sqlType = fieldMeta.columnType;
		// Still compute default value if present
		if (hasDefault && defaultValue !== undefined) {
			// For explicit types, just stringify the default
			if (typeof defaultValue === "string") {
				sqlDefault = `'${defaultValue.replace(/'/g, "''")}'`;
			} else if (
				typeof defaultValue === "number" ||
				typeof defaultValue === "boolean"
			) {
				sqlDefault = String(defaultValue);
			} else {
				sqlDefault = `'${JSON.stringify(defaultValue).replace(/'/g, "''")}'`;
			}
		}
		return {sqlType, defaultValue: sqlDefault};
	}

	// Use instanceof checks instead of _def.typeName
	if (core instanceof z.ZodString) {
		// Use public maxLength property
		const maxLength = (core as any).maxLength as number | undefined;

		if (maxLength && maxLength <= 255 && dialect !== "sqlite") {
			sqlType = `VARCHAR(${maxLength})`;
		} else {
			sqlType = "TEXT";
		}

		if (hasDefault && defaultValue !== undefined) {
			sqlDefault = `'${String(defaultValue).replace(/'/g, "''")}'`;
		}
	} else if (core instanceof z.ZodNumber) {
		// Use public isInt property
		const isInt = (core as any).isInt as boolean | undefined;

		if (isInt) {
			sqlType = "INTEGER";
		} else {
			sqlType = dialect === "postgresql" ? "DOUBLE PRECISION" : "REAL";
		}

		if (hasDefault && defaultValue !== undefined) {
			sqlDefault = String(defaultValue);
		}
	} else if (core instanceof z.ZodBoolean) {
		sqlType = dialect === "sqlite" ? "INTEGER" : "BOOLEAN";
		if (hasDefault && defaultValue !== undefined) {
			if (dialect === "sqlite") {
				sqlDefault = defaultValue ? "1" : "0";
			} else {
				sqlDefault = defaultValue ? "TRUE" : "FALSE";
			}
		}
	} else if (core instanceof z.ZodDate) {
		if (dialect === "postgresql") {
			sqlType = "TIMESTAMPTZ";
		} else if (dialect === "mysql") {
			sqlType = "DATETIME";
		} else {
			sqlType = "TEXT";
		}

		if (hasDefault) {
			// Date defaults are usually functions (new Date()), use DB default
			if (dialect === "sqlite") {
				sqlDefault = "CURRENT_TIMESTAMP";
			} else if (dialect === "postgresql") {
				sqlDefault = "NOW()";
			} else {
				sqlDefault = "CURRENT_TIMESTAMP";
			}
		}
	} else if (core instanceof z.ZodEnum) {
		sqlType = "TEXT";
		if (hasDefault && defaultValue !== undefined) {
			sqlDefault = `'${String(defaultValue).replace(/'/g, "''")}'`;
		}
	} else if (core instanceof z.ZodArray || core instanceof z.ZodObject) {
		// Objects and arrays: JSONB in PostgreSQL, TEXT elsewhere
		if (dialect === "postgresql") {
			sqlType = "JSONB";
		} else {
			sqlType = "TEXT";
		}
		if (hasDefault && defaultValue !== undefined) {
			sqlDefault = `'${JSON.stringify(defaultValue).replace(/'/g, "''")}'`;
		}
	} else if (isForeignZodSchema(core)) {
		// Every instanceof check above failed, yet this *is* a Zod schema — it just
		// came from a different copy of Zod than the one zen imported. Falling
		// through would silently type every column as TEXT, so refuse instead.
		throw new TableDefinitionError(
			"Schema came from a different copy of Zod than the one @b9g/zen is using, " +
				"so its type could not be recognized. This usually means two versions of " +
				"Zod are installed. zen declares zod as a peer dependency: make sure your " +
				"project resolves exactly one zod (try `npm ls zod` / `bun pm ls zod`). " +
				"Without this, every column would silently be created as TEXT.",
		);
	} else {
		// Fallback for genuinely unhandled (but ours) Zod types, e.g. ZodUnion.
		sqlType = "TEXT";
		if (hasDefault && defaultValue !== undefined) {
			sqlDefault = `'${String(defaultValue).replace(/'/g, "''")}'`;
		}
	}

	return {sqlType, defaultValue: sqlDefault};
}

/**
 * Detect a Zod schema that originates from a *different* Zod instance.
 *
 * Zod schemas from our own copy always pass `instanceof z.ZodType` — including
 * types we don't map explicitly — so those still take the TEXT fallback. A
 * schema that advertises itself as Zod via Standard Schema but fails the
 * instanceof check can only be from a duplicate install (the dual package
 * hazard), which would otherwise make every instanceof in this file fail
 * silently.
 */
function isForeignZodSchema(value: unknown): boolean {
	if (value instanceof z.ZodType) {
		return false;
	}
	const vendor = (value as any)?.["~standard"]?.vendor;
	return vendor === "zod";
}

// ============================================================================
// DDL Generation
// ============================================================================

/**
 * Generate a single column definition as a template with ident markers.
 * @internal Used by driver ensureTable implementations.
 */
export function generateColumnDDL(
	fieldName: string,
	zodType: z.ZodType,
	fieldMeta: Record<string, any>,
	dialect: SQLDialect = "sqlite",
): SQLTemplate {
	const {isOptional, isNullable, hasDefault} = unwrapType(zodType);
	const {sqlType, defaultValue: sqlDefault} = mapZodToSQL(
		zodType,
		dialect,
		fieldMeta,
	);

	// Build: ${ident(col)} TYPE [NOT NULL] [DEFAULT ...]
	const strings: string[] = [""];
	const values: unknown[] = [ident(fieldName)];

	let suffix = ` ${sqlType}`;
	const nullable = isOptional || isNullable || hasDefault;
	if (!nullable) {
		suffix += " NOT NULL";
	}
	if (sqlDefault !== undefined) {
		suffix += ` DEFAULT ${sqlDefault}`;
	}
	if (fieldMeta.unique === true) {
		suffix += " UNIQUE";
	}
	strings.push(suffix);

	return createTemplate(makeTemplate(strings), values);
}

/**
 * Generate CREATE TABLE DDL as a template with ident markers.
 * Pass to driver.run() to execute, or use renderDDL() in tests.
 */
export function generateDDL<T extends Table<any>>(
	table: T,
	options: DDLOptions = {},
): SQLTemplate {
	const {dialect = "sqlite", ifNotExists = true} = options;
	const shape = table.schema.shape;
	const meta = getTableMeta(table);

	// We'll build a template that looks like:
	// CREATE TABLE [IF NOT EXISTS] ${tableName} (
	//   ${col1} TYPE...,
	//   ${col2} TYPE...,
	//   PRIMARY KEY (${pk}),
	//   FOREIGN KEY (${fk}) REFERENCES ${refTable}(${refCol}),
	//   ...
	// );
	// CREATE INDEX IF NOT EXISTS ${idx} ON ${table} (${col});

	const strings: string[] = [];
	const values: unknown[] = [];

	// Start: CREATE TABLE [IF NOT EXISTS] ${tableName} (
	const tableExists = ifNotExists ? "IF NOT EXISTS " : "";
	// MySQL doesn't support IF NOT EXISTS for CREATE INDEX
	const indexExists =
		ifNotExists && dialect !== "mysql" ? "IF NOT EXISTS " : "";
	strings.push(`CREATE TABLE ${tableExists}`);
	values.push(ident(table.name));
	strings.push(" (\n  ");

	// Collect all column definitions and constraints as template parts
	let needsComma = false;

	// Process columns
	for (const [name, zodType] of Object.entries(shape)) {
		const fieldMeta = meta.fields[name] || {};
		const {isOptional, isNullable, hasDefault} = unwrapType(
			zodType as z.ZodType,
		);
		const {sqlType, defaultValue: sqlDefault} = mapZodToSQL(
			zodType as z.ZodType,
			dialect,
			fieldMeta,
		);

		if (needsComma) {
			strings[strings.length - 1] += ",\n  ";
		}
		needsComma = true;

		// Column: ${name} TYPE [modifiers]
		values.push(ident(name));
		let colDef = ` ${sqlType}`;

		// Handle auto-increment (dialect-specific keywords)
		if (fieldMeta.autoIncrement) {
			if (dialect === "sqlite") {
				colDef += " PRIMARY KEY AUTOINCREMENT";
			} else if (dialect === "postgresql") {
				colDef += " GENERATED ALWAYS AS IDENTITY";
			} else if (dialect === "mysql") {
				colDef += " AUTO_INCREMENT";
			}
		}

		const nullable = isOptional || isNullable || hasDefault;
		if (!nullable && !fieldMeta.autoIncrement) {
			colDef += " NOT NULL";
		}

		if (sqlDefault !== undefined && !fieldMeta.autoIncrement) {
			colDef += ` DEFAULT ${sqlDefault}`;
		}

		// SQLite primary key is inline (and already added for autoIncrement)
		if (
			fieldMeta.primaryKey &&
			dialect === "sqlite" &&
			!fieldMeta.autoIncrement
		) {
			colDef += " PRIMARY KEY";
		}

		if (fieldMeta.unique && !fieldMeta.primaryKey) {
			colDef += " UNIQUE";
		}

		strings.push(colDef);
	}

	// PRIMARY KEY constraint for non-SQLite
	if (meta.primary && dialect !== "sqlite") {
		if (needsComma) {
			strings[strings.length - 1] += ",\n  PRIMARY KEY (";
		} else {
			strings[strings.length - 1] += "PRIMARY KEY (";
			needsComma = true;
		}
		values.push(ident(meta.primary));
		strings.push(")");
	}

	// FOREIGN KEY constraints (single-field)
	for (const ref of meta.references) {
		if (needsComma) {
			strings[strings.length - 1] += ",\n  FOREIGN KEY (";
		} else {
			strings[strings.length - 1] += "FOREIGN KEY (";
			needsComma = true;
		}

		values.push(ident(ref.fieldName));
		strings.push(") REFERENCES ");
		values.push(ident(ref.table.name));
		strings.push("(");
		values.push(ident(ref.referencedField));

		let fkSuffix = ")";
		if (ref.onDelete) {
			const onDeleteSQL =
				ref.onDelete === "set null" ? "SET NULL" : ref.onDelete.toUpperCase();
			fkSuffix += ` ON DELETE ${onDeleteSQL}`;
		}
		strings.push(fkSuffix);
	}

	// Compound FOREIGN KEY constraints
	for (const ref of table.compoundReferences) {
		if (needsComma) {
			strings[strings.length - 1] += ",\n  FOREIGN KEY (";
		} else {
			strings[strings.length - 1] += "FOREIGN KEY (";
			needsComma = true;
		}

		// Add columns: ${col1}, ${col2}, ...
		for (let i = 0; i < ref.fields.length; i++) {
			if (i > 0) strings[strings.length - 1] += ", ";
			values.push(ident(ref.fields[i]));
			strings.push("");
		}
		strings[strings.length - 1] += ") REFERENCES ";
		values.push(ident(ref.table.name));
		strings.push("(");

		const refFields = ref.referencedFields ?? ref.fields;
		for (let i = 0; i < refFields.length; i++) {
			if (i > 0) strings[strings.length - 1] += ", ";
			values.push(ident(refFields[i]));
			strings.push("");
		}

		let fkSuffix = ")";
		if (ref.onDelete) {
			const onDeleteSQL =
				ref.onDelete === "set null" ? "SET NULL" : ref.onDelete.toUpperCase();
			fkSuffix += ` ON DELETE ${onDeleteSQL}`;
		}
		strings[strings.length - 1] += fkSuffix;
	}

	// Compound UNIQUE constraints
	for (const uniqueCols of table.unique) {
		if (needsComma) {
			strings[strings.length - 1] += ",\n  UNIQUE (";
		} else {
			strings[strings.length - 1] += "UNIQUE (";
			needsComma = true;
		}

		for (let i = 0; i < uniqueCols.length; i++) {
			if (i > 0) strings[strings.length - 1] += ", ";
			values.push(ident(uniqueCols[i]));
			strings.push("");
		}
		strings[strings.length - 1] += ")";
	}

	// Close CREATE TABLE
	strings[strings.length - 1] += "\n);";

	// Add indexes for indexed fields
	for (const indexedField of meta.indexed) {
		const indexName = `idx_${table.name}_${indexedField}`;
		strings[strings.length - 1] += `\n\nCREATE INDEX ${indexExists}`;
		values.push(ident(indexName));
		strings.push(" ON ");
		values.push(ident(table.name));
		strings.push(" (");
		values.push(ident(indexedField));
		strings.push(");");
	}

	// Add compound indexes
	for (const indexCols of table.indexes) {
		const indexName = `idx_${table.name}_${indexCols.join("_")}`;
		strings[strings.length - 1] += `\n\nCREATE INDEX ${indexExists}`;
		values.push(ident(indexName));
		strings.push(" ON ");
		values.push(ident(table.name));
		strings.push(" (");

		for (let i = 0; i < indexCols.length; i++) {
			if (i > 0) strings[strings.length - 1] += ", ";
			values.push(ident(indexCols[i]));
			strings.push("");
		}
		strings[strings.length - 1] += ");";
	}

	return createTemplate(makeTemplate(strings), values);
}

/**
 * Generate CREATE VIEW DDL as a template.
 *
 * For views, the DDL is:
 * DROP VIEW IF EXISTS "view_name";
 * CREATE VIEW "view_name" AS SELECT * FROM "base_table" WHERE ...;
 *
 * Note: We use DROP + CREATE because SQLite doesn't support CREATE OR REPLACE VIEW.
 */
export function generateViewDDL<T extends View<any>>(
	viewObj: T,
	_options: DDLOptions = {},
): SQLTemplate {
	const viewMeta = getViewMeta(viewObj);

	const strings: string[] = [];
	const values: unknown[] = [];

	// DROP VIEW IF EXISTS
	strings.push("DROP VIEW IF EXISTS ");
	values.push(ident(viewObj.name));
	strings.push(";\n\n");

	// CREATE VIEW ... AS SELECT * FROM base_table
	strings[strings.length - 1] += "CREATE VIEW ";
	values.push(ident(viewObj.name));
	strings.push(" AS SELECT * FROM ");
	values.push(ident(viewMeta.baseTable.name));
	strings.push(" ");

	// Append the WHERE clause from the view's template
	const whereTemplate = viewMeta.whereTemplate;
	const whereStrings = whereTemplate[0];
	const whereValues = whereTemplate.slice(1);

	// Merge the WHERE clause template
	strings[strings.length - 1] += whereStrings[0];
	for (let i = 0; i < whereValues.length; i++) {
		values.push(whereValues[i]);
		strings.push(whereStrings[i + 1]);
	}

	strings[strings.length - 1] += ";";

	return createTemplate(makeTemplate(strings), values);
}
