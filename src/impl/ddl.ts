/**
 * DDL generation from table definitions.
 *
 * Generates CREATE TABLE statements for SQLite, PostgreSQL, and MySQL.
 * Uses only Zod's public APIs - no _def access.
 */

import {z} from "zod";
import type {Table} from "./table.js";

// ============================================================================
// Types
// ============================================================================

export type SQLDialect = "sqlite" | "postgresql" | "mysql";

export interface DDLOptions {
	dialect?: SQLDialect;
	ifNotExists?: boolean;
}

interface ColumnDef {
	name: string;
	sqlType: string;
	nullable: boolean;
	primaryKey: boolean;
	unique: boolean;
	autoIncrement: boolean;
	defaultValue?: string;
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
	} else {
		// Fallback for unknown types
		sqlType = "TEXT";
		if (hasDefault && defaultValue !== undefined) {
			sqlDefault = `'${String(defaultValue).replace(/'/g, "''")}'`;
		}
	}

	return {sqlType, defaultValue: sqlDefault};
}

// ============================================================================
// DDL Generation
// ============================================================================

function quoteIdent(name: string, dialect: SQLDialect): string {
	if (dialect === "mysql") {
		// MySQL: backticks, doubled to escape
		return `\`${name.replace(/`/g, "``")}\``;
	}
	// PostgreSQL and SQLite: double quotes, doubled to escape
	return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Generate a single column definition for ALTER TABLE ADD COLUMN.
 * @internal Used by Table.ensureColumn()
 */
export function generateColumnDDL(
	fieldName: string,
	zodType: z.ZodType,
	fieldMeta: Record<string, any>,
	dialect: SQLDialect = "sqlite",
): string {
	const {isOptional, isNullable, hasDefault} = unwrapType(zodType);
	const {sqlType, defaultValue: sqlDefault} = mapZodToSQL(
		zodType,
		dialect,
		fieldMeta,
	);

	let def = `${quoteIdent(fieldName, dialect)} ${sqlType}`;

	const nullable = isOptional || isNullable || hasDefault;
	if (!nullable) {
		def += " NOT NULL";
	}

	if (sqlDefault !== undefined) {
		def += ` DEFAULT ${sqlDefault}`;
	}

	// Primary key handled at table level for ALTER TABLE
	// SQLite doesn't allow adding PRIMARY KEY via ALTER TABLE anyway

	if (fieldMeta.unique === true) {
		def += " UNIQUE";
	}

	return def;
}

/**
 * Generate CREATE TABLE DDL from a table definition.
 */
export function generateDDL<T extends Table<any>>(
	table: T,
	options: DDLOptions = {},
): string {
	const {dialect = "sqlite", ifNotExists = true} = options;
	const shape = table.schema.shape;
	const meta = table.meta;

	const columns: ColumnDef[] = [];

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

		const column: ColumnDef = {
			name,
			sqlType,
			nullable: isOptional || isNullable || hasDefault,
			primaryKey: fieldMeta.primaryKey === true,
			unique: fieldMeta.unique === true,
			autoIncrement: fieldMeta.autoIncrement === true,
			defaultValue: sqlDefault,
		};

		columns.push(column);
	}

	// Build column definitions
	const columnDefs: string[] = [];

	for (const col of columns) {
		let def = `${quoteIdent(col.name, dialect)} ${col.sqlType}`;

		// Handle auto-increment based on dialect
		if (col.autoIncrement) {
			if (dialect === "sqlite") {
				// SQLite: INTEGER PRIMARY KEY is auto-increment by default
				// Adding AUTOINCREMENT prevents rowid reuse (usually not needed)
				def += " PRIMARY KEY AUTOINCREMENT";
			} else if (dialect === "postgresql") {
				// PostgreSQL: Use GENERATED ALWAYS AS IDENTITY (SQL standard, PG 10+)
				def += " GENERATED ALWAYS AS IDENTITY";
			} else if (dialect === "mysql") {
				// MySQL: AUTO_INCREMENT (must be a key, handled with PRIMARY KEY below)
				def += " AUTO_INCREMENT";
			}
		}

		if (!col.nullable && !col.autoIncrement) {
			// Auto-increment columns are implicitly NOT NULL
			def += " NOT NULL";
		}

		if (col.defaultValue !== undefined && !col.autoIncrement) {
			// Auto-increment columns shouldn't have DEFAULT
			def += ` DEFAULT ${col.defaultValue}`;
		}

		// SQLite primary key is inline (and already added for autoIncrement)
		if (col.primaryKey && dialect === "sqlite" && !col.autoIncrement) {
			def += " PRIMARY KEY";
		}

		if (col.unique && !col.primaryKey) {
			def += " UNIQUE";
		}

		columnDefs.push(def);
	}

	// PRIMARY KEY constraint for non-SQLite or composite keys
	if (meta.primary && dialect !== "sqlite") {
		columnDefs.push(`PRIMARY KEY (${quoteIdent(meta.primary, dialect)})`);
	}

	// FOREIGN KEY constraints (single-field)
	for (const ref of meta.references) {
		const fkColumn = quoteIdent(ref.fieldName, dialect);
		const refTable = quoteIdent(ref.table.name, dialect);
		const refColumn = quoteIdent(ref.referencedField, dialect);

		let fk = `FOREIGN KEY (${fkColumn}) REFERENCES ${refTable}(${refColumn})`;

		// Add ON DELETE behavior
		if (ref.onDelete) {
			const onDeleteSQL =
				ref.onDelete === "set null" ? "SET NULL" : ref.onDelete.toUpperCase();
			fk += ` ON DELETE ${onDeleteSQL}`;
		}

		columnDefs.push(fk);
	}

	// Compound FOREIGN KEY constraints
	for (const ref of table.compoundReferences) {
		const fkColumns = ref.fields.map((f) => quoteIdent(f, dialect)).join(", ");
		const refTable = quoteIdent(ref.table.name, dialect);
		// Use referencedFields if provided, otherwise use the same field names
		const refFields = ref.referencedFields ?? ref.fields;
		const refColumns = refFields.map((f) => quoteIdent(f, dialect)).join(", ");

		let fk = `FOREIGN KEY (${fkColumns}) REFERENCES ${refTable}(${refColumns})`;

		if (ref.onDelete) {
			const onDeleteSQL =
				ref.onDelete === "set null" ? "SET NULL" : ref.onDelete.toUpperCase();
			fk += ` ON DELETE ${onDeleteSQL}`;
		}

		columnDefs.push(fk);
	}

	// Compound UNIQUE constraints
	for (const uniqueCols of table.unique) {
		const cols = uniqueCols.map((c) => quoteIdent(c, dialect)).join(", ");
		columnDefs.push(`UNIQUE (${cols})`);
	}

	// Build CREATE TABLE
	const tableName = quoteIdent(table.name, dialect);
	const exists = ifNotExists ? "IF NOT EXISTS " : "";
	let sql = `CREATE TABLE ${exists}${tableName} (\n  ${columnDefs.join(",\n  ")}\n);`;

	// Add indexes for indexed fields
	for (const indexedField of meta.indexed) {
		const indexName = `idx_${table.name}_${indexedField}`;
		sql += `\n\nCREATE INDEX ${exists}${quoteIdent(indexName, dialect)} ON ${tableName} (${quoteIdent(indexedField, dialect)});`;
	}

	// Add compound indexes from table options
	for (const indexCols of table.indexes) {
		const indexName = `idx_${table.name}_${indexCols.join("_")}`;
		const cols = indexCols.map((c) => quoteIdent(c, dialect)).join(", ");
		sql += `\n\nCREATE INDEX ${exists}${quoteIdent(indexName, dialect)} ON ${tableName} (${cols});`;
	}

	return sql;
}

/**
 * Convenience function for generating DDL.
 */
export function ddl(table: Table<any>, dialect: SQLDialect = "sqlite"): string {
	return generateDDL(table, {dialect});
}
