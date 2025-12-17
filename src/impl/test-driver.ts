/**
 * Test driver for rendering SQL templates without a real database.
 *
 * This is the ONLY place outside of production drivers where dialect-specific
 * rendering happens. Used by tests and DDL generation.
 */

import {isSQLIdentifier} from "./template.js";

// ============================================================================
// Types
// ============================================================================

export type SQLDialect = "sqlite" | "postgresql" | "mysql";

// ============================================================================
// Rendering
// ============================================================================

/**
 * Quote an identifier based on dialect.
 */
function quoteIdent(name: string, dialect: SQLDialect): string {
	if (dialect === "mysql") {
		return `\`${name.replace(/`/g, "``")}\``;
	}
	return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Get placeholder syntax based on dialect.
 */
function placeholder(index: number, dialect: SQLDialect): string {
	if (dialect === "postgresql") {
		return `$${index}`;
	}
	return "?";
}

/**
 * Render a template to SQL string with parameters.
 * Handles SQLIdentifier markers and regular values.
 */
export function renderSQL(
	strings: TemplateStringsArray,
	values: readonly unknown[],
	dialect: SQLDialect,
): {sql: string; params: unknown[]} {
	let sql = "";
	const params: unknown[] = [];

	for (let i = 0; i < strings.length; i++) {
		sql += strings[i];
		if (i < values.length) {
			const value = values[i];
			if (isSQLIdentifier(value)) {
				sql += quoteIdent(value.name, dialect);
			} else {
				params.push(value);
				sql += placeholder(params.length, dialect);
			}
		}
	}

	return {sql, params};
}

/**
 * Render a DDL template to SQL string.
 * DDL templates only contain identifiers (no parameter placeholders).
 */
export function renderDDL(
	strings: TemplateStringsArray,
	values: readonly unknown[],
	dialect: SQLDialect,
): string {
	let sql = "";
	for (let i = 0; i < strings.length; i++) {
		sql += strings[i];
		if (i < values.length) {
			const value = values[i];
			if (isSQLIdentifier(value)) {
				sql += quoteIdent(value.name, dialect);
			} else {
				throw new Error(`Unexpected value in DDL template: ${value}`);
			}
		}
	}
	return sql;
}
