/**
 * SQL rendering utilities for all dialects.
 *
 * This is the single source of truth for dialect-specific SQL rendering:
 * - Identifier quoting
 * - Placeholder syntax
 * - SQL builtin resolution
 * - Template rendering (for DDL and queries)
 */

import {isSQLIdentifier} from "./template.js";
import {isSQLBuiltin, resolveSQLBuiltin} from "./builtins.js";

// ============================================================================
// Types
// ============================================================================

export type SQLDialect = "sqlite" | "postgresql" | "mysql";

// ============================================================================
// Core Helpers
// ============================================================================

/**
 * Quote an identifier based on dialect.
 * MySQL uses backticks, PostgreSQL/SQLite use double quotes.
 */
export function quoteIdent(name: string, dialect: SQLDialect): string {
	if (dialect === "mysql") {
		return `\`${name.replace(/`/g, "``")}\``;
	}
	return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Get placeholder syntax based on dialect.
 * PostgreSQL uses $1, $2, etc. MySQL/SQLite use ?.
 */
export function placeholder(index: number, dialect: SQLDialect): string {
	if (dialect === "postgresql") {
		return `$${index}`;
	}
	return "?";
}

// Re-export for consumers that import from sql.ts
export {resolveSQLBuiltin} from "./builtins.js";

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

// ============================================================================
// Query Building
// ============================================================================

/**
 * Build SQL from template parts with parameter placeholders.
 *
 * This is the shared implementation used by all Node drivers (MySQL, PostgreSQL, SQLite).
 * Handles SQLBuiltin symbols, SQLIdentifiers, and regular parameter values.
 *
 * SQL builtins and identifiers are inlined directly; other values use placeholders.
 */
export function buildSQL(
	strings: TemplateStringsArray,
	values: unknown[],
	dialect: SQLDialect,
): {sql: string; params: unknown[]} {
	let sql = strings[0];
	const params: unknown[] = [];

	for (let i = 0; i < values.length; i++) {
		const value = values[i];
		if (isSQLBuiltin(value)) {
			// Inline the symbol's SQL directly
			sql += resolveSQLBuiltin(value) + strings[i + 1];
		} else if (isSQLIdentifier(value)) {
			// Quote identifier based on dialect
			sql += quoteIdent(value.name, dialect) + strings[i + 1];
		} else {
			// Add placeholder and keep value
			sql += placeholder(params.length + 1, dialect) + strings[i + 1];
			params.push(value);
		}
	}

	return {sql, params};
}
