/**
 * Tests for bug fixes - these should fail before the fix and pass after.
 */

import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {table, extendZod} from "./table.js";
import {Database, type Driver} from "./database.js";

extendZod(z);

// Helper to create mock drivers
function createMockDriver(overrides: Partial<Driver> = {}): Driver {
	const driver: Driver = {
		supportsReturning: true,
		all: async () => [],
		get: async () => null,
		run: async () => 0,
		val: async () => null,
		close: async () => {},
		transaction: async (fn) => fn(driver),
		...overrides,
	};
	return driver;
}

// =============================================================================
// Issue #1: val() type should be Promise<T | null>, not Promise<T>
// =============================================================================

describe("Issue #1: val() type safety", () => {
	test("val() should have correct return type allowing null", () => {
		// This is a compile-time check - if val() returns Promise<T | null>,
		// this code should compile without error
		const driver = createMockDriver({
			val: async <T>(): Promise<T | null> => null, // Should match interface
		});

		expect(driver.val).toBeDefined();
	});
});

// =============================================================================
// Issue #3: Empty array in in() - document behavior or throw
// The current behavior returns "1 = 0" which is intentional but could surprise users.
// Test that it at least works correctly.
// =============================================================================

describe("Issue #3: Empty array in in() clause", () => {
	const Users = table("users", {
		id: z.string().db.primary(),
		name: z.string(),
	});

	test("in() with empty array returns 1 = 0 (always false)", () => {
		const fragment = Users.in("id", []);
		expect(fragment.sql).toBe("1 = 0");
		expect(fragment.params).toEqual([]);
	});
});

// =============================================================================
// Issue #5: JSON.parse failures should have clear error message
// =============================================================================

describe("Issue #5: JSON decode error handling", () => {
	test("malformed JSON in object field should have clear error message", async () => {
		const Settings = table("settings", {
			id: z.string().db.primary(),
			config: z.object({theme: z.string()}),
		});

		const driver = createMockDriver({
			all: async () =>
				[{"settings.id": "1", "settings.config": "not-valid-json"}] as any,
			get: async () =>
				({"settings.id": "1", "settings.config": "not-valid-json"}) as any,
		});

		const db = new Database(driver);

		// Should throw with a message that mentions JSON parsing, not just "Validation failed"
		try {
			await db.get(Settings)`WHERE id = ${"1"}`;
			expect(true).toBe(false); // Should not reach here
		} catch (e: any) {
			// The error message should mention JSON or parse to help debugging
			expect(e.message).toMatch(/JSON|parse|config/i);
		}
	});
});

// =============================================================================
// Issue #6: Identifier validation for control chars
// =============================================================================

describe("Issue #6: Identifier validation", () => {
	test("table name with newline should throw", () => {
		expect(() => {
			table("users\nDROP TABLE users;--", {
				id: z.string().db.primary(),
			});
		}).toThrow(/invalid.*identifier|control.*char/i);
	});

	test("column name with null byte should throw", () => {
		expect(() => {
			table("users", {
				"id\x00": z.string().db.primary(),
			});
		}).toThrow(/invalid.*identifier|control.*char/i);
	});

	test("table name with semicolon should throw", () => {
		expect(() => {
			table("users; DROP TABLE users;--", {
				id: z.string().db.primary(),
			});
		}).toThrow(/invalid.*identifier/i);
	});
});

// =============================================================================
// Issue #7: PostgreSQL param limit (32767)
// =============================================================================

describe("Issue #7: PostgreSQL parameter limit", () => {
	const Users = table("users", {
		id: z.string().db.primary(),
	});

	test("should warn or throw when exceeding PostgreSQL 32767 param limit", () => {
		// Create array with 40000 values (exceeds 32767)
		const tooManyValues = Array.from({length: 40000}, (_, i) => `id-${i}`);

		// Should throw when creating the IN clause
		expect(() => {
			Users.in("id", tooManyValues);
		}).toThrow(/too many|param.*limit|exceed|32767/i);
	});
});

// =============================================================================
// Issue #9: Invalid Date validation
// =============================================================================

describe("Issue #9: Date validation", () => {
	test("invalid date string should throw with clear message", async () => {
		const Events = table("events", {
			id: z.string().db.primary(),
			startedAt: z.date(),
		});

		const driver = createMockDriver({
			all: async () =>
				[{"events.id": "1", "events.startedAt": "not-a-date"}] as any,
			get: async () =>
				({"events.id": "1", "events.startedAt": "not-a-date"}) as any,
		});

		const db = new Database(driver);

		// Should throw with a message mentioning the date issue
		try {
			await db.get(Events)`WHERE id = ${"1"}`;
			expect(true).toBe(false); // Should not reach here
		} catch (e: any) {
			// The error should mention date or the field name
			expect(e.message).toMatch(/date|startedAt|invalid/i);
		}
	});
});

// =============================================================================
// Issue #10: Circular reference detection (informational)
// =============================================================================

describe("Issue #10: Circular reference detection", () => {
	test("self-referential table should work (valid use case)", () => {
		// Self-reference is valid (e.g., employee -> manager)
		const Employees = table("employees", {
			id: z.string().db.primary(),
			name: z.string(),
			managerId: z.string().nullable(),
		});

		// This is a valid pattern, should not throw
		expect(Employees.name).toBe("employees");
	});
});

// =============================================================================
// Issue #11: findNextPlaceholder safety
// =============================================================================

describe("Issue #11: findNextPlaceholder safety", () => {
	test("unterminated string should not cause infinite loop", () => {
		const {parseTemplate} = require("./query.js");

		// Unterminated single quote - should handle gracefully, not hang
		const strings = ["WHERE name = '", ""] as unknown as TemplateStringsArray;

		// Should complete within reasonable time (not hang)
		const start = Date.now();
		try {
			parseTemplate(strings, ["test"], "postgresql");
		} catch {
			// Error is fine, hanging is not
		}
		const elapsed = Date.now() - start;

		expect(elapsed).toBeLessThan(1000); // Should complete in under 1 second
	});

	test("deeply nested quotes should not cause issues", () => {
		const {parseTemplate} = require("./query.js");

		// Many nested quote patterns
		const strings = [
			`WHERE a = '"'"'"'"'"' AND b = `,
			"",
		] as unknown as TemplateStringsArray;

		const start = Date.now();
		const result = parseTemplate(strings, [123], "postgresql");
		const elapsed = Date.now() - start;

		expect(elapsed).toBeLessThan(100);
		expect(result.params).toEqual([123]);
	});
});

// =============================================================================
// Issue #2: Migration race condition test
// =============================================================================

describe("Issue #2: Migration race condition", () => {
	test("ensureMigrationsTable should be called inside lock", async () => {
		// This is more of an audit than a test - verify the code path
		// The fix ensures #ensureMigrationsTable is called within withMigrationLock

		let lockAcquired = false;
		let tableCreatedWhileLocked = false;

		// Helper to build SQL from template parts
		const buildSql = (strings: TemplateStringsArray): string => {
			return strings.join("?");
		};

		const driver: Driver = {
			supportsReturning: true,
			all: async () => [],
			get: async (strings: TemplateStringsArray) => {
				const sql = buildSql(strings);
				if (sql.includes("MAX(version)")) {
					return {version: 0} as any;
				}
				return null;
			},
			run: async (strings: TemplateStringsArray) => {
				const sql = buildSql(strings);
				if (sql.includes("CREATE TABLE") && sql.includes("_migrations")) {
					tableCreatedWhileLocked = lockAcquired;
				}
				return 0;
			},
			val: async () => null,
			close: async () => {},
			transaction: async (fn) => fn(driver),
			withMigrationLock: async (fn) => {
				lockAcquired = true;
				try {
					return await fn();
				} finally {
					lockAcquired = false;
				}
			},
		};

		const db = new Database(driver);
		await db.open(1);

		// The migrations table should be created while the lock is held
		expect(tableCreatedWhileLocked).toBe(true);
	});
});
