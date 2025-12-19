/**
 * PostgreSQL driver schema management tests (Node.js)
 *
 * Tests ensureTable() and ensureConstraints() for postgres driver.
 * Run with: npm run test:node
 *
 * Note: Requires PostgreSQL to be running. Skips if unavailable.
 */

import {describe, test, expect} from "./node-test-utils.js";
import {table} from "./table.js";
import {z} from "zod";
import {SchemaDriftError} from "./errors.js";

// Connection string matching docker-compose.yml
const POSTGRES_URL = "postgresql://testuser:testpass@localhost:15432/test_db";

// Check if PostgreSQL is available
let PostgresDriver: any;
let postgresAvailable = false;

try {
	const module = await import("../postgres.js");
	PostgresDriver = module.default;

	// Try to connect
	const testDriver = new PostgresDriver(POSTGRES_URL);
	try {
		await testDriver.run(["SELECT 1"] as any, []);
		postgresAvailable = true;
		await testDriver.close();
	} catch {
		postgresAvailable = false;
	}
} catch {
	postgresAvailable = false;
}

if (postgresAvailable) {
	describe("PostgresDriver Schema Management", () => {
		test("ensureTable() creates new table", async () => {
			const driver = new PostgresDriver(POSTGRES_URL);

			const users = table("test_users_create", {
				id: z.number().int().db.primary(),
				email: z.string(),
			});

			try {
				const result = await driver.ensureTable(users);
				expect(result.applied).toBe(true);

				// Cleanup
				await driver.run(["DROP TABLE IF EXISTS test_users_create"] as any, []);
			} finally {
				await driver.close();
			}
		});

		test("ensureTable() detects missing unique constraint", async () => {
			const driver = new PostgresDriver(POSTGRES_URL);

			try {
				// Create table without unique constraint
				await driver.run(
					[
						"CREATE TABLE test_users_unique (id SERIAL PRIMARY KEY, email TEXT)",
					] as any,
					[],
				);

				const users = table("test_users_unique", {
					id: z.number().int().db.primary(),
					email: z.string().db.unique(),
				});

				await expect(driver.ensureTable(users)).rejects.toThrow(
					SchemaDriftError,
				);

				// Cleanup
				await driver.run(["DROP TABLE IF EXISTS test_users_unique"] as any, []);
			} finally {
				await driver.close();
			}
		});

		test("ensureConstraints() applies foreign key with preflight", async () => {
			const driver = new PostgresDriver(POSTGRES_URL);

			try {
				const users = table("test_users_fk", {
					id: z.number().int().db.primary(),
				});

				// Create posts table WITHOUT foreign key first
				const posts1 = table("test_posts_fk", {
					id: z.number().int().db.primary(),
					userId: z.number().int(),
				});

				await driver.ensureTable(users);
				await driver.ensureTable(posts1);

				// Now try to add FK with ensureConstraints
				const posts2 = table("test_posts_fk", {
					id: z.number().int().db.primary(),
					userId: z.number().int().db.references(users, "author"),
				});

				const result = await driver.ensureConstraints(posts2);
				expect(result.applied).toBe(true);

				// Cleanup
				await driver.run(["DROP TABLE IF EXISTS test_posts_fk"] as any, []);
				await driver.run(["DROP TABLE IF EXISTS test_users_fk"] as any, []);
			} finally {
				await driver.close();
			}
		});
	});
} else {
	describe.skip("PostgresDriver Schema Management (PostgreSQL not available)", () => {
		test("skipped", () => {});
	});
}
