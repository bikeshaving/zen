/**
 * Tests for transaction behavior and concurrency safety.
 *
 * These tests verify that:
 * 1. Operations within a transaction use the transaction connection
 * 2. Concurrent transactions don't interfere with each other
 */

import {test, expect, describe, mock} from "bun:test";
import {z} from "zod";
import {table, extendZod} from "./table.js";
import {Database, type Driver} from "./database.js";

// Extend Zod once before tests
extendZod(z);

const Users = table("users", {
	id: z.string().uuid().db.primary(),
	name: z.string(),
});

describe("Transaction connection binding", () => {
	test("operations within transaction should use transaction connection", async () => {
		// Track which "connection" each operation used
		const operationConnections: string[] = [];
		let connectionCounter = 0;

		const createTrackedDriver = (): Driver => {
			const mainConnectionId = `main-${++connectionCounter}`;

			return {
				supportsReturning: true,
				all: mock(async () => {
					operationConnections.push(mainConnectionId);
					return [];
				}) as Driver["all"],
				get: mock(async () => {
					operationConnections.push(mainConnectionId);
					return null;
				}) as Driver["get"],
				run: mock(async () => {
					operationConnections.push(mainConnectionId);
					return 1;
				}) as Driver["run"],
				val: mock(async () => 0) as Driver["val"],
				close: mock(async () => {}),
				transaction: mock(
					async <T>(fn: (txDriver: Driver) => Promise<T>): Promise<T> => {
						// Create a transaction-specific connection ID
						const txConnectionId = `tx-${++connectionCounter}`;

						// Create a transaction-bound driver
						const txDriver: Driver = {
							supportsReturning: true,
							all: mock(async () => {
								operationConnections.push(txConnectionId);
								return [];
							}) as Driver["all"],
							get: mock(async () => {
								operationConnections.push(txConnectionId);
								return null;
							}) as Driver["get"],
							run: mock(async () => {
								operationConnections.push(txConnectionId);
								return 1;
							}) as Driver["run"],
							val: mock(async () => 0) as Driver["val"],
							close: mock(async () => {}),
							transaction: mock(async () => {
								throw new Error("Nested transactions not supported");
							}) as Driver["transaction"],
						};

						return await fn(txDriver);
					},
				) as Driver["transaction"],
			};
		};

		const driver = createTrackedDriver();
		const db = new Database(driver);

		// Execute a transaction with multiple operations
		await db.transaction(async (tx) => {
			await tx.all(Users)``;
			await tx.get(Users)`WHERE name = ${"test"}`;
			await tx.exec`SELECT 1`;
		});

		// All operations should have used the same transaction connection
		expect(operationConnections.length).toBe(3);
		const txConnections = operationConnections.filter((c) =>
			c.startsWith("tx-"),
		);
		expect(txConnections.length).toBe(3);
		// All should be the same transaction connection
		expect(new Set(txConnections).size).toBe(1);
	});

	test("concurrent transactions should not interfere with each other", async () => {
		// Track which connection each operation used
		const operations: Array<{txId: number; connectionId: string}> = [];
		let connectionCounter = 0;

		const createConcurrencyTestDriver = (): Driver => {
			return {
				supportsReturning: true,
				all: mock(async () => []) as Driver["all"],
				get: mock(async () => null) as Driver["get"],
				run: mock(async () => 1) as Driver["run"],
				val: mock(async () => 0) as Driver["val"],
				close: mock(async () => {}),
				transaction: mock(
					async <T>(fn: (txDriver: Driver) => Promise<T>): Promise<T> => {
						// Capture the transaction ID at start time
						const txId = ++connectionCounter;
						const txConnectionId = `tx-${txId}`;

						const txDriver: Driver = {
							supportsReturning: true,
							all: mock(async () => {
								// Use captured txId, not the current connectionCounter
								operations.push({txId, connectionId: txConnectionId});
								return [];
							}) as Driver["all"],
							get: mock(async () => null) as Driver["get"],
							run: mock(async () => 1) as Driver["run"],
							val: mock(async () => 0) as Driver["val"],
							close: mock(async () => {}),
							transaction: mock(async () => {
								throw new Error("Nested transactions not supported");
							}) as Driver["transaction"],
						};

						return await fn(txDriver);
					},
				) as Driver["transaction"],
			};
		};

		const driver = createConcurrencyTestDriver();
		const db = new Database(driver);

		// Run two transactions concurrently
		const [result1, result2] = await Promise.all([
			db.transaction(async (tx) => {
				await tx.all(Users)``;
				// Yield to allow interleaving
				await new Promise((resolve) => setTimeout(resolve, 10));
				await tx.all(Users)``;
				return "tx1";
			}),
			db.transaction(async (tx) => {
				await tx.all(Users)``;
				await new Promise((resolve) => setTimeout(resolve, 10));
				await tx.all(Users)``;
				return "tx2";
			}),
		]);

		expect(result1).toBe("tx1");
		expect(result2).toBe("tx2");

		// Should have 4 operations total (2 per transaction)
		expect(operations.length).toBe(4);

		// Group operations by connection
		const byConnection = new Map<string, number[]>();
		for (const op of operations) {
			const existing = byConnection.get(op.connectionId) || [];
			existing.push(op.txId);
			byConnection.set(op.connectionId, existing);
		}

		// There should be 2 distinct connections (one per transaction)
		expect(byConnection.size).toBe(2);

		// Each connection should have exactly 2 operations from the same transaction
		for (const [_connectionId, txIds] of byConnection) {
			expect(txIds.length).toBe(2);
			// All txIds in this connection should be the same
			expect(new Set(txIds).size).toBe(1);
		}
	});
});

describe("Transaction rollback on error", () => {
	test("transaction should be rolled back if function throws", async () => {
		let commitCalled = false;
		let rollbackCalled = false;

		const driver: Driver = {
			supportsReturning: true,
			all: mock(async () => []) as Driver["all"],
			get: mock(async () => null) as Driver["get"],
			run: mock(async (strings: TemplateStringsArray) => {
				const sql = strings.join("?");
				if (sql.includes("COMMIT")) commitCalled = true;
				if (sql.includes("ROLLBACK")) rollbackCalled = true;
				return 1;
			}) as Driver["run"],
			val: mock(async () => 0) as Driver["val"],
			close: mock(async () => {}),
			transaction: mock(
				async <T>(fn: (txDriver: Driver) => Promise<T>): Promise<T> => {
					// Simulate a real transaction that tracks commit/rollback
					try {
						const result = await fn(driver);
						commitCalled = true;
						return result;
					} catch (error) {
						rollbackCalled = true;
						throw error;
					}
				},
			) as Driver["transaction"],
		};

		const db = new Database(driver);

		// Transaction that throws
		await expect(
			db.transaction(async (_tx) => {
				throw new Error("Something went wrong");
			}),
		).rejects.toThrow("Something went wrong");

		// Rollback should have been called, not commit
		expect(rollbackCalled).toBe(true);
		expect(commitCalled).toBe(false);
	});
});
