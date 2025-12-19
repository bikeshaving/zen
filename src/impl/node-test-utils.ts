/**
 * Bun-compatible test utilities for Node's built-in test runner.
 *
 * Provides describe, test, expect API compatible with Bun tests.
 */

import {describe as nodeDescribe, test as nodeTest} from "node:test";
import assert from "node:assert";

export {describe, test};

// Re-export Node's describe and test
const describe = nodeDescribe;
const test = nodeTest;

// Bun-compatible expect API
export function expect<T>(actual: T) {
	return {
		toBe(expected: T) {
			assert.strictEqual(actual, expected);
		},
		toEqual(expected: T) {
			assert.deepStrictEqual(actual, expected);
		},
		toBeNull() {
			assert.strictEqual(actual, null);
		},
		toBeUndefined() {
			assert.strictEqual(actual, undefined);
		},
		toBeTruthy() {
			assert.ok(actual);
		},
		toBeFalsy() {
			assert.ok(!actual);
		},
		toBeGreaterThan(expected: number) {
			assert.ok((actual as any) > expected);
		},
		toBeLessThan(expected: number) {
			assert.ok((actual as any) < expected);
		},
		toContain(expected: any) {
			if (Array.isArray(actual)) {
				assert.ok(actual.includes(expected));
			} else if (typeof actual === "string") {
				assert.ok(actual.includes(expected));
			} else {
				throw new Error("toContain expects an array or string");
			}
		},
		toMatch(expected: RegExp) {
			assert.match(actual as any, expected);
		},
		toThrow(expected?: string | RegExp | Function) {
			if (typeof actual !== "function") {
				throw new Error("toThrow expects a function");
			}
			if (expected) {
				assert.throws(actual as any, expected as any);
			} else {
				assert.throws(actual as any);
			}
		},
		not: {
			toBe(expected: T) {
				assert.notStrictEqual(actual, expected);
			},
			toEqual(expected: T) {
				assert.notDeepStrictEqual(actual, expected);
			},
			toBeNull() {
				assert.notStrictEqual(actual, null);
			},
		},
		rejects: {
			async toThrow(expected?: string | RegExp | Function) {
				if (!(actual instanceof Promise)) {
					throw new Error("rejects.toThrow expects a Promise");
				}
				if (expected) {
					await assert.rejects(actual, expected as any);
				} else {
					await assert.rejects(actual);
				}
			},
		},
	};
}

// beforeEach hook
export function beforeEach(fn: () => void | Promise<void>) {
	// Node's test runner doesn't have beforeEach in the same way
	// We'll need to call it manually in each test or use a different pattern
	// For now, just export it for compatibility
	return fn;
}
