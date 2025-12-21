/**
 * Driver-level encode/decode tests (Node.js)
 *
 * Tests encodeValue() and decodeValue() for better-sqlite3 driver.
 * Run with: npm run test:node
 */

import {describe, test, expect} from "./node-test-utils.js";
import {table, extendZod, inferFieldType, decodeData} from "./table.js";
import {encodeData} from "./database.js";
import {Database} from "./database.js";
import {z} from "zod";
import SQLiteDriver from "../sqlite.js";

// Extend Zod before using .db extensions
extendZod(z);

describe("inferFieldType", () => {
	test("infers text for string schema", () => {
		const schema = z.string();
		expect(inferFieldType(schema)).toBe("text");
	});

	test("infers real for number schema", () => {
		const schema = z.number();
		expect(inferFieldType(schema)).toBe("real");
	});

	test("infers boolean for boolean schema", () => {
		const schema = z.boolean();
		expect(inferFieldType(schema)).toBe("boolean");
	});

	test("infers datetime for date schema", () => {
		const schema = z.date();
		expect(inferFieldType(schema)).toBe("datetime");
	});

	test("infers json for object schema", () => {
		const schema = z.object({foo: z.string()});
		expect(inferFieldType(schema)).toBe("json");
	});

	test("infers json for array schema", () => {
		const schema = z.array(z.string());
		expect(inferFieldType(schema)).toBe("json");
	});

	test("unwraps optional schemas", () => {
		const schema = z.date().optional();
		expect(inferFieldType(schema)).toBe("datetime");
	});

	test("unwraps nullable schemas", () => {
		const schema = z.boolean().nullable();
		expect(inferFieldType(schema)).toBe("boolean");
	});

	test("unwraps deeply nested optional/nullable", () => {
		const schema = z.object({x: z.number()}).optional().nullable();
		expect(inferFieldType(schema)).toBe("json");
	});
});

describe("SQLiteDriver encodeValue", () => {
	test("encodes Date to ISO string", () => {
		const driver = new SQLiteDriver(":memory:");
		const date = new Date("2024-01-15T10:30:00.000Z");

		const encoded = driver.encodeValue(date, "datetime");
		expect(encoded).toBe("2024-01-15T10:30:00.000Z");

		driver.close();
	});

	test("encodes boolean to 1/0", () => {
		const driver = new SQLiteDriver(":memory:");

		expect(driver.encodeValue(true, "boolean")).toBe(1);
		expect(driver.encodeValue(false, "boolean")).toBe(0);

		driver.close();
	});

	test("encodes objects to JSON string", () => {
		const driver = new SQLiteDriver(":memory:");
		const obj = {foo: "bar", num: 42};

		const encoded = driver.encodeValue(obj, "json");
		expect(encoded).toBe('{"foo":"bar","num":42}');

		driver.close();
	});

	test("encodes arrays to JSON string", () => {
		const driver = new SQLiteDriver(":memory:");
		const arr = [1, 2, 3];

		const encoded = driver.encodeValue(arr, "json");
		expect(encoded).toBe("[1,2,3]");

		driver.close();
	});

	test("passes through null", () => {
		const driver = new SQLiteDriver(":memory:");

		expect(driver.encodeValue(null, "datetime")).toBe(null);
		expect(driver.encodeValue(null, "boolean")).toBe(null);
		expect(driver.encodeValue(null, "json")).toBe(null);

		driver.close();
	});

	test("passes through text values", () => {
		const driver = new SQLiteDriver(":memory:");

		expect(driver.encodeValue("hello", "text")).toBe("hello");

		driver.close();
	});
});

describe("SQLiteDriver decodeValue", () => {
	test("decodes ISO string to Date", () => {
		const driver = new SQLiteDriver(":memory:");
		const dateStr = "2024-01-15T10:30:00.000Z";

		const decoded = driver.decodeValue(dateStr, "datetime");
		expect(decoded).toBeInstanceOf(Date);
		expect((decoded as Date).toISOString()).toBe(dateStr);

		driver.close();
	});

	test("decodes 1/0 to boolean", () => {
		const driver = new SQLiteDriver(":memory:");

		expect(driver.decodeValue(1, "boolean")).toBe(true);
		expect(driver.decodeValue(0, "boolean")).toBe(false);

		driver.close();
	});

	test("decodes JSON string to object", () => {
		const driver = new SQLiteDriver(":memory:");
		const jsonStr = '{"foo":"bar","num":42}';

		const decoded = driver.decodeValue(jsonStr, "json");
		expect(decoded).toEqual({foo: "bar", num: 42});

		driver.close();
	});

	test("decodes JSON string to array", () => {
		const driver = new SQLiteDriver(":memory:");
		const jsonStr = "[1,2,3]";

		const decoded = driver.decodeValue(jsonStr, "json");
		expect(decoded).toEqual([1, 2, 3]);

		driver.close();
	});

	test("passes through null", () => {
		const driver = new SQLiteDriver(":memory:");

		expect(driver.decodeValue(null, "datetime")).toBe(null);
		expect(driver.decodeValue(null, "boolean")).toBe(null);
		expect(driver.decodeValue(null, "json")).toBe(null);

		driver.close();
	});

	test("throws on invalid datetime string", () => {
		const driver = new SQLiteDriver(":memory:");

		// Invalid datetime throws an error
		expect(() => driver.decodeValue("not-a-date", "datetime")).toThrow(
			/Invalid date value/,
		);

		driver.close();
	});
});

describe("encodeData with driver", () => {
	test("uses driver.encodeValue for datetime", async () => {
		const driver = new SQLiteDriver(":memory:");

		const events = table("events", {
			id: z.number().int().db.primary(),
			createdAt: z.date(),
		});

		const date = new Date("2024-01-15T10:30:00.000Z");
		const encoded = encodeData(events, {id: 1, createdAt: date}, driver);

		expect(encoded.createdAt).toBe("2024-01-15T10:30:00.000Z");

		await driver.close();
	});

	test("uses driver.encodeValue for boolean", async () => {
		const driver = new SQLiteDriver(":memory:");

		const users = table("users", {
			id: z.number().int().db.primary(),
			active: z.boolean(),
		});

		const encoded = encodeData(users, {id: 1, active: true}, driver);
		expect(encoded.active).toBe(1);

		const encoded2 = encodeData(users, {id: 2, active: false}, driver);
		expect(encoded2.active).toBe(0);

		await driver.close();
	});

	test("uses driver.encodeValue for json", async () => {
		const driver = new SQLiteDriver(":memory:");

		const items = table("items", {
			id: z.number().int().db.primary(),
			metadata: z.object({tags: z.array(z.string())}),
		});

		const encoded = encodeData(
			items,
			{id: 1, metadata: {tags: ["a", "b"]}},
			driver,
		);

		expect(encoded.metadata).toBe('{"tags":["a","b"]}');

		await driver.close();
	});
});

describe("decodeData with driver", () => {
	test("uses driver.decodeValue for datetime", async () => {
		const driver = new SQLiteDriver(":memory:");

		const events = table("events", {
			id: z.number().int().db.primary(),
			createdAt: z.date(),
		});

		const decoded = decodeData(
			events,
			{id: 1, createdAt: "2024-01-15T10:30:00.000Z"},
			driver,
		)!;

		expect(decoded.createdAt).toBeInstanceOf(Date);
		expect((decoded.createdAt as Date).toISOString()).toBe(
			"2024-01-15T10:30:00.000Z",
		);

		await driver.close();
	});

	test("uses driver.decodeValue for boolean", async () => {
		const driver = new SQLiteDriver(":memory:");

		const users = table("users", {
			id: z.number().int().db.primary(),
			active: z.boolean(),
		});

		const decoded = decodeData(users, {id: 1, active: 1}, driver)!;
		expect(decoded.active).toBe(true);

		const decoded2 = decodeData(users, {id: 2, active: 0}, driver)!;
		expect(decoded2.active).toBe(false);

		await driver.close();
	});

	test("uses driver.decodeValue for json", async () => {
		const driver = new SQLiteDriver(":memory:");

		const items = table("items", {
			id: z.number().int().db.primary(),
			metadata: z.object({tags: z.array(z.string())}),
		});

		const decoded = decodeData(
			items,
			{id: 1, metadata: '{"tags":["a","b"]}'},
			driver,
		)!;

		expect(decoded.metadata).toEqual({tags: ["a", "b"]});

		await driver.close();
	});
});

describe("end-to-end driver encode/decode", () => {
	test("round-trips data through SQLite", async () => {
		const driver = new SQLiteDriver(":memory:");
		const db = new Database(driver);
		await db.open(1);

		const events = table("events_e2e_1", {
			id: z.number().int().db.primary(),
			name: z.string(),
			active: z.boolean(),
			createdAt: z.date(),
			metadata: z.object({count: z.number()}),
		});

		await db.ensureTable(events);

		const date = new Date("2024-06-15T12:00:00.000Z");
		await db.insert(events, {
			id: 1,
			name: "Test Event",
			active: true,
			createdAt: date,
			metadata: {count: 42},
		});

		const rows = await db.all(events)``;
		const row = rows[0];

		expect(row.id).toBe(1);
		expect(row.name).toBe("Test Event");
		expect(row.active).toBe(true);
		expect(row.createdAt).toBeInstanceOf(Date);
		expect((row.createdAt as Date).toISOString()).toBe(
			"2024-06-15T12:00:00.000Z",
		);
		expect(row.metadata).toEqual({count: 42});

		await driver.close();
	});

	test("handles nullable fields", async () => {
		const driver = new SQLiteDriver(":memory:");
		const db = new Database(driver);
		await db.open(1);

		const events = table("events_e2e_2", {
			id: z.number().int().db.primary(),
			endedAt: z.date().nullable(),
			settings: z.object({enabled: z.boolean()}).nullable(),
		});

		await db.ensureTable(events);

		// Insert with nulls
		await db.insert(events, {
			id: 1,
			endedAt: null,
			settings: null,
		});

		const rows = await db.all(events)``;
		const row = rows[0];

		expect(row.endedAt).toBe(null);
		expect(row.settings).toBe(null);

		// Insert with values
		const date = new Date("2024-06-15T12:00:00.000Z");
		await db.insert(events, {
			id: 2,
			endedAt: date,
			settings: {enabled: true},
		});

		const rows2 = await db.all(events)`WHERE id = ${2}`;
		const row2 = rows2[0];

		expect(row2.endedAt).toBeInstanceOf(Date);
		expect(row2.settings).toEqual({enabled: true});

		await driver.close();
	});
});

describe("custom field encode/decode priority", () => {
	test("custom encode takes priority over driver.encodeValue", async () => {
		const driver = new SQLiteDriver(":memory:");
		const db = new Database(driver);
		await db.open(1);

		const items = table("items_custom", {
			id: z.number().int().db.primary(),
			tags: z
				.array(z.string())
				.db.encode((arr) => arr.join(","))
				.db.decode((str) => (str as string).split(",")),
		});

		await db.ensureTable(items);

		await db.insert(items, {
			id: 1,
			tags: ["a", "b", "c"],
		});

		// Verify stored as CSV, not JSON
		const rawRows = await driver.all(
			["SELECT tags FROM items_custom WHERE id = 1"] as any,
			[],
		);
		expect((rawRows as any)[0].tags).toBe("a,b,c");

		// Verify decoded back to array
		const rows = await db.all(items)``;
		const row = rows[0];
		expect(row.tags).toEqual(["a", "b", "c"]);

		await driver.close();
	});
});
