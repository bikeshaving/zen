import {test, expect, describe} from "bun:test";
import {
	z,
	table,
	view,
	isTable,
	isView,
	extendZod,
	NOW,
	CURRENT_TIMESTAMP,
	ValidationError,
	type Row,
	type Insert,
} from "../src/schema.js";

describe("@b9g/zen/schema", () => {
	describe("table definition without a database", () => {
		test("defines tables and validates, with no driver or connection", () => {
			const Users = table("users", {
				id: z.string().uuid().db.primary().db.auto(),
				email: z.string().email().db.unique(),
				name: z.string().max(100),
				role: z.enum(["user", "admin"]),
			});

			expect(isTable(Users)).toBe(true);
			expect(Users.name).toBe("users");

			// The whole point: validate a form payload client-side.
			const ok = Users.schema.safeParse({
				id: "550e8400-e29b-41d4-a716-446655440000",
				email: "alice@example.com",
				name: "Alice",
				role: "admin",
			});
			expect(ok.success).toBe(true);

			const bad = Users.schema.safeParse({
				id: "550e8400-e29b-41d4-a716-446655440000",
				email: "not-an-email",
				name: "Alice",
				role: "admin",
			});
			expect(bad.success).toBe(false);
		});

		test("field metadata is available for form generation", () => {
			const Users = table("users", {
				id: z.string().uuid().db.primary(),
				email: z.string().email().db.unique(),
				name: z.string(),
			});

			const fields = Users.fields();
			expect(fields.email.name).toBe("email");
			expect(fields.email.db.unique).toBe(true);
			expect(fields.id.db.primaryKey).toBe(true);
		});

		test("views work", () => {
			const Users = table("users", {
				id: z.string().db.primary(),
				role: z.enum(["user", "admin"]),
			});
			const Admins = view("admin_users", Users)`
				WHERE ${Users.cols.role} = ${"admin"}
			`;
			expect(isView(Admins)).toBe(true);
		});

		test("SQL builtins are plain registered symbols", () => {
			// They carry no runtime, which is why they can cross this entrypoint.
			expect(typeof NOW).toBe("symbol");
			expect(NOW).toBe(CURRENT_TIMESTAMP);
			expect(NOW).toBe(Symbol.for("@b9g/zen:CURRENT_TIMESTAMP"));

			const Posts = table("posts", {
				id: z.string().db.primary(),
				createdAt: z.date().db.inserted(NOW),
			});
			expect(isTable(Posts)).toBe(true);
		});

		test("exports validation errors and extendZod", () => {
			expect(typeof extendZod).toBe("function");
			expect(typeof ValidationError).toBe("function");
		});

		test("row/insert types are re-exported", () => {
			const Users = table("users", {
				id: z.string().uuid().db.primary(),
				email: z.string().email(),
			});
			const row: Row<typeof Users> = {id: "x", email: "a@b.com"};
			const draft: Insert<typeof Users> = {id: "x", email: "a@b.com"};
			expect(row.id).toBe("x");
			expect(draft.email).toBe("a@b.com");
		});
	});

	// The reason this entrypoint exists (#7): it must not drag the database
	// runtime into a client bundle. Assert that structurally rather than trusting
	// the import graph to stay clean.
	describe("bundle isolation", () => {
		async function bundle(entry: string): Promise<string> {
			const built = await Bun.build({
				entrypoints: [entry],
				target: "browser",
				external: ["zod"],
			});
			expect(built.success).toBe(true);
			return await built.outputs[0].text();
		}

		// NB: match on unambiguous markers. "class Database" is a substring of
		// "class DatabaseError", which legitimately *is* in the schema bundle.
		test("schema entrypoint excludes Database, Transaction and migrations", async () => {
			const code = await bundle("./src/schema.ts");

			expect(code).not.toContain("class Database extends EventTarget");
			expect(code).not.toContain("class DatabaseUpgradeEvent");
			expect(code).not.toContain("upgradeneeded");
		});

		test("main entrypoint does include it (control)", async () => {
			// If this ever stops being true, the assertions above prove nothing.
			const code = await bundle("./src/zen.ts");

			expect(code).toContain("class Database extends EventTarget");
			expect(code).toContain("upgradeneeded");
		});

		test("schema bundle is substantially smaller than the main bundle", async () => {
			const schema = await bundle("./src/schema.ts");
			const main = await bundle("./src/zen.ts");
			expect(schema.length).toBeLessThan(main.length);
		});
	});
});
