import {test, expect, describe} from "bun:test";
import {z} from "zod";
import {extendZod, table} from "../src/impl/table.js";

// Extend Zod once before tests
extendZod(z);

describe(".db namespace API", () => {
	test("prototype extension loaded", () => {
		const schema = z.string();

		// Verify .db property exists
		expect("db" in schema).toBe(true);
		expect(typeof schema.db).toBe("object");
		expect(typeof schema.db.primary).toBe("function");
	});

	test("z.string().db.primary() works", () => {
		const Users = table("users", {
			id: z.string().uuid().db.primary(),
			name: z.string(),
		});

		expect(Users.meta.primary).toBe("id");
	});

	test("z.string().db.unique() works", () => {
		const Users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email().db.unique(),
		});

		expect(Users.meta.unique).toContain("email");
	});

	test("z.date().db.index() works", () => {
		const Posts = table("posts", {
			id: z.string().db.primary(),
			createdAt: z.date().db.index(),
		});

		expect(Posts.meta.indexed).toContain("createdAt");
	});

	test("z.date().db.softDelete() works", () => {
		const Users = table("users", {
			id: z.string().db.primary(),
			deletedAt: z.date().nullable().db.softDelete(),
		});

		expect(Users.meta.softDeleteField).toBe("deletedAt");
	});

	test("z.string().db.references() works", () => {
		const Users = table("users", {
			id: z.string().db.primary(),
			name: z.string(),
		});

		const Posts = table("posts", {
			id: z.string().db.primary(),
			authorId: z.string().db.references(Users, {as: "author"}),
		});

		const refs = Posts.references();
		expect(refs).toHaveLength(1);
		expect(refs[0].as).toBe("author");
		expect(refs[0].table).toBe(Users);
	});

	test("chaining .db methods works", () => {
		const Users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email().db.unique().db.index(),
		});

		expect(Users.meta.primary).toBe("id");
		expect(Users.meta.unique).toContain("email");
		expect(Users.meta.indexed).toContain("email");
	});

	test("mixing Zod and .db methods works", () => {
		const Users = table("users", {
			id: z.string().uuid().db.primary(),
			email: z.string().email().db.unique(),
			age: z.number().min(0).max(150).optional(),
			role: z.enum(["user", "admin"]),
		});

		expect(Users.meta.primary).toBe("id");
		expect(Users.meta.unique).toContain("email");
	});

	test(".db.encode() and .db.decode() store metadata", () => {
		const Users = table("users", {
			id: z.string().db.primary(),
			password: z.string().db.encode((pw: string) => `hashed_${pw}`),
			legacy: z.string().db.decode((val: string) => val.toUpperCase()),
		});

		const passwordMeta = Users.meta.fields["password"];
		expect(passwordMeta.encode).toBeDefined();

		const legacyMeta = Users.meta.fields["legacy"];
		expect(legacyMeta.decode).toBeDefined();
	});
});
