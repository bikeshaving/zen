/**
 * Compile-time tests for the public API.
 *
 * This file is never executed. It exists because nothing else typechecks the
 * public surface: `bun test` does not typecheck at all, and `tsconfig.json`
 * used to include only `src/**`. That gap is how `db.insert()` shipped unable
 * to compile the README's own Quick Start — the runtime generated the id
 * correctly, but TypeScript rejected the call.
 *
 * Anything asserted here is checked by `npm run typecheck`. Type errors in this
 * file are test failures.
 */
import {
	z,
	table,
	Database,
	NOW,
	type Insert,
	type Row,
	type Update,
} from "../../src/zen.js";

declare const db: Database;

// ============================================================================
// The README Quick Start, verbatim. This must compile.
// ============================================================================

const Users = table("users", {
	id: z.string().uuid().db.primary().db.auto(),
	email: z.string().email().db.unique(),
	name: z.string(),
});

const Posts = table("posts", {
	id: z.string().uuid().db.primary().db.auto(),
	authorId: z.string().uuid().db.references(Users, "author"),
	title: z.string(),
	published: z.boolean().db.inserted(() => false),
});

export async function readmeQuickStart() {
	// `id` is omitted: it is .db.auto().
	const user = await db.insert(Users, {
		email: "alice@example.com",
		name: "Alice",
	});

	// `id` and `published` are omitted: .db.auto() and .db.inserted().
	await db.insert(Posts, {authorId: user.id, title: "Hello"});

	// The generated id is present on the returned row.
	const id: string = user.id;
	return id;
}

// ============================================================================
// Insert: generated fields optional, everything else still required.
// ============================================================================

const Timestamps = table("timestamps", {
	id: z.string().uuid().db.primary().db.auto(),
	title: z.string(),
	createdAt: z.date().db.inserted(NOW),
	updatedAt: z.date().db.upserted(NOW),
});

// Every generated field may be omitted...
export const minimalInsert: Insert<typeof Timestamps> = {title: "Hello"};

// ...and may still be supplied explicitly.
export const explicitInsert: Insert<typeof Timestamps> = {
	id: "e5b7c1f0-0000-4000-8000-000000000000",
	title: "Hello",
	createdAt: new Date(),
	updatedAt: new Date(),
};

// A field with no .db generator stays required.
// @ts-expect-error - `title` is required
export const missingRequired: Insert<typeof Timestamps> = {};

// @ts-expect-error - `name` is required
export const missingName: Insert<typeof Users> = {email: "a@b.com"};

// Unknown fields are still rejected. (The excess-property error is reported on
// the offending property, so the directive has to sit directly above it.)
export const unknownField: Insert<typeof Users> = {
	email: "a@b.com",
	name: "Alice",
	// @ts-expect-error - `nope` is not a field
	nope: true,
};

// The brand must survive chaining in either order.
const EitherOrder = table("either_order", {
	a: z.string().uuid().db.primary().db.auto(),
	b: z.string().uuid().db.auto().db.unique(),
	c: z.string(),
});
export const chainOrder: Insert<typeof EitherOrder> = {c: "required"};

// ============================================================================
// Row: generated fields are present after a read, not optional.
// ============================================================================

export function rowKeepsGeneratedFields(row: Row<typeof Timestamps>) {
	const id: string = row.id;
	const createdAt: Date = row.createdAt;
	return {id, createdAt};
}

// ============================================================================
// Update: still all-optional.
// ============================================================================

export const emptyUpdate: Update<typeof Users> = {};
export const partialUpdate: Update<typeof Users> = {name: "Bob"};
