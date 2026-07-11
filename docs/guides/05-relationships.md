---
title: Relationships
description: How ZenDB normalizes query results into deduplicated object graphs, resolves forward and reverse references, and keeps JSON serialization cycle-free.
---

Normalization is driven by table metadata, not query shape — so your SQL stays
unrestricted. When you pass multiple tables to `all()` / `get()`, ZenDB:

1. Generates a `SELECT` with prefixed column aliases (`posts.id AS "posts.id"`)
2. Parses rows into per-table entities
3. Deduplicates by primary key (same PK = same object instance)
4. Resolves `references()` into actual entity objects (forward and reverse)

Relationship properties are typed based on your `references()` declarations. They
can be `null` when the foreign key is missing or the JOIN yields no row, so use
optional chaining.

## Forward references (belongs-to)

```typescript
const Posts = table("posts", {
  id: z.string().db.primary(),
  authorId: z.string().db.references(Users, "author"),
  title: z.string(),
});

const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
`;
posts[0].author?.name; // typed as string | undefined
```

## Reverse references (has-many)

Use `reverseAs` to populate arrays of referencing entities:

```typescript
const Posts = table("posts", {
  id: z.string().db.primary(),
  authorId: z.string().db.references(Users, "author", {
    reverseAs: "posts", // populate author.posts = Post[]
  }),
  title: z.string(),
});

const posts = await db.all([Posts, Users])`
  JOIN "users" ON ${Users.on(Posts)}
`;
posts[0].author?.posts; // [{id: "p1", ...}, {id: "p2", ...}]
```

> Reverse relationships are runtime-only materializations that reflect data in
> the current result set. There are no automatic JOINs, lazy loading, or cascade
> fetching — related data comes from *your* JOINs.

## Many-to-many

```typescript
const PostTags = table("post_tags", {
  id: z.string().db.primary(),
  postId: z.string().db.references(Posts, "post", {reverseAs: "postTags"}),
  tagId: z.string().db.references(Tags, "tag", {reverseAs: "postTags"}),
});

const results = await db.all([PostTags, Posts, Tags])`
  JOIN "posts" ON ${Posts.on(PostTags)}
  JOIN "tags"  ON ${Tags.on(PostTags)}
  WHERE ${Posts.cols.id} = ${"p1"}
`;

// Access through the join table:
const tags = results.map((pt) => pt.tag);

// Or via the reverse relationship:
results[0].post?.postTags?.forEach((pt) => console.log(pt.tag?.name));
```

## Serialization rules

References have specific serialization behavior to prevent circular JSON:

```typescript
const post = posts[0];

// Forward references (belongs-to): enumerable and immutable
Object.keys(post);    // ["id", "title", "authorId", "author"]
JSON.stringify(post); // includes "author"

// Reverse references (has-many): non-enumerable and immutable
const author = post.author;
Object.keys(author);    // ["id", "name"]  (no "posts")
JSON.stringify(author); // excludes "posts" (prevents a cycle)
author.posts;           // still accessible — just hidden from enumeration
```

Because reverse refs are non-enumerable, `JSON.stringify(post)` never throws on
a cycle. When you *do* want a reverse ref in the output, spread it explicitly:

```typescript
const explicit = {...author, posts: author.posts};
JSON.stringify(explicit); // now includes posts
```

**Why this design:** forward refs are safe to serialize (no cycles by
themselves); reverse refs create cycles when paired with a forward ref. Making
reverse refs non-enumerable prevents accidental circular-JSON errors, and both
are immutable because these are query results, not mutable records.

## Performance

- Normalization is `O(rows)` with hash maps per table
- Reference resolution is zero-cost after deduplication
- Zod validation happens on writes, never on reads
