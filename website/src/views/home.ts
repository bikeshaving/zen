import {jsx, Raw} from "@b9g/crank/standalone";
import {css} from "@emotion/css";

import {Root} from "../components/root.js";
import {Logo} from "../components/logo.js";
import {highlight, normalizeLang} from "../utils/prism.js";

interface ViewProps {
	url: string;
	params: Record<string, string>;
}

function CodeBlock({code, lang = "typescript"}: {code: string; lang?: string}) {
	const name = normalizeLang(lang);
	const highlighted = highlight(code.trim(), lang);
	return jsx`
		<div class="code-block-container" data-lang=${name}>
			<pre class="language-${name}"><code class="language-${name}"><${Raw} value=${highlighted} /></code></pre>
		</div>
	`;
}

const QUICK_START = `import {z, table, Database} from "@b9g/zen";
import SQLiteDriver from "@b9g/zen/sqlite";

// 1. Define tables with Zod schema
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

// 2. Open the database and create tables
const db = new Database(new SQLiteDriver("file:app.db"));
db.addEventListener("upgradeneeded", (e) => {
  e.waitUntil(db.ensureTable(Users).then(() => db.ensureTable(Posts)));
});
await db.open(1);

// 3. Insert with validation — id is auto-generated
const user = await db.insert(Users, {email: "alice@example.com", name: "Alice"});

// 4. Write raw SQL, get typed & normalized objects back
const posts = await db.all([Posts, Users])\`
  JOIN "users" ON \${Users.on(Posts)}
  WHERE \${Posts.cols.published} = \${true}
\`;

posts[0].author?.name; // "Alice" — resolved from the JOIN, fully typed`;

const NOT = [
	{
		title: "Not a query builder",
		body: "You write SQL directly with tagged templates — no <code class=\"inline\">.where().orderBy().limit()</code> chains. Helpers handle the tedious parts without hiding or limiting your queries.",
	},
	{
		title: "Not an ORM",
		body: "Tables aren't classes. They're Zod-powered singletons that validate writes, generate DDL, and deduplicate joined data. No lazy loading, no session-wide identity map.",
	},
	{
		title: "Not a startup",
		body: "ZenDB is an open-source library, not a venture-backed SaaS. There will never be a managed instance or a “Zen Studio” — just a thin wrapper around Zod and your SQL driver.",
	},
];

const FEATURES = [
	{
		title: "Typed CRUD",
		body: "Insert, update, and query return fully-typed rows inferred straight from your Zod schema — including DB-computed defaults via <code class=\"inline\">RETURNING</code>.",
	},
	{
		title: "Normalized references",
		body: "<code class=\"inline\">all()</code> deduplicates rows by primary key and resolves <code class=\"inline\">references()</code> into real object graphs — forward and reverse — with zero N+1 queries.",
	},
	{
		title: "Idempotent migrations",
		body: "IndexedDB-style <code class=\"inline\">upgradeneeded</code> events with safe, additive-only helpers: <code class=\"inline\">ensureTable</code>, <code class=\"inline\">ensureConstraints</code>, <code class=\"inline\">copyColumn</code>.",
	},
	{
		title: "SQLite · Postgres · MySQL",
		body: "One template API across three dialects. Drivers build native placeholders (<code class=\"inline\">?</code> / <code class=\"inline\">$1</code>) — no SQL parsing, no lock-in.",
	},
	{
		title: "Zod validation on writes",
		body: "Every insert and update is validated against your schema. Reads are never re-validated, so queries stay fast.",
	},
	{
		title: "Raw SQL, always",
		body: "JOINs, CTEs, window functions, dialect-specific syntax — write whatever SQL you need. Normalization is driven by table metadata, not query shape.",
	},
];

const glow = css`
	position: absolute;
	inset: 0;
	z-index: -1;
	background:
		radial-gradient(
			ellipse 60% 50% at 50% 30%,
			rgba(214, 138, 92, 0.22),
			transparent 70%
		);
	pointer-events: none;
`;

const heroTextShadow = `
	text-shadow:
		0 0 20px var(--bg-color),
		0 0 40px var(--bg-color),
		0 0 60px var(--bg-color);
`;

function Hero() {
	return jsx`
		<header class=${css`
			position: relative;
			min-height: 90vh;
			display: flex;
			flex-direction: column;
			justify-content: center;
			align-items: center;
			text-align: center;
			padding: 6rem 1rem 3rem;
			overflow: hidden;
		`}>
			<div class=${glow} />
			<div class=${css`
				color: var(--highlight-color);
				margin-bottom: 0.5rem;
				filter: drop-shadow(0 0 24px rgba(214, 138, 92, 0.5));
			`}>
				<${Logo} width="clamp(64px, 14vw, 120px)" height="clamp(64px, 14vw, 120px)" />
			</div>
			<h1 class=${css`
				color: var(--highlight-color);
				font-size: clamp(48px, 12vw, 120px);
				margin: 0.1em 0;
				padding: 0 0.2em;
				line-height: 1;
				${heroTextShadow}
			`}>ZenDB</h1>
			<p class=${css`
				color: var(--text-color);
				font-size: clamp(20px, 4vw, 34px);
				font-weight: 600;
				margin: 0.4em 0;
				line-height: 1.25;
				${heroTextShadow}
			`}>Define Zod tables.<br />Write raw SQL.<br />Get typed objects.</p>
			<p class=${css`
				opacity: 0.75;
				font-size: clamp(15px, 2.5vw, 18px);
				margin: 0.4em 0 2em;
				max-width: 46ch;
			`}>
				The missing link between SQL and typed data${" — a thin, forward-only layer over Zod and your SQL driver."}
			</p>
			<nav class=${css`
				display: flex;
				flex-wrap: wrap;
				justify-content: center;
				gap: 1rem;
				font-size: 1.05rem;
				margin-bottom: 2.5rem;

				a {
					text-decoration: none;
					padding: 0.55em 1.4em;
					border-radius: 4px;
					border: 1px solid var(--text-color);
					opacity: 0.85;
					transition: opacity 0.15s, color 0.15s;
					&:hover { opacity: 1; color: var(--highlight-color); }
				}
			`}>
				<a href="/guides/getting-started/" class=${css`
					&& {
						background: var(--highlight-color);
						color: var(--bg-color);
						border-color: var(--highlight-color);
						font-weight: bold;
						opacity: 1;
						&:hover { color: var(--bg-color); filter: brightness(1.08); }
					}
				`}>Get Started</a>
				<a href="/guides/api-reference/">API Reference</a>
				<a href="https://github.com/bikeshaving/zen">GitHub</a>
			</nav>
			<div class=${css`width: 100%; max-width: 560px;`}>
				<${CodeBlock} code="npm install @b9g/zen zod better-sqlite3" lang="bash" />
			</div>
		</header>
	`;
}

function SectionHeading({children}: {children: unknown}) {
	return jsx`
		<h2 class=${css`
			text-align: center;
			font-size: clamp(28px, 5vw, 42px);
			color: var(--highlight-color);
			margin: 0 auto 0.4em;
		`}>${children}</h2>
	`;
}

function Card({title, body}: {title: string; body: string}) {
	return jsx`
		<div class=${css`
			border: 1px solid var(--text-color);
			border-color: rgba(128, 128, 128, 0.35);
			border-radius: 8px;
			padding: 1.5rem;
			background: rgba(128, 128, 128, 0.04);
			transition: border-color 0.15s, transform 0.15s;
			&:hover {
				border-color: var(--highlight-color);
				transform: translateY(-2px);
			}
		`}>
			<h3 class=${css`
				margin: 0 0 0.5rem;
				font-size: 1.2rem;
				color: var(--highlight-color);
			`}>${title}</h3>
			<p class=${css`
				margin: 0;
				opacity: 0.85;
				line-height: 1.5;
				font-size: 0.98rem;
			`}><${Raw} value=${body} /></p>
		</div>
	`;
}

const sectionWrap = css`
	max-width: 1100px;
	margin: 0 auto;
	padding: 4rem 1.25rem;
`;

const cardGrid = css`
	display: grid;
	gap: 1.25rem;
	grid-template-columns: repeat(auto-fit, minmax(min(100%, 280px), 1fr));
	margin-top: 2rem;
`;

export default function Home({url}: ViewProps) {
	return jsx`
		<${Root}
			title="ZenDB — Define Zod tables. Write raw SQL. Get typed objects."
			url=${url}
			description="ZenDB is the missing link between SQL and typed data. Define tables with Zod, write raw SQL with tagged templates, and get typed, normalized objects back. Works with SQLite, PostgreSQL, and MySQL."
		>
			<${Hero} />

			<section class=${sectionWrap}>
				<${SectionHeading}>What ZenDB is not<//SectionHeading>
				<div class=${cardGrid}>
					${NOT.map((c) => jsx`<${Card} title=${c.title} body=${c.body} />`)}
				</div>
			</section>

			<section class=${css`
				background: rgba(128, 128, 128, 0.05);
				border-top: 1px solid rgba(128, 128, 128, 0.2);
				border-bottom: 1px solid rgba(128, 128, 128, 0.2);
			`}>
				<div class=${sectionWrap}>
					<${SectionHeading}>Everything you need, nothing you don't<//SectionHeading>
					<div class=${cardGrid}>
						${FEATURES.map((c) => jsx`<${Card} title=${c.title} body=${c.body} />`)}
					</div>
				</div>
			</section>

			<section class=${sectionWrap}>
				<${SectionHeading}>Quick start<//SectionHeading>
				<p class=${css`
					text-align: center;
					opacity: 0.75;
					margin: 0 auto 1rem;
					max-width: 52ch;
				`}>
					Define tables, open the database, and query. That's the whole loop.
				</p>
				<div class=${css`max-width: 820px; margin: 0 auto;`}>
					<${CodeBlock} code=${QUICK_START} lang="typescript" />
				</div>
				<div class=${css`text-align: center; margin-top: 2.5rem;`}>
					<a href="/guides/getting-started/" class=${css`
						display: inline-block;
						background: var(--highlight-color);
						color: var(--bg-color);
						font-weight: bold;
						text-decoration: none;
						padding: 0.7em 1.8em;
						border-radius: 4px;
						&:hover { filter: brightness(1.08); }
					`}>Read the guides${" →"}</a>
				</div>
			</section>
		<//Root>
	`;
}
