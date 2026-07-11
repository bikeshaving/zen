/**
 * Verify that every internal link in the generated site resolves to a file.
 * Runs against dist/public after `npm run static`. Exits non-zero on any
 * broken internal link. External (http/https), mailto, and #anchor links are
 * skipped. Run with: bun run check-links
 */
import {readFileSync, existsSync, readdirSync, statSync} from "node:fs";
import {join, dirname} from "node:path";
import {fileURLToPath} from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..", "dist", "public");

function walk(dir: string): string[] {
	const out: string[] = [];
	for (const name of readdirSync(dir)) {
		const full = join(dir, name);
		if (statSync(full).isDirectory()) out.push(...walk(full));
		else if (name.endsWith(".html")) out.push(full);
	}
	return out;
}

function resolves(pathname: string): boolean {
	if (pathname === "/") pathname = "/index.html";
	const rel = pathname.replace(/^\//, "");
	const candidates = [
		join(root, rel),
		join(root, rel, "index.html"),
		join(root, rel.endsWith("/") ? rel + "index.html" : rel + ".html"),
	];
	return candidates.some((c) => existsSync(c));
}

if (!existsSync(root)) {
	console.error(`No build found at ${root}. Run \`npm run static\` first.`);
	process.exit(1);
}

const hrefRe = /href="([^"#]+)"/g;
let broken = 0;
let checked = 0;

for (const file of walk(root)) {
	const html = readFileSync(file, "utf8");
	let m: RegExpExecArray | null;
	while ((m = hrefRe.exec(html))) {
		const href = m[1];
		if (/^(https?:|mailto:|tel:|data:)/.test(href)) continue;
		if (!href.startsWith("/")) continue;
		if (href.startsWith("/static/") || href.startsWith("/pagefind/")) continue;
		checked++;
		if (!resolves(href)) {
			console.error(`Broken link ${href} in ${file.replace(root, "")}`);
			broken++;
		}
	}
}

console.log(`Checked ${checked} internal links.`);
if (broken > 0) {
	console.error(`${broken} broken link(s).`);
	process.exit(1);
}
console.log("All internal links OK.");
