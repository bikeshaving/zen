import Prism from "prismjs";

// Prism language components are CJS side-effect modules that reference a global
// `Prism` variable. In bundled ESM the core import only creates a local
// binding, so expose it globally for language components to find.
(globalThis as any).Prism = Prism;
Prism.manual = true;

// Map fenced-code language hints to registered Prism grammars.
const ALIASES: Record<string, string> = {
	ts: "typescript",
	typescript: "typescript",
	tsx: "tsx",
	js: "javascript",
	javascript: "javascript",
	jsx: "jsx",
	json: "json",
	sh: "bash",
	shell: "bash",
	bash: "bash",
	sql: "sql",
	diff: "diff",
};

function escapeHTML(text: string): string {
	return text
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;");
}

/**
 * Normalize a fenced-code language token (e.g. "ts", "typescript live") to a
 * canonical Prism language name, or "text" when unknown.
 */
export function normalizeLang(lang: string): string {
	const first = (lang || "").trim().split(/\s+/)[0] || "";
	return ALIASES[first] || "text";
}

/**
 * Server-side syntax highlighting. Returns an HTML string (safe to inject via
 * Raw) with Prism token markup, or HTML-escaped plain text for unknown langs.
 */
export function highlight(code: string, lang: string): string {
	const name = normalizeLang(lang);
	const grammar = (Prism.languages as any)[name];
	if (!grammar) {
		return escapeHTML(code);
	}
	return Prism.highlight(code, grammar, name);
}
