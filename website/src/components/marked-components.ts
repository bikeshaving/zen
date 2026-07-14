import {jsx, Raw} from "@b9g/crank/standalone";
import {highlight, normalizeLang} from "../utils/prism.js";

function resolveMarkdownHref(href: string, basePath: string): string {
	const baseParts = basePath.split("/").filter(Boolean);
	for (const part of href.split("/")) {
		if (part === "..") {
			baseParts.pop();
		} else if (part !== ".") {
			baseParts.push(part);
		}
	}

	return (
		"/" +
		baseParts
			.join("/")
			.replace(/\.md$/, "")
			.replace(/([0-9]+-)+/, "")
	);
}

export const components = {
	link({token, rootProps, children}: any) {
		const {href, title} = token;
		const resolvedHref =
			href && href.endsWith(".md") && rootProps.basePath
				? resolveMarkdownHref(href, rootProps.basePath)
				: href;
		return jsx`<a href=${resolvedHref} title=${title}>${children}</a>`;
	},

	codespan({token}: any) {
		return jsx`<code class="inline">${token.text}</code>`;
	},

	code({token}: any) {
		const {text: code, lang} = token;
		const name = normalizeLang(lang);
		const highlighted = highlight(code, lang);

		// Static, server-highlighted code block. The copy button is attached
		// client-side by clients/copy.ts.
		return jsx`
			<div class="code-block-container" data-lang=${name}>
				<pre class="language-${name}"><code class="language-${name}"><${Raw} value=${highlighted} /></code></pre>
			</div>
		`;
	},
};
