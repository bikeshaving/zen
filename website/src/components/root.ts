import {jsx, Raw} from "@b9g/crank/standalone";
import type {Children, Context} from "@b9g/crank";
import {extractCritical} from "@emotion/server";
import {Navbar} from "./navbar.js";
import {Footer} from "./footer.js";
import {getColorSchemeScript} from "../utils/color-scheme.js";
import {assets, SITE_URL} from "../server.js";

function ColorSchemeScript() {
	// This script must be executed as early as possible to prevent a FOUC.
	// It also cannot be `type="module"` because that will also cause an FOUC.
	const scriptText = `(() => { ${getColorSchemeScript()} })()`;
	return jsx`
		<script>
			<${Raw} value=${scriptText} />
		</script>
	`;
}

export function* Root(
	this: Context,
	{
		title,
		children,
		url,
		description = "",
		noFooter = false,
	}: {
		title: string;
		children: Children;
		url: string;
		description?: string;
		noFooter?: boolean;
	},
) {
	for ({title, children, url, description = "", noFooter = false} of this) {
		this.schedule(() => this.refresh());
		const childrenHTML: string = yield jsx`
			<div id="navbar-root">
				<${Navbar} url=${url} />
			</div>
			${children}
			${!noFooter && jsx`<${Footer} />`}
		`;
		const {html, css} = extractCritical(childrenHTML);
		const ogImage = assets.ogImage ? `${SITE_URL}${assets.ogImage}` : undefined;
		yield jsx`
			<${Raw} value="<!DOCTYPE html>" />
			<html lang="en">
				<head>
					<meta charset="UTF-8" />
					<meta name="viewport" content="width=device-width" />
					<title>${title}</title>
					<link rel="icon" type="image/svg+xml" href=${assets.logo} />
					<link rel="apple-touch-icon" href=${assets.logo} />
					<style><${Raw} value=${css} /></style>
					<link rel="stylesheet" type="text/css" href=${assets.clientCSS} />
					<meta name="description" content=${description} />
					<meta property="og:title" content=${title} />
					<meta property="og:url" content=${`${SITE_URL}${url}`} />
					<meta property="og:description" content=${description} />
					<meta property="og:type" content="website" />
					<meta property="og:site_name" content="ZenDB" />
					${ogImage && jsx`<meta property="og:image" content=${ogImage} />`}
					<meta name="twitter:card" content=${ogImage ? "summary_large_image" : "summary"} />
					<meta name="twitter:title" content=${title} />
					<meta name="twitter:description" content=${description} />
					${ogImage && jsx`<meta name="twitter:image" content=${ogImage} />`}
					<script type="application/ld+json">
						<${Raw} value=${JSON.stringify({
							"@context": "https://schema.org",
							"@type": "SoftwareApplication",
							name: "ZenDB",
							url: SITE_URL,
							description: "Define Zod tables. Write raw SQL. Get typed objects.",
							applicationCategory: "DeveloperApplication",
							operatingSystem: "Any",
							author: {
								"@type": "Person",
								name: "Brian Kim",
								url: "https://github.com/brainkim",
							},
							license: "https://opensource.org/licenses/MIT",
							offers: {
								"@type": "Offer",
								price: "0",
								priceCurrency: "USD",
							},
						})} />
					</script>
				</head>
				<body>
					<${ColorSchemeScript} />
					<${Raw} value=${html} />
					<script type="module" src=${assets.navbarScript}></script>
					<script type="module" src=${assets.searchScript}></script>
					<script type="module" src=${assets.copyScript}></script>
				</body>
			</html>
		`;
	}
}
