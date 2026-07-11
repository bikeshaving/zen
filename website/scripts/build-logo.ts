/**
 * Rasterize static/logo.svg into the PNG/favicon set used for icons and social
 * cards. Optional — the site builds fine without it (it falls back to the SVG
 * favicon and omits og:image). Run with: bun run build-logo
 */
import {readFileSync, writeFileSync} from "node:fs";
import {join, dirname} from "node:path";
import {fileURLToPath} from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const staticDir = join(here, "..", "static");
const svgPath = join(staticDir, "logo.svg");

const SIZES = [16, 32, 48, 64, 96, 128, 192, 256, 512];

async function main() {
	let sharp: any;
	try {
		sharp = (await import("sharp")).default;
	} catch {
		console.error(
			"sharp is not installed. Run `bun add -d sharp` to generate PNG icons.",
		);
		process.exit(1);
	}

	const svg = readFileSync(svgPath);

	// Transparent PNGs at every size.
	for (const size of SIZES) {
		const out = join(staticDir, `logo-${size}.png`);
		await sharp(svg, {density: 384})
			.resize(size, size, {fit: "contain", background: {r: 0, g: 0, b: 0, alpha: 0}})
			.png()
			.toFile(out);
		console.log(`wrote logo-${size}.png`);
	}

	// favicon.ico from the small sizes.
	try {
		const toIco = (await import("svg-to-ico")).default;
		await toIco({
			input_name: svgPath,
			output_name: join(staticDir, "favicon.ico"),
			sizes: [16, 32, 48],
		});
		console.log("wrote favicon.ico");
	} catch (e) {
		console.warn("Skipped favicon.ico (svg-to-ico unavailable)", e);
	}
}

main();
