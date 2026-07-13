import {jsx} from "@b9g/crank/standalone";

// Terracotta pair: the lit top face of each disk and its shadowed side wall.
// Seen slightly from the side, each stone reads as a stacked database platter.
const TOP = "#e0a074";
const SIDE = "#9a5c39";

/**
 * The ZenDB mark: a zen balancing-stone cairn that doubles as a database.
 * Three flattened, gently-tilted stones are drawn as pucks — a lit elliptical
 * top face over a curved side wall — so each stone reads as a stacked DB disk
 * even though the cairn leans. Balanced in feel, not in geometry.
 */
export function Logo({
	width = "1.9em",
	height = "1.9em",
	top = TOP,
	side = SIDE,
}: {
	width?: string;
	height?: string;
	top?: string;
	side?: string;
}) {
	function Stone({
		cx,
		cy,
		rx,
		ry,
		h,
		angle,
	}: {
		cx: number;
		cy: number;
		rx: number;
		ry: number;
		h: number; // visible side-wall height
		angle: number;
	}) {
		// Side wall: down the left edge, across the front of the bottom ellipse,
		// up the right edge, and back along the front arc of the top ellipse.
		const wall =
			`M ${cx - rx},${cy} L ${cx - rx},${cy + h} ` +
			`A ${rx},${ry} 0 0 0 ${cx + rx},${cy + h} L ${cx + rx},${cy} ` +
			`A ${rx},${ry} 0 0 1 ${cx - rx},${cy} Z`;
		return jsx`
			<g transform="rotate(${angle} ${cx} ${cy})">
				<path d=${wall} fill=${side} />
				<ellipse cx=${cx} cy=${cy} rx=${rx} ry=${ry} fill=${top} />
			</g>
		`;
	}

	return jsx`
		<svg
			width=${width}
			height=${height}
			viewBox="0 0 32 32"
			fill="none"
			role="img"
			aria-label="ZenDB"
			xmlns="http://www.w3.org/2000/svg"
		>
			<${Stone} cx=${16} cy=${20.6} rx=${10} ry=${2.7} h=${3.8} angle=${-3} />
			<${Stone} cx=${16} cy=${13.3} rx=${7.2} ry=${2.1} h=${3.4} angle=${4} />
			<${Stone} cx=${16} cy=${7.4} rx=${4.7} ry=${1.7} h=${3} angle=${-5} />
		</svg>
	`;
}
