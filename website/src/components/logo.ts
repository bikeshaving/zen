import {jsx} from "@b9g/crank/standalone";

// Terracotta pair: the lit top face of each disk and its shadowed side wall.
// Seen slightly from the side, each stone reads as a stacked database platter.
const TOP = "#e0a074";
const SIDE = "#9a5c39";

/**
 * The ZenDB mark: a zen balancing-stone cairn that doubles as a database.
 *
 * Each stone is a puck — a lit elliptical top face over a curved side wall — so
 * it reads as a stacked DB platter. Geometry is physical, not eyeballed: a
 * stone's base sits ON the top face below it (never embedded), and each is set
 * back along the depth axis, which exposes the front crescent of the stone
 * beneath and is what makes the stack read as seated rather than floating.
 *
 * The stack stays symmetric about the vertical axis — no lateral stagger, or it
 * reads as toppling rather than balanced. The lower stones carry the lean; the
 * top one is near level, so the cairn resolves into stillness.
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
			<${Stone} cx=${16} cy=${19.02} rx=${11.58} ry=${5.26} h=${5.22} angle=${-2} />
			<${Stone} cx=${16} cy=${11.9} rx=${9.26} ry=${4.21} h=${4.63} angle=${1.5} />
			<${Stone} cx=${16} cy=${5.86} rx=${7.41} ry=${3.36} h=${4.03} angle=${-0.5} />
		</svg>
	`;
}
