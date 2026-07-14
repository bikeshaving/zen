# ZenDB website

The source for [zendb.org](https://zendb.org). Built with
[Crank.js](https://crank.js.org) + [Shovel.js](https://github.com/bikeshaving/shovel),
the same stack as the other bikeshaving sites.

- **Content** lives in `../docs/guides/*.md` (numbered for ordering; the numeric
  prefix is stripped from the URL). Frontmatter: `title`, `description`.
- **Views** are in `src/views/` (`home`, `guide`, `not-found`).
- **Shared UI** is in `src/components/` (`root`, `navbar`, `footer`, `sidebar`,
  `search`, the `marked` pipeline, color-scheme toggle, logo).
- Code blocks are **server-highlighted** with Prism at build time; the
  `clients/copy.ts` script adds copy buttons as progressive enhancement.
- Full-text search is powered by [Pagefind](https://pagefind.app) over the
  generated HTML.

## Develop

```bash
bun install
bun run develop   # http://localhost:1338
```

Note: search only works after a static build (Pagefind indexes the output).

## Build

```bash
bun run static    # builds to dist/public/ and runs Pagefind
```

Optionally regenerate the icon set (PNGs + favicon.ico) from `static/logo.svg`:

```bash
bun run build-logo
```

## Deploy

Both targets serve the static `dist/public/` directory.

**GitHub Pages** (writes a `CNAME` for the custom domain):

```bash
bun run deploy    # runs `static` + link check, then gh-pages -> zendb.org
```

**Cloudflare Pages** — point a Pages project at this repo with:

- Build command: `cd website && bun install && bun run static`
- Build output directory: `website/dist/public`

(Or upload `dist/public` directly with `wrangler pages deploy dist/public`.)
