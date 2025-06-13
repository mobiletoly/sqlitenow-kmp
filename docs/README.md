# SQLiteNow Documentation

This directory contains the Jekyll-based documentation website for SQLiteNow.

## Local Development

To run the documentation site locally:

```bash
cd docs
bundle install
bundle exec jekyll serve
```

The site will be available at `http://localhost:4000/sqlitenow-kmp/`

## Structure

- `index.markdown` - Homepage
- `overview.md` - Project overview and features
- `getting-started.md` - Step-by-step setup guide
- `_config.yml` - Jekyll configuration

## Deployment

The site is configured to be deployed to GitHub Pages at:
`https://mobiletoly.github.io/sqlitenow-kmp/`

## Navigation

The site uses the Minima theme with custom navigation defined in `_config.yml`:

```yaml
header_pages:
  - overview.md
  - getting-started.md
```

This creates a navigation bar with tabs for Overview, How it Works, and Getting Started.
