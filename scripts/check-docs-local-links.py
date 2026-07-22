#!/usr/bin/env python3
"""Validate local references in a generated Jekyll site without network access."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path, PurePosixPath
import re
import sys
from urllib.parse import unquote, urljoin, urlsplit


IGNORED_SCHEMES = {"data", "http", "https", "javascript", "mailto", "tel"}
CSS_URL_PATTERN = re.compile(r"""url\(\s*(['"]?)(.*?)\1\s*\)""", re.IGNORECASE)
META_REFRESH_URL_PATTERN = re.compile(r"""(?:^|;)\s*url\s*=\s*(['"]?)(.*?)\1\s*$""", re.IGNORECASE)


@dataclass(frozen=True)
class Reference:
    source: Path
    source_url: str
    value: str
    attribute: str


class DocumentParser(HTMLParser):
    def __init__(self, source: Path, source_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.source = source
        self.source_url = source_url
        self.references: list[Reference] = []
        self.anchors: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {name.lower(): value for name, value in attrs}
        anchor = attributes.get("id")
        if anchor:
            self.anchors.add(anchor)
        if tag.lower() == "a" and attributes.get("name"):
            self.anchors.add(attributes["name"] or "")

        for attribute in ("href", "src"):
            value = attributes.get(attribute)
            if value:
                self.references.append(Reference(self.source, self.source_url, value, attribute))

        srcset = attributes.get("srcset")
        if srcset:
            for candidate in srcset.split(","):
                value = candidate.strip().split(maxsplit=1)[0]
                if value:
                    self.references.append(Reference(self.source, self.source_url, value, "srcset"))

        if (
            tag.lower() == "meta"
            and (attributes.get("http-equiv") or "").lower() == "refresh"
            and attributes.get("content")
        ):
            match = META_REFRESH_URL_PATTERN.search(attributes["content"] or "")
            if match and match.group(2):
                self.references.append(
                    Reference(self.source, self.source_url, match.group(2), "meta-refresh"),
                )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-dir", type=Path, required=True, help="Generated site directory")
    parser.add_argument("--baseurl", default="", help="Deployment base URL, for example /sqlitenow-kmp")
    return parser.parse_args()


def source_url(site_root: Path, source: Path) -> str:
    relative = source.relative_to(site_root).as_posix()
    if relative == "index.html":
        return "/"
    if relative.endswith("/index.html"):
        return f"/{relative[:-10]}"
    return f"/{relative}"


def strip_baseurl(path: str, baseurl: str) -> str:
    if not baseurl:
        return path
    if path == baseurl:
        return "/"
    prefix = f"{baseurl}/"
    if path.startswith(prefix):
        return path[len(baseurl) :]
    return path


def candidate_files(site_root: Path, url_path: str) -> list[Path]:
    relative = PurePosixPath(url_path.lstrip("/"))
    direct = site_root.joinpath(*relative.parts)
    if url_path.endswith("/"):
        return [direct / "index.html"]
    candidates = [direct]
    if not direct.suffix:
        candidates.extend([direct.with_suffix(".html"), direct / "index.html"])
    elif direct.suffix.lower() in {".md", ".markdown"}:
        candidates.append(direct.with_suffix(".html"))
    return candidates


def resolve_reference(
    reference: Reference,
    site_root: Path,
    baseurl: str,
) -> tuple[Path, str] | None:
    value = reference.value.strip()
    if not value or value == "#":
        return None
    parsed = urlsplit(value)
    if parsed.scheme.lower() in IGNORED_SCHEMES or parsed.scheme or parsed.netloc:
        return None

    absolute = urlsplit(urljoin(reference.source_url, value))
    decoded_path = unquote(strip_baseurl(absolute.path, baseurl))
    candidates = candidate_files(site_root, decoded_path)
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve(), unquote(absolute.fragment)
    return candidates[0].resolve(), unquote(absolute.fragment)


def main() -> int:
    arguments = parse_arguments()
    site_root = arguments.site_dir.resolve()
    if not site_root.is_dir():
        print(f"site directory does not exist: {site_root}", file=sys.stderr)
        return 2

    baseurl = f"/{arguments.baseurl.strip('/')}" if arguments.baseurl.strip("/") else ""
    references: list[Reference] = []
    anchors_by_file: dict[Path, set[str]] = {}
    html_files = sorted(site_root.rglob("*.html"))
    css_files = sorted(site_root.rglob("*.css"))

    for html_file in html_files:
        parser = DocumentParser(html_file, source_url(site_root, html_file))
        parser.feed(html_file.read_text(encoding="utf-8"))
        parser.close()
        references.extend(parser.references)
        anchors_by_file[html_file.resolve()] = parser.anchors

    for css_file in css_files:
        css_source_url = source_url(site_root, css_file)
        for match in CSS_URL_PATTERN.finditer(css_file.read_text(encoding="utf-8")):
            value = match.group(2).strip()
            if value:
                references.append(Reference(css_file, css_source_url, value, "css-url"))

    failures: list[str] = []
    local_reference_count = 0
    anchor_reference_count = 0
    for reference in sorted(
        references,
        key=lambda item: (item.source.as_posix(), item.attribute, item.value),
    ):
        resolved = resolve_reference(reference, site_root, baseurl)
        if resolved is None:
            continue
        local_reference_count += 1
        target, fragment = resolved
        try:
            target.relative_to(site_root)
        except ValueError:
            failures.append(
                f"{reference.source.relative_to(site_root)}: {reference.value!r} escapes the generated site",
            )
            continue
        if not target.is_file():
            failures.append(
                f"{reference.source.relative_to(site_root)}: {reference.value!r} "
                f"does not resolve to a generated file",
            )
            continue
        if fragment:
            anchor_reference_count += 1
            if target.suffix.lower() != ".html":
                failures.append(
                    f"{reference.source.relative_to(site_root)}: {reference.value!r} "
                    f"uses an anchor on non-HTML target {target.relative_to(site_root)}",
                )
            elif fragment not in anchors_by_file.get(target, set()):
                failures.append(
                    f"{reference.source.relative_to(site_root)}: {reference.value!r} "
                    f"references missing anchor {fragment!r}",
                )

    if failures:
        print(f"broken local references: {len(failures)}", file=sys.stderr)
        for failure in sorted(set(failures)):
            print(f"- {failure}", file=sys.stderr)
        return 1

    print(
        "local link check passed: "
        f"{len(html_files)} HTML files, {len(css_files)} CSS files, "
        f"{local_reference_count} local references, {anchor_reference_count} anchor references",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
