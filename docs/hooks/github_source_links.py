"""Build-time link/page adaptation for the CronosDB docs site.

Two jobs:

1. ``on_files`` — inject the repo-root markdown docs (README.md, ARCHITECTURE.md,
   DOCKER.md, plan.md, ...) into the site as virtual pages. The root files stay
   canonical (single source of truth for GitHub browsing); at build time their
   content is copied with ``docs/``-prefixed link targets rebased to the site
   root, so cross-links resolve identically in both places.

2. ``on_page_markdown`` — rewrite relative links that point at source files
   (internal/..., pkg/..., Makefile, ...) into absolute GitHub blob URLs, since
   those files are not part of the site. A trailing ``:NNN`` line suffix is
   converted to GitHub's ``#LNNN`` anchor.
"""

import os
import re

from mkdocs.structure.files import File

GITHUB_BLOB = "https://github.com/jatin711-debug/cronos_db_golang/blob/developement/"

# Root-level docs injected into the site: repo path -> site path.
ROOT_PAGES = {
    "README.md": "index.md",
    "ARCHITECTURE.md": "ARCHITECTURE.md",
    "TECHNICAL_DEEP_DIVE.md": "TECHNICAL_DEEP_DIVE.md",
    "DOCKER.md": "DOCKER.md",
    "CONTRIBUTING.md": "CONTRIBUTING.md",
    "CODE_OF_CONDUCT.md": "CODE_OF_CONDUCT.md",
    "plan.md": "plan.md",
}

# Matches markdown links/images: [text](target) — target captured without title.
LINK_RE = re.compile(r"(\]\()([^)\s]+)(\s*[^)]*)\)")

# Line-number suffix used by plan.md and friends, e.g. internal/x/y.go:1232.
LINE_SUFFIX_RE = re.compile(r"^(?P<path>.+?):(?P<line>\d+)$")


def on_files(files, config):
    repo_root = os.path.dirname(config.config_file_path)
    for repo_path, site_path in ROOT_PAGES.items():
        abs_path = os.path.join(repo_root, repo_path)
        if not os.path.isfile(abs_path):
            continue
        with open(abs_path, encoding="utf-8") as fh:
            content = fh.read()
        # The repo README is the site homepage (index.md).
        content = content.replace("](README.md", "](index.md")
        files.append(File.generated(config, site_path, content=content))
    return files


def on_page_markdown(markdown, page, config, files):
    page_dir = os.path.dirname(page.file.src_path)
    docs_dir_abs = os.path.abspath(config.docs_dir)
    repo_root = os.path.dirname(config.config_file_path)

    def rewrite(match):
        prefix, target, suffix = match.groups()
        # Skip absolute URLs and mailto.
        if target.startswith(("http://", "https://", "mailto:")):
            return match.group(0)
        path_part, _, fragment = target.partition("#")
        # In-page anchors: GitHub keeps consecutive hyphens in slugs
        # ("A & B" -> "a--b") while MkDocs' default slugify collapses them,
        # so collapse anchor fragments to match the site.
        if not path_part:
            collapsed = re.sub(r"-{2,}", "-", fragment)
            if collapsed != fragment:
                return f"{prefix}#{collapsed}{suffix})"
            return match.group(0)

        # Site-root pages are the generated repo-root docs: their relative
        # links resolve against the repo root, not docs_dir.
        base = repo_root if page_dir == "" else os.path.join(docs_dir_abs, page_dir)
        abs_target = os.path.normpath(os.path.join(base, path_part))
        docs_rel = os.path.relpath(abs_target, docs_dir_abs).replace(os.sep, "/")
        repo_rel = os.path.relpath(abs_target, repo_root).replace(os.sep, "/")

        # Markdown links that stay inside docs_dir are native MkDocs links.
        if path_part.lower().endswith(".md") and not docs_rel.startswith(".."):
            collapsed = re.sub(r"-{2,}", "-", fragment)
            # Root pages resolve against the repo root, so a "docs/..." target
            # must be re-emitted as the site-relative path from the site root.
            if page_dir == "":
                out = docs_rel
                if collapsed:
                    out += "#" + collapsed
                return f"{prefix}{out}{suffix})"
            if collapsed != fragment:
                return f"{prefix}{path_part}#{collapsed}{suffix})"
            return match.group(0)
        # Markdown links that escape docs_dir point at a root-level doc:
        # re-relativize to the generated page when there is one.
        if path_part.lower().endswith(".md") and repo_rel in ROOT_PAGES:
            site_base = docs_dir_abs if page_dir == "" else os.path.join(docs_dir_abs, page_dir)
            site_target = os.path.relpath(
                os.path.join(docs_dir_abs, ROOT_PAGES[repo_rel]),
                site_base,
            ).replace(os.sep, "/")
            if fragment:
                site_target += "#" + fragment
            return f"{prefix}{site_target}{suffix})"

        # Everything else is a source file — rewrite to GitHub.
        if path_part.lower().endswith(".md"):
            # Escaping markdown without a generated page: link to GitHub.
            path_part = repo_rel
        else:
            # Convert a trailing :NNN line suffix to GitHub's #LNNN anchor.
            line = None
            line_match = LINE_SUFFIX_RE.match(path_part)
            if line_match and not fragment:
                path_part = line_match.group("path")
                line = line_match.group("line")
            abs_target = os.path.normpath(os.path.join(base, path_part))
            path_part = os.path.relpath(abs_target, repo_root).replace(os.sep, "/")
            if path_part.startswith("../"):
                return match.group(0)
            if line:
                fragment = "L" + line

        url = GITHUB_BLOB + path_part.lstrip("./")
        if fragment:
            url += "#" + fragment
        return f"{prefix}{url}{suffix})"

    return LINK_RE.sub(rewrite, markdown)
