"""Resolving tracked companies into the set of page titles to watch for.

A hardcoded list of English titles would be wrong immediately. Apple Inc. is
`Apple_Inc.` on English Wikipedia, `Apple` on French, German and Japanese, and
`أبل` on Arabic. Across 180+ language editions per company, hand-maintaining
that is not realistic.

Wikidata already stores the mapping: one entity per company, with a `sitelinks`
structure naming its article on every wiki. This module turns a handful of QIDs into
the concrete `(domain_code, page_title)` pairs the hourly filter matches on.

Two conversions matter and are easy to get wrong:

- **Spaces become underscores.** Wikidata returns `Apple Inc.`; the dumps contain
  `Apple_Inc.`. Matching without this converts every multi-word title into a silent
  zero.
- **Underscores in the wiki key become hyphens.** The sitelink key `zh_yuewiki`
  corresponds to domain code `zh-yue`.

Each resolved article yields two rows, desktop and mobile, because the dumps record
those as separate domain codes.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from wikiflow.config import get_settings
from wikiflow.parsing import WIKIMEDIA_SITES, build_domain_code

WIKIDATA_API = "https://www.wikidata.org/w/api.php"

#: Sitelink keys end with the project. Ordered longest-first so that `enwikiquote`
#: does not match the `wiki` suffix before `wikiquote` is tried.
PROJECT_BY_SITELINK_SUFFIX: tuple[tuple[str, str], ...] = (
    ("wikiquote", "wikiquote"),
    ("wikisource", "wikisource"),
    ("wikibooks", "wikibooks"),
    ("wikiversity", "wikiversity"),
    ("wikivoyage", "wikivoyage"),
    ("wikinews", "wikinews"),
    ("wiktionary", "wiktionary"),
    ("wiki", "wikipedia"),
)

#: Sitelink keys with no language component.
STANDALONE_WIKIS: dict[str, str] = {
    "wikidatawiki": "wikidata",
    "mediawikiwiki": "mediawiki",
    "sourceswiki": "wikisource",
}


@dataclass(frozen=True, slots=True)
class Company:
    qid: str
    name: str


@dataclass(frozen=True, slots=True)
class CompanyPage:
    """One article about a tracked company, on one wiki, at one access method."""

    qid: str
    company_name: str
    sitelink_key: str
    language: str | None
    project: str
    page_title: str
    """Underscored, exactly as it appears in the dump's page_title column."""

    domain_code: str
    access_method: str

    match_type: str = "sitelink"
    """`sitelink` for the article Wikidata names; `redirect` for a title that redirects to
    it. Pageviews are counted against the title REQUESTED, so a reader arriving via
    `Apple_Computer` is recorded there, not on `Apple_Inc.` - see wikiflow.redirects."""

    canonical_title: str = ""
    """The article this row's views belong to. Equal to page_title for a sitelink."""

    site_host: str = ""
    """e.g. `de.wikipedia.org` - where to ask MediaWiki about this article's redirects.
    Taken from Wikidata's own sitelink URL rather than derived from the language code,
    because the two differ (`be_x_oldwiki` is served from be-tarask.wikipedia.org)."""


class SitelinkResolutionError(RuntimeError):
    """Wikidata returned something unusable."""


def load_companies(path: Path) -> list[Company]:
    """Read the seed file of tracked companies."""
    with path.open(newline="", encoding="utf-8") as handle:
        return [
            Company(qid=row["qid"].strip(), name=row["name"].strip())
            for row in csv.DictReader(handle)
            if row.get("qid", "").strip() and not row["qid"].startswith("#")
        ]


def split_sitelink_key(key: str) -> tuple[str | None, str] | None:
    """Split e.g. `zh_yuewikiquote` into ("zh-yue", "wikiquote").

    Returns None for keys this pipeline does not model, rather than guessing.
    """
    if key in STANDALONE_WIKIS:
        return None, STANDALONE_WIKIS[key]

    for suffix, project in PROJECT_BY_SITELINK_SUFFIX:
        if not key.endswith(suffix):
            continue
        prefix = key[: -len(suffix)]
        if not prefix:
            return None, project
        language = prefix.replace("_", "-")
        # commonswiki / metawiki / specieswiki are wikimedia.org sites, not languages
        # with a wikipedia article.
        if language in WIKIMEDIA_SITES:
            return language, "wikimedia"
        return language, project

    return None


def resolve_company_pages(
    companies: list[Company], *, client: httpx.Client | None = None
) -> list[CompanyPage]:
    """Expand each company QID into every article about it, desktop and mobile."""
    settings = get_settings()
    owns_client = client is None
    client = client or httpx.Client(
        headers={"User-Agent": settings.user_agent},
        timeout=settings.request_timeout_seconds,
        follow_redirects=True,
    )

    try:
        pages: list[CompanyPage] = []
        # wbgetentities accepts up to 50 ids per call.
        for batch_start in range(0, len(companies), 50):
            batch = companies[batch_start : batch_start + 50]
            entities = _fetch_sitelinks(client, [c.qid for c in batch])
            by_qid = {c.qid: c for c in batch}

            for qid, entity in entities.items():
                company = by_qid.get(qid)
                if company is None:
                    continue
                pages.extend(_expand_entity(company, entity.get("sitelinks", {})))

        return pages
    finally:
        if owns_client:
            client.close()


def _fetch_sitelinks(client: httpx.Client, qids: list[str]) -> dict[str, Any]:
    response = client.get(
        WIKIDATA_API,
        params={
            "action": "wbgetentities",
            "ids": "|".join(qids),
            # sitelinks/urls adds each article's URL, which supplies the wiki's hostname.
            "props": "sitelinks/urls",
            "format": "json",
        },
    )
    response.raise_for_status()
    payload = response.json()

    if "error" in payload:
        raise SitelinkResolutionError(f"Wikidata API error: {payload['error']}")
    entities = payload.get("entities")
    if not entities:
        raise SitelinkResolutionError(f"no entities returned for {qids}")
    return dict(entities)


def _expand_entity(company: Company, sitelinks: dict[str, Any]) -> list[CompanyPage]:
    pages: list[CompanyPage] = []

    for key, link in sitelinks.items():
        split = split_sitelink_key(key)
        if split is None:
            continue
        language, project = split

        # The dumps store titles with underscores, not spaces.
        title = link["title"].replace(" ", "_")
        host = urlsplit(link.get("url", "")).hostname or ""

        for access in ("desktop", "mobile-web"):
            try:
                domain_code = build_domain_code(
                    project=project, language=language, access_method=access
                )
            except ValueError:
                # A project this pipeline cannot express as a domain code. Skipping is
                # correct: those pageviews are simply outside the modelled universe.
                continue

            pages.append(
                CompanyPage(
                    qid=company.qid,
                    company_name=company.name,
                    sitelink_key=key,
                    language=language,
                    project=project,
                    page_title=title,
                    domain_code=domain_code,
                    access_method=access,
                    canonical_title=title,
                    site_host=host,
                )
            )

    return pages
