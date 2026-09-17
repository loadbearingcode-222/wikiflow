"""Closing the redirect undercount.

Pageviews are recorded against the title a reader REQUESTED, not the article they
landed on. Someone following a link to `Apple_Computer` on English Wikipedia reads the
`Apple_Inc.` article, but the dump counts that view under `Apple_Computer`. Wikidata
sitelinks name only the article itself, so a filter built from sitelinks alone misses
every one of those views.

The article's redirects are listed by MediaWiki (`prop=redirects`). This module asks
each wiki for them and expands every tracked article into its redirect titles too, so
the hourly filter matches them. The scale was measured before building this: English
Wikipedia alone has 103 redirects to Apple Inc., 74 to Microsoft and 138 to Amazon.

Two rules this follows, both from Wikimedia's API etiquette:

* **Requests are serial.** One wiki at a time, never in parallel. A refresh takes
  about three minutes (315 requests); fine for data that changes over weeks.
* **`maxlag` is sent, and honoured.** When database replicas are lagging, the API
  refuses with `maxlag` and a Retry-After, and a well-behaved client waits.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import httpx

from wikiflow.config import get_settings
from wikiflow.download import RETRYABLE_STATUS, _backoff_seconds, _retry_after_seconds
from wikiflow.wikidata import CompanyPage, load_companies, resolve_company_pages

log = logging.getLogger(__name__)

#: MediaWiki accepts at most 50 titles per query for clients without the bot right.
TITLES_PER_REQUEST = 50

#: Seconds of replica lag above which the API should refuse us. 5 is what Wikimedia
#: recommends for non-interactive clients.
MAXLAG_SECONDS = 5

MAX_ATTEMPTS = 5

#: Longest wait between attempts. A Retry-After beyond this fails the refresh, and the
#: previous reference snapshot stays in use - see wikiflow.reference.
MAX_BACKOFF_SECONDS = 120.0


class RedirectResolutionError(RuntimeError):
    """A wiki could not be asked about its redirects."""


@dataclass(frozen=True, slots=True)
class RedirectStats:
    hosts: int
    requests: int
    articles: int
    articles_missing: int
    redirect_titles: int


def resolve_redirects(
    pages: Iterable[CompanyPage],
    *,
    client: httpx.Client | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[list[CompanyPage], RedirectStats]:
    """Return every page in `pages` plus one row per redirect to each article.

    A redirect inherits the domain code and access method of the article it points at,
    so each redirect yields a desktop and a mobile row, exactly as the article does.

    Raises rather than returning a partial result: one wiki that could not be asked
    would otherwise become a silent undercount for that language.
    """
    pages = list(pages)
    settings = get_settings()
    owns_client = client is None
    client = client or httpx.Client(
        headers={"User-Agent": settings.user_agent},
        timeout=settings.request_timeout_seconds,
        follow_redirects=True,
    )

    # host -> canonical title -> the article rows (desktop and mobile) for that title
    articles: dict[str, dict[str, list[CompanyPage]]] = defaultdict(lambda: defaultdict(list))
    for page in pages:
        if page.match_type == "sitelink" and page.site_host:
            articles[page.site_host][page.page_title].append(page)

    redirects_by_host: dict[str, dict[str, list[str]]] = {}
    requests = 0
    try:
        for host in sorted(articles):
            titles = sorted(articles[host])
            found: dict[str, list[str]] = {}
            for start in range(0, len(titles), TITLES_PER_REQUEST):
                batch = titles[start : start + TITLES_PER_REQUEST]
                batch_found, batch_requests = _fetch_redirects(client, host, batch, sleep=sleep)
                found.update(batch_found)
                requests += batch_requests
            redirects_by_host[host] = found
    finally:
        if owns_client:
            client.close()

    expanded = _expand(pages, articles, redirects_by_host)
    article_count = sum(len(titles) for titles in articles.values())
    stats = RedirectStats(
        hosts=len(articles),
        requests=requests,
        articles=article_count,
        articles_missing=sum(
            1
            for host, titles in articles.items()
            for title in titles
            if title not in redirects_by_host.get(host, {})
        ),
        redirect_titles=sum(len(r) for found in redirects_by_host.values() for r in found.values()),
    )
    log.info("redirect resolution: %s", stats)
    return expanded, stats


def _expand(
    pages: list[CompanyPage],
    articles: dict[str, dict[str, list[CompanyPage]]],
    redirects_by_host: dict[str, dict[str, list[str]]],
) -> list[CompanyPage]:
    """Add redirect rows, never letting one displace an article row.

    Keyed on (domain_code, page_title), the join key of the hourly filter. Articles go
    in first; a redirect with the same key is dropped, since a duplicate key would
    double-count that row's views in the extraction join.
    """
    by_key: dict[tuple[str, str], CompanyPage] = {}
    for page in pages:
        by_key.setdefault((page.domain_code, page.page_title), page)

    for host, found in redirects_by_host.items():
        for canonical, redirect_titles in found.items():
            for article in articles[host][canonical]:
                for title in redirect_titles:
                    key = (article.domain_code, title)
                    if key in by_key:
                        continue
                    by_key[key] = replace(
                        article,
                        page_title=title,
                        match_type="redirect",
                        canonical_title=canonical,
                    )

    return list(by_key.values())


def _fetch_redirects(
    client: httpx.Client,
    host: str,
    titles: list[str],
    *,
    sleep: Callable[[float], None],
) -> tuple[dict[str, list[str]], int]:
    """Ask one wiki for the redirects to up to 50 titles, following continuation.

    Returns {requested title: [redirect titles]}, all underscored as in the dumps. A
    title the wiki reports as missing - a sitelink gone stale - is absent from the
    result rather than mapped to an empty list, so the caller can count it.
    """
    params: dict[str, Any] = {
        "action": "query",
        "prop": "redirects",
        "titles": "|".join(titles),
        "rdprop": "title",
        "rdlimit": "max",
        "format": "json",
        "formatversion": "2",
        "maxlag": MAXLAG_SECONDS,
    }
    found: dict[str, list[str]] = {}
    # The API reports titles in normalised form (spaces, not underscores); map back.
    requested = {title.replace("_", " "): title for title in titles}
    requests = 0

    while True:
        payload = _get_json(client, f"https://{host}/w/api.php", params, sleep=sleep)
        requests += 1

        query = payload.get("query", {})
        for pair in query.get("normalized", []):
            if pair["from"] in requested:
                requested[pair["to"]] = requested[pair["from"]]

        for page in query.get("pages", []):
            original = requested.get(page["title"])
            if original is None or page.get("missing") or page.get("invalid"):
                continue
            titles_found = found.setdefault(original, [])
            titles_found.extend(r["title"].replace(" ", "_") for r in page.get("redirects", []))

        if "continue" not in payload:
            return found, requests
        params = {**params, **payload["continue"]}


def _get_json(
    client: httpx.Client,
    url: str,
    params: dict[str, Any],
    *,
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        retry_after: float | None = None
        problem: str

        try:
            response = client.get(url, params=params)
        except httpx.TransportError as exc:
            problem = f"{type(exc).__name__}: {exc}"
        else:
            if response.status_code in RETRYABLE_STATUS:
                problem = f"HTTP {response.status_code}"
                retry_after = _retry_after_seconds(response)
            else:
                response.raise_for_status()
                payload: dict[str, Any] = response.json()
                error = payload.get("error")
                if error is None:
                    return payload
                if error.get("code") != "maxlag":
                    raise RedirectResolutionError(f"{url}: {error}")
                problem = f"maxlag: {error.get('info', '')}"
                retry_after = _retry_after_seconds(response) or float(MAXLAG_SECONDS)

        delay = _backoff_seconds(attempt, retry_after)
        if attempt == MAX_ATTEMPTS or delay > MAX_BACKOFF_SECONDS:
            raise RedirectResolutionError(f"{url}: gave up after {attempt} attempt(s): {problem}")
        log.warning("%s: %s; retrying in %.0fs", url, problem, delay)
        sleep(delay)

    raise AssertionError("unreachable: the loop always returns or raises")


def resolve_filter(seed: Path) -> list[CompanyPage]:
    """The full company filter: every article from Wikidata, plus its redirects."""
    articles = resolve_company_pages(load_companies(seed))
    if not articles:
        raise RedirectResolutionError(
            "Wikidata resolved the seed to zero articles - refusing to build an empty filter"
        )
    pages, _ = resolve_redirects(articles)
    return pages
