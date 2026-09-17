"""Redirect resolution, against a fake MediaWiki API. No network."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from wikiflow.redirects import RedirectResolutionError, resolve_redirects
from wikiflow.wikidata import CompanyPage


def article(title: str, domain: str, access: str, host: str = "en.wikipedia.org") -> CompanyPage:
    return CompanyPage(
        qid="Q312",
        company_name="Apple Inc.",
        sitelink_key="enwiki",
        language="en",
        project="wikipedia",
        page_title=title,
        domain_code=domain,
        access_method=access,
        canonical_title=title,
        site_host=host,
    )


APPLE = [article("Apple_Inc.", "en", "desktop"), article("Apple_Inc.", "en.m", "mobile-web")]

Responder = Callable[[httpx.Request, int], httpx.Response]


def api(responder: Responder) -> httpx.Client:
    """A client whose responder also receives the 1-based call number."""
    count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal count
        count += 1
        return responder(request, count)

    return httpx.Client(transport=httpx.MockTransport(handler))


def ok(payload: dict[str, Any], headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(200, content=json.dumps(payload), headers=headers)


def pages(*entries: dict[str, Any]) -> dict[str, Any]:
    return {"query": {"pages": list(entries)}}


@pytest.mark.unit
def test_each_redirect_yields_a_desktop_and_a_mobile_row() -> None:
    client = api(
        lambda request, n: ok(
            pages({"title": "Apple Inc.", "redirects": [{"title": "Apple Computer"}]})
        )
    )
    expanded, stats = resolve_redirects(APPLE, client=client)

    redirects = sorted(
        (p.domain_code, p.page_title, p.canonical_title)
        for p in expanded
        if p.match_type == "redirect"
    )
    assert redirects == [
        ("en", "Apple_Computer", "Apple_Inc."),
        ("en.m", "Apple_Computer", "Apple_Inc."),
    ]
    assert stats.redirect_titles == 1
    assert len(expanded) == 4


@pytest.mark.unit
def test_requested_titles_are_mapped_back_through_normalisation() -> None:
    """The API answers with `Apple Inc.`; the dumps and the filter use `Apple_Inc.`."""

    def respond(request: httpx.Request, n: int) -> httpx.Response:
        assert request.url.params["titles"] == "Apple_Inc."
        return ok(
            {
                "query": {
                    "normalized": [{"from": "Apple_Inc.", "to": "Apple Inc."}],
                    "pages": [{"title": "Apple Inc.", "redirects": [{"title": "AAPL"}]}],
                }
            }
        )

    expanded, _ = resolve_redirects(APPLE, client=api(respond))
    assert {p.page_title for p in expanded} == {"Apple_Inc.", "AAPL"}


@pytest.mark.unit
def test_continuation_is_followed_and_merged() -> None:
    def respond(request: httpx.Request, n: int) -> httpx.Response:
        if n == 1:
            assert "rdcontinue" not in request.url.params
            return ok(
                {
                    **pages({"title": "Apple Inc.", "redirects": [{"title": "A1"}]}),
                    "continue": {"rdcontinue": "123|456", "continue": "||"},
                }
            )
        assert request.url.params["rdcontinue"] == "123|456"
        return ok(pages({"title": "Apple Inc.", "redirects": [{"title": "A2"}]}))

    expanded, stats = resolve_redirects(APPLE, client=api(respond))
    assert {p.page_title for p in expanded if p.match_type == "redirect"} == {"A1", "A2"}
    assert stats.requests == 2


@pytest.mark.unit
def test_a_missing_article_is_counted_not_expanded() -> None:
    client = api(lambda request, n: ok(pages({"title": "Apple Inc.", "missing": True})))
    expanded, stats = resolve_redirects(APPLE, client=client)
    assert expanded == APPLE
    assert stats.articles_missing == 1


@pytest.mark.unit
def test_a_redirect_never_displaces_an_article_with_the_same_key() -> None:
    """A duplicate (domain_code, page_title) would double-count in the extraction join."""
    client = api(
        lambda request, n: ok(
            pages({"title": "Apple Inc.", "redirects": [{"title": "Apple Inc."}]})
        )
    )
    expanded, _ = resolve_redirects(APPLE, client=client)
    assert expanded == APPLE


@pytest.mark.unit
def test_maxlag_is_waited_out_as_the_server_asks() -> None:
    slept: list[float] = []

    def respond(request: httpx.Request, n: int) -> httpx.Response:
        assert request.url.params["maxlag"] == "5"
        if n == 1:
            return ok({"error": {"code": "maxlag", "info": "lagged"}}, {"Retry-After": "7"})
        return ok(pages({"title": "Apple Inc.", "redirects": []}))

    resolve_redirects(APPLE, client=api(respond), sleep=slept.append)
    assert slept == [7.0]


@pytest.mark.unit
def test_other_api_errors_fail_the_resolution() -> None:
    client = api(lambda request, n: ok({"error": {"code": "badvalue", "info": "nope"}}))
    with pytest.raises(RedirectResolutionError, match="badvalue"):
        resolve_redirects(APPLE, client=client, sleep=lambda s: None)


@pytest.mark.unit
def test_persistent_rate_limiting_fails_rather_than_returning_a_partial_filter() -> None:
    client = api(lambda request, n: httpx.Response(429))
    with pytest.raises(RedirectResolutionError, match="gave up after 5"):
        resolve_redirects(APPLE, client=client, sleep=lambda s: None)


@pytest.mark.unit
def test_each_wiki_is_asked_on_its_own_host() -> None:
    hosts: list[str] = []

    def respond(request: httpx.Request, n: int) -> httpx.Response:
        hosts.append(request.url.host)
        return ok(pages({"title": request.url.params["titles"].replace("_", " ")}))

    de = article("Apple", "de", "desktop", host="de.wikipedia.org")
    resolve_redirects([*APPLE, de], client=api(respond))
    assert sorted(hosts) == ["de.wikipedia.org", "en.wikipedia.org"]
