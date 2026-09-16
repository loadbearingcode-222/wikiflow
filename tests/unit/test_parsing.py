"""Tests for domain_code parsing.

The most valuable test here is the last one: it parses the entire real inventory of
1,946 domain codes from a committed hourly file. Hand-written examples cannot cover
a scheme whose edge cases were discovered by looking at production data, and a new
Wikimedia project appearing upstream should surface as a test failure rather than as
silently mis-attributed rows.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from wikiflow.parsing import (
    DomainCode,
    UnparseableDomainCode,
    parse_domain_code,
)

FIXTURE = Path(__file__).parents[1] / "fixtures" / "projectviews-20260915-120000"


# ---------------------------------------------------------------- the common shapes


@pytest.mark.unit
@pytest.mark.parametrize(
    ("code", "project", "language", "access"),
    [
        ("en", "wikipedia", "en", "desktop"),
        ("en.m", "wikipedia", "en", "mobile-web"),
        ("en.d", "wiktionary", "en", "desktop"),
        ("en.m.d", "wiktionary", "en", "mobile-web"),
        ("fr.b", "wikibooks", "fr", "desktop"),
        ("de.m.q", "wikiquote", "de", "mobile-web"),
        ("ru.voy", "wikivoyage", "ru", "desktop"),
        # Hyphenated and three-letter wiki codes are real and must survive.
        ("zh-yue.m", "wikipedia", "zh-yue", "mobile-web"),
        ("bat-smg", "wikipedia", "bat-smg", "desktop"),
        ("ceb", "wikipedia", "ceb", "desktop"),
    ],
)
def test_common_shapes(code: str, project: str, language: str, access: str) -> None:
    parsed = parse_domain_code(code)
    assert parsed == DomainCode(
        raw=code,
        project=project,
        language=language,
        access_method=access,  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------- the .m overload


@pytest.mark.unit
def test_dot_m_means_mobile_after_a_language() -> None:
    assert parse_domain_code("en.m").project == "wikipedia"
    assert parse_domain_code("en.m").is_mobile


@pytest.mark.unit
def test_dot_m_means_wikimedia_org_after_a_special_site() -> None:
    """`commons.m` is commons.wikimedia.org DESKTOP - not mobile Commons."""
    parsed = parse_domain_code("commons.m")

    assert parsed.project == "wikimedia"
    assert parsed.access_method == "desktop"
    assert not parsed.is_mobile


@pytest.mark.unit
def test_double_dot_m_is_mobile_wikimedia() -> None:
    """`commons.m.m` carries both meanings of .m at once."""
    parsed = parse_domain_code("commons.m.m")

    assert parsed.project == "wikimedia"
    assert parsed.language == "commons"
    assert parsed.is_mobile


@pytest.mark.unit
@pytest.mark.parametrize("site", ["meta", "species", "wikitech", "incubator", "foundation"])
def test_every_wikimedia_site_follows_the_same_rule(site: str) -> None:
    assert parse_domain_code(f"{site}.m").access_method == "desktop"
    assert parse_domain_code(f"{site}.m.m").access_method == "mobile-web"


@pytest.mark.unit
@pytest.mark.parametrize("wiki", ["simple", "abstract", "nostalgia", "test", "test2", "beta"])
def test_language_like_sites_are_not_treated_as_wikimedia_sites(wiki: str) -> None:
    """These look non-ISO but behave like languages: their .m means mobile.

    Getting this wrong would silently relabel Simple English Wikipedia as a
    wikimedia.org project.
    """
    parsed = parse_domain_code(f"{wiki}.m")

    assert parsed.project == "wikipedia"
    assert parsed.is_mobile


# ---------------------------------------------------------------- prefix-less wikis


@pytest.mark.unit
@pytest.mark.parametrize(
    ("code", "project"),
    [("m.wd", "wikidata"), ("m.w", "mediawiki"), ("m.s", "wikisource")],
)
def test_wikis_without_a_language_prefix_put_mobile_first(code: str, project: str) -> None:
    parsed = parse_domain_code(code)

    assert parsed.project == project
    assert parsed.language is None
    assert parsed.is_mobile


# ---------------------------------------------------------------- refusals


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad",
    ['""', "", "  ", "en.", ".en", "en..d", "en.m.d.q", "en.zzz", "m.zzz"],
)
def test_malformed_codes_are_refused_not_guessed(bad: str) -> None:
    """A wrong-but-plausible parse is worse than a failure: it corrupts silently."""
    with pytest.raises(UnparseableDomainCode):
        parse_domain_code(bad)


# ---------------------------------------------------------------- the real inventory


@pytest.mark.unit
def test_parses_the_entire_real_domain_code_inventory() -> None:
    """Parse all 1,946 codes from a real hourly file.

    Exactly one is expected to fail - a literal `""` code that genuinely appears
    upstream carrying several thousand views per hour. Asserting the exact count
    means a NEW unparseable code appearing upstream fails the build rather than
    quietly becoming a dropped row.
    """
    codes = [
        line.split(" ")[0]
        for line in FIXTURE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(codes) == 1946, "fixture changed; update the expectations below too"

    failures = []
    for code in codes:
        try:
            parse_domain_code(code)
        except UnparseableDomainCode:
            failures.append(code)

    assert failures == ['""'], f"unexpected unparseable domain codes: {failures}"


@pytest.mark.unit
def test_real_inventory_produces_sane_distributions() -> None:
    """Guards against a parser that 'succeeds' by labelling everything the same."""
    codes = [
        line.split(" ")[0]
        for line in FIXTURE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    parsed = []
    for code in codes:
        try:
            parsed.append(parse_domain_code(code))
        except UnparseableDomainCode:
            continue

    projects = {p.project for p in parsed}
    assert {"wikipedia", "wiktionary", "wikibooks", "wikimedia", "wikidata"} <= projects

    # Wikipedia should dominate but nowhere near everything.
    wikipedia_share = sum(p.project == "wikipedia" for p in parsed) / len(parsed)
    assert 0.3 < wikipedia_share < 0.5, wikipedia_share

    # Nearly every wiki has both a desktop and a mobile code, so the split is close
    # to even. A wildly skewed split means the mobile marker is being missed.
    mobile_share = sum(p.is_mobile for p in parsed) / len(parsed)
    assert 0.45 < mobile_share < 0.55, mobile_share


# ---------------------------------------------------------------- round trip


@pytest.mark.unit
def test_build_is_the_exact_inverse_of_parse_across_the_real_inventory() -> None:
    """Round-trip every real code through parse -> build and back to the original.

    The company filter needs to go from (language, project, access) to the code the
    dump uses. If the two directions ever disagree - say `commons.m` rebuilds as
    `commons` - the filter would silently match nothing for those wikis. This test
    proves they agree on all 1,945 parseable real codes rather than on examples.
    """
    from wikiflow.parsing import build_domain_code

    codes = [
        line.split(" ")[0]
        for line in FIXTURE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    mismatches = []
    checked = 0
    for code in codes:
        try:
            parsed = parse_domain_code(code)
        except UnparseableDomainCode:
            continue
        rebuilt = build_domain_code(
            project=parsed.project,
            language=parsed.language,
            access_method=parsed.access_method,
        )
        checked += 1
        if rebuilt != code:
            mismatches.append((code, rebuilt))

    assert checked == 1945
    assert mismatches == [], f"parse/build disagree on: {mismatches[:10]}"
