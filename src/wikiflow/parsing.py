"""Decoding the `domain_code` field of a Wikimedia pageviews row.

A domain code is an abbreviated hostname: `en` is en.wikipedia.org, `en.m.d` is
en.m.wiktionary.org. Three facts make this harder than it looks, and all three were
confirmed against a real hourly file containing 1,946 distinct codes
(tests/fixtures/projectviews-20260915-120000):

1. **`.m` is overloaded.** In `en.m` it marks mobile. In `commons.m` it is the
   *wikimedia.org* project suffix. In `commons.m.m` it is both at once.

2. **Some wikis have no language prefix**, and there the mobile marker moves to the
   front: `m.wd` is m.wikidata.org, `m.w` is m.mediawiki.org.

3. **Not every non-ISO first segment is a special site.** `simple`, `abstract`,
   `nostalgia`, `test`, `test2`, `beta` and `thankyou` behave like languages - their
   `.m` means mobile. Only a specific set of wikimedia.org sites take `.m` as a
   project suffix.

Performance note: this is deliberately Python rather than SQL, and that is safe
because it never runs per row. The hourly file has ~8M rows but only ~1,400 distinct
domain codes, so parsing happens after aggregation, on the small grain.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

AccessMethod = Literal["desktop", "mobile-web"]

#: Suffix in the final position -> project. An absent suffix means wikipedia.
PROJECT_BY_SUFFIX: Final[dict[str, str]] = {
    "b": "wikibooks",
    "d": "wiktionary",
    "f": "wikifunctions",
    "n": "wikinews",
    "q": "wikiquote",
    "s": "wikisource",
    "v": "wikiversity",
    "voy": "wikivoyage",
    "w": "mediawiki",
    "wd": "wikidata",
    "m": "wikimedia",
}

#: Sites hosted on wikimedia.org. For these - and ONLY these - a trailing `.m` is the
#: project suffix rather than the mobile marker. Derived from the real code inventory;
#: anything outside this set is treated as a language-style wiki.
WIKIMEDIA_SITES: Final[frozenset[str]] = frozenset(
    {
        "advisory",
        "commons",
        "foundation",
        "incubator",
        "meta",
        "outreach",
        "species",
        "strategy",
        "usability",
        "wikimania",
        "wikitech",
    }
)


class UnparseableDomainCode(ValueError):
    """Raised for a domain code that does not fit the documented scheme.

    Real files contain at least one: a literal `""` code carrying several thousand
    views per hour. Callers decide whether to drop or quarantine such rows; this
    module refuses to invent a plausible-looking answer for them.
    """


@dataclass(frozen=True, slots=True)
class DomainCode:
    """A parsed `domain_code`."""

    raw: str
    project: str
    """e.g. wikipedia, wiktionary, wikimedia."""

    language: str | None
    """ISO-ish wiki code, or None for wikis with no language prefix (wikidata.org)."""

    access_method: AccessMethod

    @property
    def is_mobile(self) -> bool:
        return self.access_method == "mobile-web"


def parse_domain_code(code: str) -> DomainCode:
    """Decode a `domain_code` into (project, language, access_method).

    Raises:
        UnparseableDomainCode: if the code does not match the documented scheme.
    """
    if not code or code.strip() != code:
        raise UnparseableDomainCode(f"empty or padded domain code: {code!r}")

    parts = code.split(".")
    if any(not part for part in parts):
        raise UnparseableDomainCode(f"empty segment in domain code: {code!r}")

    head, *rest = parts

    # --- prefix-less wikis: the mobile marker leads, e.g. `m.wd`, `m.w` ----------
    # Guarded by len(rest) == 1 so that a hypothetical language "m" cannot swallow
    # longer forms; no such language exists, but the check keeps the branch honest.
    if head == "m" and len(rest) == 1:
        project = PROJECT_BY_SUFFIX.get(rest[0])
        if project is None:
            raise UnparseableDomainCode(f"unknown project suffix in {code!r}")
        return DomainCode(raw=code, project=project, language=None, access_method="mobile-web")

    # --- wikimedia.org sites: the trailing `.m` is the PROJECT, not mobile -------
    if head in WIKIMEDIA_SITES:
        if rest == ["m"]:
            return DomainCode(raw=code, project="wikimedia", language=head, access_method="desktop")
        if rest == ["m", "m"]:
            return DomainCode(
                raw=code, project="wikimedia", language=head, access_method="mobile-web"
            )
        raise UnparseableDomainCode(f"unexpected shape for wikimedia site: {code!r}")

    # --- language-style wikis: `<lang>[.m][.<project>]` --------------------------
    if not _looks_like_a_wiki_prefix(head):
        raise UnparseableDomainCode(f"implausible language/site segment: {code!r}")

    access: AccessMethod = "desktop"
    if rest and rest[0] == "m":
        access = "mobile-web"
        rest = rest[1:]

    if not rest:
        return DomainCode(raw=code, project="wikipedia", language=head, access_method=access)

    if len(rest) > 1:
        raise UnparseableDomainCode(f"too many segments after the mobile marker: {code!r}")

    project = PROJECT_BY_SUFFIX.get(rest[0])
    if project is None:
        raise UnparseableDomainCode(f"unknown project suffix {rest[0]!r} in {code!r}")

    return DomainCode(raw=code, project=project, language=head, access_method=access)


#: Inverse of PROJECT_BY_SUFFIX, for reconstructing codes.
SUFFIX_BY_PROJECT: Final[dict[str, str]] = {
    project: suffix for suffix, project in PROJECT_BY_SUFFIX.items()
}


def build_domain_code(*, project: str, language: str | None, access_method: AccessMethod) -> str:
    """Reconstruct a `domain_code` from its parts - the inverse of `parse_domain_code`.

    Needed because the company filter works the other way round: Wikidata gives
    (language, project, title), and matching against the dump requires the code the
    dump would use. Doing the conversion here, rather than approximating it in SQL,
    keeps one definition of the scheme and lets a round-trip test prove the two
    directions agree across the entire real inventory.
    """
    mobile = access_method == "mobile-web"

    if language is None:
        suffix = SUFFIX_BY_PROJECT.get(project)
        if suffix is None:
            raise ValueError(f"no suffix known for prefix-less project {project!r}")
        return f"m.{suffix}" if mobile else suffix

    if project == "wikimedia":
        # `commons` -> `commons.m`, and mobile adds a second `.m`.
        return f"{language}.m.m" if mobile else f"{language}.m"

    if project == "wikipedia":
        return f"{language}.m" if mobile else language

    suffix = SUFFIX_BY_PROJECT.get(project)
    if suffix is None:
        raise ValueError(f"unknown project {project!r}")
    return f"{language}.m.{suffix}" if mobile else f"{language}.{suffix}"


def _looks_like_a_wiki_prefix(segment: str) -> bool:
    """Reject obvious junk without hardcoding every valid wiki.

    New language editions appear over time, so this is deliberately permissive about
    *which* code it is and strict only about the shape. It exists to catch the `""`
    row and anything similarly malformed, not to validate against ISO 639.
    """
    return segment.replace("-", "").replace("_", "").isalnum()
