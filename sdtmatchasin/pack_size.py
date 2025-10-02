"""Utilities for determining product pack sizes."""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any, Iterable, Mapping, Optional

# Patterns tuned for German and French heuristics. We keep the patterns very
# small so that the behaviour is easy to reason about in the accompanying unit
# tests.
_GERMAN_PATTERNS = (
    r"(?P<count>\d+)\s*(?:er\s*)?(?:pack|stück|st\.?|tlg)\b",
    r"(?P<count>\d+)\s*(?:x|mal)\s*(?:stück|pack)\b",
)
_FRENCH_PATTERNS = (
    r"lot\s+de\s+(?P<count>\d+)",
    r"paquet\s+de\s+(?P<count>\d+)",
)
# Fallback for titles in any locale.
_GENERIC_PATTERNS = (
    r"pack\s+of\s+(?P<count>\d+)",
    r"(?P<count>\d+)\s*(?:pack|pcs|pieces|units?)\b",
)


def _extract_numeric(value: Any) -> Optional[int]:
    """Normalise different structured attribute formats to ``int``."""

    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, str):
        # Strings may contain commas or other separators, keep it simple.
        cleaned = value.strip().lower().replace(",", ".")
        if not cleaned:
            return None
        match = re.match(r"(\d+)", cleaned)
        if match:
            return int(match.group(1))
    return None


def _search_patterns(title: str, patterns: Iterable[str]) -> Optional[int]:
    for pattern in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            try:
                return int(match.group("count"))
            except (KeyError, ValueError):
                continue
    return None


def parse_pack_size(product: Mapping[str, Any]) -> int:
    """Return the inferred pack size for an Amazon catalogue entry.

    The function first looks for structured attribute information. If no usable
    number is found, a set of light-weight heuristics for German and French
    titles is applied before falling back to generic English style patterns.
    """

    attributes: Mapping[str, Any] = product.get("attributes", {}) or {}
    for key in ("item_package_quantity", "number_of_items", "packageQuantity"):
        value = _extract_numeric(attributes.get(key))
        if value:
            return value

    title = (product.get("title") or "").strip()
    if not title:
        return 1

    locale = (product.get("locale") or product.get("language") or "").lower()

    if locale.startswith("de"):
        value = _search_patterns(title, _GERMAN_PATTERNS)
        if value:
            return value
    elif locale.startswith("fr"):
        value = _search_patterns(title, _FRENCH_PATTERNS)
        if value:
            return value

    value = _search_patterns(title, _GENERIC_PATTERNS)
    if value:
        return value

    # Titles may include numbers within parentheses (e.g. "(3 Stück)"). As a
    # last attempt we fall back to a simple ``Nx`` detection.
    fallback = re.search(r"(?P<count>\d+)\s*[x×]\s*\b", title)
    if fallback:
        try:
            return int(fallback.group("count"))
        except ValueError:
            pass

    return 1


__all__ = ["parse_pack_size"]
