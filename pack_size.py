from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, Optional, Sequence


_STRUCTURED_KEYS = {
    "itempackagequantity",
    "numberofitems",
    "item_package_quantity",
    "itemPackageQuantity",
    "numberOfItems",
    "unitcount",
    "unitCount",
}


_GERMAN_REGEXES = [
    re.compile(r"\b(?:packung|pack|vorrat|set)\s*(?:mit)?\s*(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,3})\s*(?:st(?:ü|u)ck|er)\b", re.IGNORECASE),
    re.compile(r"\bpack\s*zu\s*(\d{1,3})\b", re.IGNORECASE),
]

_FRENCH_REGEXES = [
    re.compile(r"\b(?:lot|pack|paquet|bo[iî]te)\s*(?:de|de\s*|)\s*(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,3})\s*(?:unit[ée]s?|pi[eè]ces?)\b", re.IGNORECASE),
]

_ENGLISH_REGEXES = [
    re.compile(r"\bpack\s*of\s*(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,3})\s*(?:count|pack|ct|pcs?)\b", re.IGNORECASE),
    re.compile(r"\bvalue\s*pack\s*(\d{1,3})\b", re.IGNORECASE),
]


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if value > 0:
            return int(value)
        return None
    try:
        value_str = str(value).strip()
    except Exception:
        return None
    if not value_str:
        return None
    value_str = value_str.replace(",", "")
    if value_str.isdigit():
        return int(value_str)
    match = re.search(r"(\d{1,3})", value_str)
    if match:
        return int(match.group(1))
    return None


def _flatten_mapping(mapping: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(mapping, Mapping):
        for key, value in mapping.items():
            yield key, value
            yield from _flatten_mapping(value)
    elif isinstance(mapping, Sequence) and not isinstance(mapping, (str, bytes)):
        for value in mapping:
            yield from _flatten_mapping(value)


def _structured_pack_size(attributes: Mapping[str, Any]) -> Optional[int]:
    for key, value in _flatten_mapping(attributes):
        if isinstance(key, str) and key.lower() in _STRUCTURED_KEYS:
            candidate = None
            if isinstance(value, Mapping):
                candidate = _coerce_int(value.get("value") or value.get("Values") or value.get("values"))
            elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                for item in value:
                    candidate = _coerce_int(item if not isinstance(item, Mapping) else item.get("value"))
                    if candidate:
                        break
            else:
                candidate = _coerce_int(value)
            if candidate:
                return candidate
    return None


def _heuristic_pack_size(texts: Iterable[str], regexes: Sequence[re.Pattern]) -> Optional[int]:
    for text in texts:
        if not text:
            continue
        for regex in regexes:
            match = regex.search(text)
            if match:
                value = match.group(1)
                if value:
                    try:
                        size = int(value)
                    except ValueError:
                        continue
                    if size > 0:
                        return size
    return None


def extract_pack_size(
    attributes: Optional[Mapping[str, Any]] = None,
    *,
    title: Optional[str] = None,
    bullet_points: Optional[Sequence[str]] = None,
    locale: Optional[str] = None,
) -> Optional[int]:
    """Determine the pack size for an item using structured attributes and heuristics.

    Structured attribute values are preferred when available. When structured data does
    not provide a value, heuristics based on locale-specific patterns are applied to
    the title and bullet points.
    """

    attributes = attributes or {}
    pack_size = _structured_pack_size(attributes)
    if pack_size:
        return pack_size

    texts = []
    if title:
        texts.append(title)
    if bullet_points:
        texts.extend(bp for bp in bullet_points if bp)

    if not texts:
        return None

    locale = (locale or "").upper()
    regex_sets = []
    if locale.startswith("DE"):
        regex_sets.append(_GERMAN_REGEXES)
    if locale.startswith("FR"):
        regex_sets.append(_FRENCH_REGEXES)
    regex_sets.append(_ENGLISH_REGEXES)

    for regexes in regex_sets:
        pack_size = _heuristic_pack_size(texts, regexes)
        if pack_size:
            return pack_size

    return None
