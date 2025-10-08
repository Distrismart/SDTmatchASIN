from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence


@dataclass
class CatalogItemSummary:
    """Normalized representation of a catalog item returned by Amazon APIs."""

    asin: str
    marketplace_id: str
    title: Optional[str]
    brand: Optional[str]
    attributes: Dict[str, Any]
    bullet_points: Sequence[str]


@dataclass
class LookupResult:
    """Result of an ASIN lookup for a specific EAN and marketplace."""

    ean: str
    marketplace: str
    item: CatalogItemSummary
