"""Command line helpers for the SDTmatchASIN project."""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

_LOGGER = logging.getLogger(__name__)


@dataclass
class Offer:
    """Normalised representation of an offer returned by a lookup client."""

    ean: str
    asin: str
    price: Optional[float]
    currency: Optional[str]
    source: str
    sources: List[str] = field(default_factory=list)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped.replace(",", "."))
        except ValueError:
            return None
    return None


def _normalise_offer(ean: str, raw: Mapping[str, Any], source: str) -> Offer:
    return Offer(
        ean=ean,
        asin=str(raw.get("asin")),
        price=_to_float(raw.get("price")),
        currency=raw.get("currency"),
        source=source,
        sources=[source],
    )


def _deduplicate_offers(offers: Iterable[Offer]) -> List[Offer]:
    combined: MutableMapping[str, Offer] = {}
    for offer in offers:
        existing = combined.get(offer.asin)
        if existing is None:
            combined[offer.asin] = Offer(
                ean=offer.ean,
                asin=offer.asin,
                price=offer.price,
                currency=offer.currency,
                source=offer.source,
                sources=list(offer.sources),
            )
            continue

        existing.sources = sorted(set(existing.sources + offer.sources))

        offer_price = offer.price
        if offer_price is None:
            continue
        if existing.price is None or offer_price < existing.price:
            existing.price = offer_price
            existing.currency = offer.currency
            existing.source = offer.source
    return list(combined.values())


def lookup_ean(
    ean: str,
    sp_client: Any,
    pa_client: Any,
    *,
    retries: int = 2,
    retry_delay: float = 0.0,
    logger: Optional[logging.Logger] = None,
) -> List[Offer]:
    """Lookup a single EAN using the provided API clients."""

    logger = logger or _LOGGER
    offers: List[Offer] = []

    for attempt in range(retries + 1):
        try:
            raw_results = sp_client.search_items(ean)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("SP-API lookup failed for %s (attempt %s/%s): %s", ean, attempt + 1, retries + 1, exc)
            if attempt < retries:
                if retry_delay:
                    time.sleep(retry_delay)
                continue
            break
        else:
            if raw_results:
                offers.extend(_normalise_offer(ean, result, "sp") for result in raw_results)
            if offers or attempt >= retries:
                break
            if retry_delay:
                time.sleep(retry_delay)

    if not offers:
        try:
            raw_results = pa_client.search_items(ean)
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("PA-API lookup failed for %s: %s", ean, exc)
            return []
        offers.extend(_normalise_offer(ean, result, "pa") for result in raw_results or [])

    return _deduplicate_offers(offers)


def lookup_eans(
    eans: Iterable[str],
    sp_client: Any,
    pa_client: Any,
    *,
    retries: int = 2,
    retry_delay: float = 0.0,
    max_workers: Optional[int] = None,
    logger: Optional[logging.Logger] = None,
) -> List[Offer]:
    """Lookup multiple EANs concurrently."""

    logger = logger or _LOGGER
    ean_list = list(dict.fromkeys(eans))
    offers: List[Offer] = []

    if not ean_list:
        return offers

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers or min(4, len(ean_list))) as executor:
        future_to_ean = {
            executor.submit(
                lookup_ean,
                ean,
                sp_client,
                pa_client,
                retries=retries,
                retry_delay=retry_delay,
                logger=logger,
            ): ean
            for ean in ean_list
        }
        for future in concurrent.futures.as_completed(future_to_ean):
            offers.extend(future.result())
    return offers


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Lookup ASINs by EAN")
    parser.add_argument("ean", nargs="+", help="EANs to look up")
    parser.add_argument("--max-workers", type=int, default=None, help="Number of worker threads")
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--retry-delay", type=float, default=0.0)
    args = parser.parse_args(list(argv) if argv is not None else None)

    # In a real deployment the clients would be constructed here. For the test
    # suite we expect dependency injection, so we simply raise a helpful error.
    raise SystemExit(
        "This CLI entry point is intended to be used with dependency injection "
        "during testing."
    )


__all__ = ["Offer", "lookup_ean", "lookup_eans", "main"]
