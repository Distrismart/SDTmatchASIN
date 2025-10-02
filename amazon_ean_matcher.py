from __future__ import annotations

import argparse
import csv
import logging
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from tqdm import tqdm

from models import CatalogItemSummary, LookupResult, PricingInfo
from pack_size import extract_pack_size
from paapi_client import create_client as create_paapi_client
from spapi_client import create_client as create_spapi_client

try:  # pragma: no cover - optional dependency
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover
    fuzz = None  # type: ignore

logger = logging.getLogger("amazon_ean_matcher")

OUTPUT_COLUMNS = [
    "ean",
    "marketplace",
    "asin",
    "title",
    "brand",
    "pack_size",
    "current_sales_price",
]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Match EANs to Amazon ASINs")
    parser.add_argument("--input", required=True, help="Path to the input CSV file containing EANs")
    parser.add_argument("--output", required=True, help="Path to the output CSV file for matches")
    parser.add_argument(
        "--marketplaces",
        required=True,
        help="Comma-separated list of marketplace codes (e.g., DE,FR,IT)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of worker threads used for marketplace lookups",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )
    parser.add_argument(
        "--resume-from",
        help="Resume processing starting from the provided EAN value",
    )
    parser.add_argument(
        "--skip-price",
        action="store_true",
        help="Skip fetching featured offer pricing",
    )
    return parser.parse_args(argv)


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def read_input_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Input file {path} not found")
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ValueError("Input CSV must contain headers including an 'ean' column")
        normalized_fieldnames = [name.strip() for name in reader.fieldnames]
        ean_column = next((name for name in normalized_fieldnames if name.lower() == "ean"), None)
        if not ean_column:
            raise ValueError("Input CSV must include an 'ean' column")
        rows: List[Dict[str, str]] = []
        for row in reader:
            normalized_row = {key.strip(): value.strip() if isinstance(value, str) else value for key, value in row.items()}
            if not normalized_row.get(ean_column):
                continue
            rows.append(normalized_row)
        return rows


def normalize_marketplaces(raw: str) -> List[str]:
    marketplaces: List[str] = []
    for part in raw.split(","):
        part = part.strip().upper()
        if part:
            marketplaces.append(part)
    return marketplaces


def brand_matches(expected: Optional[str], actual: Optional[str], threshold: int = 80) -> bool:
    if not expected or not actual:
        return True
    expected_norm = expected.strip().lower()
    actual_norm = actual.strip().lower()
    if not expected_norm or not actual_norm:
        return True
    if expected_norm == actual_norm:
        return True
    if fuzz:
        try:
            score = fuzz.partial_ratio(expected_norm, actual_norm)
            return score >= threshold
        except Exception:  # pragma: no cover - library specific
            return expected_norm in actual_norm or actual_norm in expected_norm
    return expected_norm in actual_norm or actual_norm in expected_norm


def make_lookup_result(
    ean: str,
    marketplace: str,
    item: CatalogItemSummary,
    pricing: Optional[PricingInfo],
) -> LookupResult:
    return LookupResult(ean=ean, marketplace=marketplace, item=item, pricing=pricing)


def process_marketplace(
    ean: str,
    marketplace: str,
    input_brand: Optional[str],
    client,
    skip_price: bool,
    seen: Dict[Tuple[str, str], Set[str]],
    seen_lock: threading.Lock,
) -> List[LookupResult]:
    results: List[LookupResult] = []
    try:
        items = client.lookup_ean(ean, marketplace)
    except Exception as exc:  # pragma: no cover - network safeguard
        logger.error("Lookup failed for %s on %s: %s", ean, marketplace, exc)
        return results
    if not items:
        return results

    for item in items:
        asin = item.asin
        if not asin:
            continue
        key = (ean, marketplace)
        with seen_lock:
            asin_set = seen.setdefault(key, set())
            if asin in asin_set:
                continue
            asin_set.add(asin)
        if not brand_matches(input_brand, item.brand):
            logger.debug("Skipping ASIN %s on %s due to brand mismatch (%s vs %s)", asin, marketplace, input_brand, item.brand)
            continue
        pricing = None
        if not skip_price:
            try:
                pricing = client.get_featured_offer_price(asin, marketplace)
            except Exception as exc:  # pragma: no cover - network safeguard
                logger.warning("Failed to fetch pricing for %s on %s: %s", asin, marketplace, exc)
                pricing = None
            else:
                if pricing and pricing.currency:
                    logger.debug(
                        "Pricing for %s on %s: %s %s (source=%s)",
                        asin,
                        marketplace,
                        pricing.currency,
                        pricing.price,
                        pricing.source,
                    )
        results.append(make_lookup_result(ean, marketplace, item, pricing))
    return results


def write_output(path: Path, results: Iterable[LookupResult]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for result in results:
            price_value = ""
            if result.pricing and result.pricing.price is not None:
                price_value = f"{result.pricing.price:.2f}"
            pack_size_value = extract_pack_size(
                result.item.attributes,
                title=result.item.title,
                bullet_points=result.item.bullet_points,
                locale=result.marketplace,
            )
            writer.writerow(
                {
                    "ean": result.ean,
                    "marketplace": result.marketplace,
                    "asin": result.item.asin,
                    "title": result.item.title or "",
                    "brand": result.item.brand or "",
                    "pack_size": str(pack_size_value) if pack_size_value is not None else "",
                    "current_sales_price": price_value,
                }
            )


def summarize(all_eans: Sequence[str], matches: List[LookupResult]) -> None:
    unique_eans = list(dict.fromkeys(all_eans))
    matched_eans = {result.ean for result in matches}
    marketplace_counts: Dict[str, int] = defaultdict(int)
    for result in matches:
        marketplace_counts[result.marketplace] += 1
    logger.info("Processed %d EANs", len(unique_eans))
    logger.info("Matched %d EAN/marketplace combinations", len(matches))
    for marketplace, count in sorted(marketplace_counts.items()):
        logger.info("%s matches: %d", marketplace, count)
    unmatched = [ean for ean in unique_eans if ean not in matched_eans]
    if unmatched:
        logger.warning("Unmatched EANs: %d", len(unmatched))
        logger.debug("Unmatched list: %s", ", ".join(unmatched))
    else:
        logger.info("All EANs matched at least one ASIN")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_level)

    input_path = Path(args.input)
    output_path = Path(args.output)

    rows = read_input_rows(input_path)
    if not rows:
        logger.warning("No EANs found in input file")
        return 0

    marketplaces = normalize_marketplaces(args.marketplaces)
    if not marketplaces:
        logger.error("No valid marketplaces provided")
        return 1

    logger.info("Attempting to create SP-API client")
    client = create_spapi_client(max_concurrency=max(args.max_workers, 1))
    if not client:
        logger.info("SP-API credentials not found. Falling back to PA-API client")
        client = create_paapi_client()
    if not client:
        logger.error("No Amazon API credentials available. Aborting.")
        return 2

    seen: Dict[Tuple[str, str], Set[str]] = {}
    seen_lock = threading.Lock()

    results: List[LookupResult] = []
    processed_eans: List[str] = []
    total_eans = len(rows)
    resume_from = args.resume_from.strip() if args.resume_from else None
    resume_reached = resume_from is None

    with ThreadPoolExecutor(max_workers=max(args.max_workers, 1)) as executor:
        with tqdm(total=total_eans, desc="Processing EANs", unit="ean") as progress:
            for row in rows:
                ean_column = next((key for key in row.keys() if key.lower() == "ean"), None)
                if not ean_column:
                    continue
                ean = row[ean_column].strip()
                if not ean:
                    continue
                if not resume_reached:
                    if ean == resume_from:
                        resume_reached = True
                    else:
                        progress.update(1)
                        continue
                input_brand = next((row.get(key) for key in row if key.lower() == "brand"), None)
                future_to_marketplace = {
                    executor.submit(
                        process_marketplace,
                        ean,
                        marketplace,
                        input_brand,
                        client,
                        args.skip_price,
                        seen,
                        seen_lock,
                    ): marketplace
                    for marketplace in marketplaces
                }
                for future in as_completed(future_to_marketplace):
                    marketplace = future_to_marketplace[future]
                    try:
                        marketplace_results = future.result()
                    except Exception as exc:  # pragma: no cover - runtime safety
                        logger.error("Marketplace processing failed for %s/%s: %s", ean, marketplace, exc)
                        continue
                    for result in marketplace_results:
                        results.append(result)
                        if result.pricing and result.pricing.currency:
                            logger.info(
                                "Currency for %s on %s: %s",
                                result.item.asin,
                                marketplace,
                                result.pricing.currency,
                            )
                        else:
                            logger.debug(
                                "No pricing information for %s on %s", result.item.asin, marketplace
                            )
                progress.update(1)
                processed_eans.append(ean)

    write_output(output_path, results)
    summarize(processed_eans, results)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:  # pragma: no cover - cli convenience
        logger.warning("Interrupted by user")
        sys.exit(130)
