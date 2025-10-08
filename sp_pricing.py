import time
from typing import Dict, List, Optional

from sp_api.base import Marketplaces
from sp_api.base.client import Client

# Map short codes you use in the UI to SP-API marketplace objects

MP = {
    "de": Marketplaces.DE,
    "fr": Marketplaces.FR,
    "it": Marketplaces.IT,
    "es": Marketplaces.ES,
    "uk": Marketplaces.GB,
}

def _lowest_landed_price(offers: list) -> Optional[float]:
    """
    Compute lowest landed (item + shipping) among the returned offers.
    """
    low = None
    for off in offers or []:
        lp = (((off.get("ListingPrice") or {}).get("Amount")))
        shp = (((off.get("Shipping") or {}).get("Amount")))
        if lp is None:
            continue
        total = float(lp) + float(shp or 0)
        low = total if low is None else min(low, total)
    return low


def get_item_offers_batch(
    asins: List[str],
    marketplace: str,
    condition: str = "New",
    sleep_between_batches: float = 0.5,
) -> Dict[str, float]:
    """
    Returns a dict {asin: price} for the given marketplace, using
    POST /batches/products/pricing/v0/itemOffers (limit 20 ASINs per request).
    """
    marketplace = marketplace.lower()
    if marketplace not in MP:
        raise ValueError(f"Unknown marketplace code: {marketplace}")

    marketplace_info = MP[marketplace]
    mkt_id = marketplace_info.marketplace_id
    client_kwargs = {"marketplace": marketplace_info}
    region = getattr(marketplace_info, "region", None)
    if region:
        client_kwargs["region"] = region
    client = Client(**client_kwargs)  # picks up your env/.env via python-amazon-sp-api

    prices: Dict[str, float] = {}
    BATCH_LIMIT = 20

    for i in range(0, len(asins), BATCH_LIMIT):
        batch = [a for a in asins[i:i+BATCH_LIMIT] if a]
        if not batch:
            continue

        body = {
            "requests": [
                {
                    "uri": f"/products/pricing/v0/items/{asin}/offers",
                    "method": "GET",
                    "MarketplaceId": mkt_id,
                    "ItemCondition": condition,
                }
                for asin in batch
            ]
        }

        resp = client._request(
            path="/batches/products/pricing/v0/itemOffers",
            data=body
        )

        for item in (resp.payload or {}).get("responses", []):
            asin = item.get("asin")
            body = item.get("body") or {}
            offers = body.get("Offers") or []
            low = _lowest_landed_price(offers)
            if asin and low is not None:
                prices[asin] = low

        time.sleep(sleep_between_batches)

    return prices
