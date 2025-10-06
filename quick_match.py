import os, csv, sys, re
from typing import Iterable, List, Dict, Any
from sp_api.base import Marketplaces
from spapi_compat import CatalogItems   # our shim

def creds() -> dict:
    return {
        "refresh_token": os.getenv("SP_API_REFRESH_TOKEN") or os.getenv("LWA_REFRESH_TOKEN"),
        "lwa_app_id": os.getenv("SP_API_CLIENT_ID") or os.getenv("LWA_APP_ID") or os.getenv("LWA_CLIENT_ID"),
        "lwa_client_secret": os.getenv("SP_API_CLIENT_SECRET") or os.getenv("LWA_CLIENT_SECRET"),
        "aws_access_key": os.getenv("AWS_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_key": os.getenv("AWS_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"),
        "role_arn": os.getenv("SP_API_ROLE_ARN") or os.getenv("ROLE_ARN"),
    }

MP = {"de": Marketplaces.DE, "fr": Marketplaces.FR}

def pack_size_from_title(title: str) -> str:
    if not title: return ""
    for p in [
        r"(\d+)\s*(?:St[üu]ck|Stk\.?|Pack|er[-\s]?Pack|x)\b",
        r"lot\s*de\s*(\d+)",
        r"(\d+)\s*(?:pcs?|pieces?|count)\b",
        r"x\s*(\d+)\b",
    ]:
        m = re.search(p, title, flags=re.I)
        if m: return m.group(1)
    return ""

def read_eans(path: str) -> Iterable[str]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "ean" not in (reader.fieldnames or []):
            print("Input CSV must have header 'ean'", file=sys.stderr); sys.exit(1)
        for row in reader:
            e = (row.get("ean") or "").strip()
            if e: yield e

def _resp_items(resp: Any) -> List[Dict[str, Any]]:
    """Normalize various shapes to a list of item dicts."""
    payload = getattr(resp, "payload", resp)
    if isinstance(payload, dict):
        return payload.get("items") or payload.get("Items") or []
    return payload or []

def _title_brand_from_item(it: Dict[str, Any]) -> (str, str):
    title = brand = ""
    # Newer responses
    summaries = it.get("summaries") or []
    if summaries:
        s = summaries[0]
        title = s.get("itemName") or s.get("title") or ""
        brand = s.get("brand") or ""
        return title, brand
    # Older attribute shape
    attrs = it.get("attributes") or {}
    if isinstance(attrs, dict):
        raw_title = attrs.get("item_name") or attrs.get("title") or ""
        title = raw_title[0] if isinstance(raw_title, list) else raw_title or ""
        raw_brand = attrs.get("brand") or ""
        brand = raw_brand[0] if isinstance(raw_brand, list) else raw_brand or ""
    return title, brand

def main(in_csv: str, out_csv: str, marketplaces: List[str]) -> None:
    out_rows: List[Dict[str, Any]] = []
    c = creds()
    for m in marketplaces:
        mp = MP[m]
        cat = CatalogItems(credentials=c, marketplace=mp)
        for ean in read_eans(in_csv):
            # Call signature (robust across library versions):
            # We try multiple variants in order. On QuotaExceeded we back off and retry.
            import time
            def _search(cat, ean):
                variants = [
                    dict(identifiers=[ean], identifiersType="EAN"),
                    dict(identifiers=ean, identifiersType="EAN"),
                    dict(keywords=[ean]),
                    dict(keywords=ean),
                    dict(query=ean),  # some very old builds
                ]
                # simple backoff if quota exceeded
                backoff = 2
                for attempt in range(6):
                    for kwargs in variants:
                        try:
                            return cat.search_catalog_items(**kwargs)
                        except Exception as e:
                            msg = str(e)
                            # keep trying next variant for InvalidInput
                            if "InvalidInput" in msg or "Missing required 'identifiers' or 'keywords'" in msg:
                                continue
                            # on quota exceeded, wait and retry whole sequence
                            if "QuotaExceeded" in msg or "You exceeded your quota" in msg:
                                time.sleep(backoff)
                                backoff = min(backoff * 2, 30)
                                break
                            # other errors: try next variant
                            continue
                    else:
                        # exhausted all variants without quota error; stop retry loop
                        break
                # If we reach here, nothing worked
                raise RuntimeError("All search_catalog_items variants failed")
            resp = _search(cat, ean)

            items = _resp_items(resp)
            if not items:
                out_rows.append({
                    "ean": ean, "marketplace": m.upper(), "asin": "",
                    "title": "", "brand": "", "pack_size": "", "current_sales_price": ""
                })
                continue

            for it in items:
                asin = (it.get("asin") or "").strip()
                title, brand = _title_brand_from_item(it)
                out_rows.append({
                    "ean": ean,
                    "marketplace": m.upper(),
                    "asin": asin,
                    "title": title,
                    "brand": brand,
                    "pack_size": pack_size_from_title(title),
                    "current_sales_price": "",  # pricing skipped in this quick runner
                })

    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ean","marketplace","asin","title","brand","pack_size","current_sales_price"])
        w.writeheader(); w.writerows(out_rows)
    print(f"✅ Wrote {len(out_rows)} rows to {out_csv}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--marketplaces", default="de,fr")
    args = ap.parse_args()
    mps = [m.strip().lower() for m in args.marketplaces.split(",") if m.strip()]
    main(args.input, args.output, mps)
