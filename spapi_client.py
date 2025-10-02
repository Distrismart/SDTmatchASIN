from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from tenacity import RetryError, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from models import CatalogItemSummary, PricingInfo

try:
    from sp_api.api import CatalogItems, ProductPricing
    from sp_api.base import Marketplaces, SellingApiException
except ImportError as exc:  # pragma: no cover - optional dependency
    CatalogItems = None  # type: ignore
    ProductPricing = None  # type: ignore
    Marketplaces = None  # type: ignore
    SellingApiException = Exception
    logging.getLogger(__name__).warning(
        "python-sp-api is not installed. SPAPIClient will not function without it: %s",
        exc,
    )


REQUIRED_ENV_VARS = {
    "SP_API_REFRESH_TOKEN",
    "SP_API_LWA_APP_ID",
    "SP_API_LWA_CLIENT_SECRET",
    "SP_API_AWS_ACCESS_KEY",
    "SP_API_AWS_SECRET_KEY",
}

OPTIONAL_ENV_VARS = {"SP_API_ROLE_ARN", "SP_API_HOST"}

_MARKETPLACE_ALIASES = {
    "UK": "GB",
}


@dataclass
class SPAPICredentials:
    refresh_token: str
    lwa_app_id: str
    lwa_client_secret: str
    aws_access_key: str
    aws_secret_key: str
    role_arn: Optional[str] = None
    host: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "refresh_token": self.refresh_token,
            "lwa_app_id": self.lwa_app_id,
            "lwa_client_secret": self.lwa_client_secret,
            "aws_access_key": self.aws_access_key,
            "aws_secret_key": self.aws_secret_key,
        }
        if self.role_arn:
            data["role_arn"] = self.role_arn
        if self.host:
            data["host"] = self.host
        return data


class MissingCredentialsError(RuntimeError):
    """Raised when SP-API credentials are missing."""


def _load_dotenv(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    env: Dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def load_credentials(env_path: str | Path = ".env") -> Optional[SPAPICredentials]:
    env: Dict[str, str] = {}
    env.update({key: os.environ.get(key, "") for key in REQUIRED_ENV_VARS | OPTIONAL_ENV_VARS})

    missing = {key for key in REQUIRED_ENV_VARS if not env.get(key)}
    if missing:
        dotenv_values = _load_dotenv(Path(env_path))
        for key in REQUIRED_ENV_VARS | OPTIONAL_ENV_VARS:
            if not env.get(key) and key in dotenv_values:
                env[key] = dotenv_values[key]
        missing = {key for key in REQUIRED_ENV_VARS if not env.get(key)}
        if missing:
            return None

    return SPAPICredentials(
        refresh_token=env["SP_API_REFRESH_TOKEN"],
        lwa_app_id=env["SP_API_LWA_APP_ID"],
        lwa_client_secret=env["SP_API_LWA_CLIENT_SECRET"],
        aws_access_key=env["SP_API_AWS_ACCESS_KEY"],
        aws_secret_key=env["SP_API_AWS_SECRET_KEY"],
        role_arn=env.get("SP_API_ROLE_ARN") or None,
        host=env.get("SP_API_HOST") or None,
    )


class SPAPIClient:
    """Wrapper around python-sp-api providing higher level helpers."""

    def __init__(
        self,
        credentials: SPAPICredentials,
        *,
        max_concurrency: int = 5,
    ) -> None:
        if CatalogItems is None or ProductPricing is None or Marketplaces is None:
            raise MissingCredentialsError(
                "python-sp-api is not available. Install python-sp-api to use SPAPIClient."
            )

        self.credentials = credentials
        self._catalog_clients: Dict[str, CatalogItems] = {}
        self._pricing_clients: Dict[str, ProductPricing] = {}
        self._lock = threading.Lock()
        self._semaphore = threading.BoundedSemaphore(max_concurrency)

    # region Helpers
    def _get_marketplace(self, marketplace_code: str):
        normalized = _MARKETPLACE_ALIASES.get(marketplace_code.upper(), marketplace_code.upper())
        try:
            return getattr(Marketplaces, normalized)
        except AttributeError as exc:
            raise ValueError(f"Unsupported marketplace code: {marketplace_code}") from exc

    def _get_catalog_client(self, marketplace_code: str) -> CatalogItems:
        with self._lock:
            client = self._catalog_clients.get(marketplace_code)
            if client is None:
                credentials_dict = self.credentials.to_dict()
                client = CatalogItems(
                    marketplace=self._get_marketplace(marketplace_code),
                    credentials=credentials_dict,
                )
                self._catalog_clients[marketplace_code] = client
        return client

    def _get_pricing_client(self, marketplace_code: str) -> ProductPricing:
        with self._lock:
            client = self._pricing_clients.get(marketplace_code)
            if client is None:
                credentials_dict = self.credentials.to_dict()
                client = ProductPricing(
                    marketplace=self._get_marketplace(marketplace_code),
                    credentials=credentials_dict,
                )
                self._pricing_clients[marketplace_code] = client
        return client

    # endregion

    def _flatten_bullet_points(self, attributes: Dict[str, Any]) -> List[str]:
        bullet_points: List[str] = []
        candidate_keys = [
            "bullet_point",
            "bulletPoint",
            "bulletPoints",
            "bullet_points",
            "bulletpoint",
        ]
        for key in candidate_keys:
            if key in attributes:
                value = attributes[key]
                if isinstance(value, list):
                    bullet_points.extend(str(item) for item in value if item)
                elif isinstance(value, dict):
                    bullet_points.extend(
                        str(item)
                        for item in value.values()
                        if not isinstance(item, dict)
                    )
                elif value:
                    bullet_points.append(str(value))
        return bullet_points

    def _extract_attributes(self, item: Dict[str, Any]) -> Dict[str, Any]:
        attributes = item.get("attributes") or {}
        if isinstance(attributes, str):
            try:
                attributes = json.loads(attributes)
            except json.JSONDecodeError:
                attributes = {}
        return attributes

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(SellingApiException),
    )
    def _search_catalog_items(
        self,
        client: CatalogItems,
        marketplace: str,
        ean: str,
    ) -> Dict[str, Any]:
        return client.search_catalog_items(
            identifiers=[ean],
            identifiersType="EAN",
            marketplaceIds=[self._get_marketplace(marketplace).marketplace_id],
            includedData=["summaries", "identifiers", "attributes"],
        ).payload

    def lookup_ean(self, ean: str, marketplace: str) -> List[CatalogItemSummary]:
        client = self._get_catalog_client(marketplace)
        try:
            with self._semaphore:
                payload = self._search_catalog_items(client, marketplace, ean)
        except RetryError as exc:
            logging.getLogger(__name__).error("Failed to lookup EAN %s on %s: %s", ean, marketplace, exc)
            return []
        except SellingApiException as exc:  # pragma: no cover - network specific
            logging.getLogger(__name__).warning(
                "SP-API error while searching for %s on %s: %s", ean, marketplace, exc
            )
            return []

        items = payload.get("items") if isinstance(payload, dict) else payload
        if not items:
            return []

        marketplace_id = self._get_marketplace(marketplace).marketplace_id
        summaries: List[CatalogItemSummary] = []
        for item in items:
            summary_payload = {}
            for summary in item.get("summaries", []):
                if summary.get("marketplaceId") == marketplace_id:
                    summary_payload = summary
                    break
            title = summary_payload.get("itemName") if summary_payload else None
            brand = summary_payload.get("brandName") or summary_payload.get("brand")
            attributes = self._extract_attributes(item)
            bullet_points = self._flatten_bullet_points(attributes)
            asin = summary_payload.get("asin") or item.get("asin")
            if not asin:
                identifiers = item.get("identifiers") or {}
                if isinstance(identifiers, dict):
                    asin = next(
                        (
                            identifier.get("identifier")
                            for identifier in identifiers.get("marketplaceASIN") or []
                            if identifier.get("marketplaceId") == marketplace_id
                        ),
                        None,
                    )
            if not asin:
                continue
            summaries.append(
                CatalogItemSummary(
                    asin=asin,
                    marketplace_id=marketplace_id,
                    title=title,
                    brand=brand,
                    attributes=attributes,
                    bullet_points=bullet_points,
                )
            )
        return summaries

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(SellingApiException),
    )
    def _get_item_offers(
        self,
        client: ProductPricing,
        marketplace: str,
        asin: str,
    ) -> Dict[str, Any]:
        marketplace_id = self._get_marketplace(marketplace).marketplace_id
        return client.get_item_offers(
            asin=asin,
            marketplaceId=marketplace_id,
            itemCondition="New",
        ).payload

    def get_featured_offer_price(self, asin: str, marketplace: str) -> Optional[PricingInfo]:
        client = self._get_pricing_client(marketplace)
        try:
            with self._semaphore:
                payload = self._get_item_offers(client, marketplace, asin)
        except RetryError as exc:
            logging.getLogger(__name__).warning(
                "Failed to obtain pricing for %s on %s: %s", asin, marketplace, exc
            )
            return None
        except SellingApiException as exc:  # pragma: no cover
            logging.getLogger(__name__).warning(
                "SP-API pricing error for %s on %s: %s", asin, marketplace, exc
            )
            return None

        offers = payload.get("Offers") or payload.get("offers") or []
        if not offers:
            return None

        def parse_price(offer: Dict[str, Any]) -> Optional[PricingInfo]:
            price_info = offer.get("ListingPrice") or offer.get("listingPrice") or {}
            amount = price_info.get("Amount") or price_info.get("amount")
            currency = price_info.get("CurrencyCode") or price_info.get("currencyCode")
            if amount is None or currency is None:
                return None
            try:
                amount_value = float(amount)
            except (TypeError, ValueError):
                return None
            return PricingInfo(price=amount_value, currency=str(currency), source=offer.get("offerType", ""))

        featured_offer = next(
            (offer for offer in offers if offer.get("OfferType") == "Featured" or offer.get("offerType") == "Featured"),
            None,
        )
        if featured_offer:
            parsed = parse_price(featured_offer)
            if parsed:
                return parsed

        for offer in offers:
            parsed = parse_price(offer)
            if parsed:
                return parsed
        return None


def create_client(env_path: str | Path = ".env", max_concurrency: int = 5) -> Optional[SPAPIClient]:
    credentials = load_credentials(env_path)
    if not credentials:
        return None
    return SPAPIClient(credentials, max_concurrency=max_concurrency)
