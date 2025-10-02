from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from tenacity import RetryError, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from models import CatalogItemSummary, PricingInfo

try:  # pragma: no cover - optional dependency
    from paapi5_python_sdk.api.default_api import DefaultApi
    from paapi5_python_sdk.models.get_items_request import GetItemsRequest
    from paapi5_python_sdk.models.get_items_resource import GetItemsResource
    from paapi5_python_sdk.models.partner_type import PartnerType
    from paapi5_python_sdk.rest import ApiException
except ImportError as exc:  # pragma: no cover
    DefaultApi = None  # type: ignore
    GetItemsRequest = None  # type: ignore
    GetItemsResource = None  # type: ignore
    PartnerType = None  # type: ignore
    ApiException = Exception
    logging.getLogger(__name__).warning(
        "paapi5-python-sdk is not installed. PAAPIClient will be unavailable: %s",
        exc,
    )


REQUIRED_ENV_VARS = {
    "PAAPI_ACCESS_KEY",
    "PAAPI_SECRET_KEY",
    "PAAPI_PARTNER_TAG",
    "PAAPI_REGION",
}

OPTIONAL_ENV_VARS = {"PAAPI_HOST"}


@dataclass
class PAAPICredentials:
    access_key: str
    secret_key: str
    partner_tag: str
    region: str
    host: Optional[str] = None


class MissingCredentialsError(RuntimeError):
    """Raised when PA-API credentials are missing."""


def _load_dotenv(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    env: Dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def load_credentials(env_path: str | Path = ".env") -> Optional[PAAPICredentials]:
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
    return PAAPICredentials(
        access_key=env["PAAPI_ACCESS_KEY"],
        secret_key=env["PAAPI_SECRET_KEY"],
        partner_tag=env["PAAPI_PARTNER_TAG"],
        region=env["PAAPI_REGION"],
        host=env.get("PAAPI_HOST") or None,
    )


class PAAPIClient:
    def __init__(self, credentials: PAAPICredentials) -> None:
        if DefaultApi is None:
            raise MissingCredentialsError(
                "paapi5-python-sdk is not installed. Install it to use PAAPIClient."
            )
        self.credentials = credentials
        self._client = DefaultApi()

    def _build_request(self, ean: str, marketplace: str) -> GetItemsRequest:
        resources = [
            GetItemsResource.ITEM_INFO_TITLE,
            GetItemsResource.ITEM_INFO_BY_LINE_INFO,
            GetItemsResource.ITEM_INFO_CLASSIFICATIONS,
            GetItemsResource.ITEM_INFO_PRODUCT_INFO,
            GetItemsResource.OFFERS_LISTINGS_PRICE,
            GetItemsResource.OFFERS_LISTINGS_PROMOTIONS,
        ]
        return GetItemsRequest(
            partner_tag=self.credentials.partner_tag,
            partner_type=PartnerType.ASSOCIATES,
            marketplace=marketplace,
            item_ids=[ean],
            resources=resources,
            id_type="EAN",
        )

    @retry(
        reraise=True,
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(ApiException),
    )
    def _invoke(self, request: GetItemsRequest):
        kwargs: Dict[str, Any] = {
            "get_items_request": request,
            "access_key": self.credentials.access_key,
            "secret_key": self.credentials.secret_key,
            "host": self.credentials.host,
            "region": self.credentials.region,
        }
        kwargs = {key: value for key, value in kwargs.items() if value is not None}
        return self._client.get_items(**kwargs)

    def lookup_ean(self, ean: str, marketplace: str) -> List[CatalogItemSummary]:
        request = self._build_request(ean, marketplace)
        try:
            response = self._invoke(request)
        except RetryError as exc:
            logging.getLogger(__name__).warning("PA-API lookup failed for %s on %s: %s", ean, marketplace, exc)
            return []
        except ApiException as exc:  # pragma: no cover
            logging.getLogger(__name__).warning("PA-API error for %s on %s: %s", ean, marketplace, exc)
            return []

        if not response.items_result or not response.items_result.items:
            return []

        summaries: List[CatalogItemSummary] = []
        for item in response.items_result.items:
            asin = item.asin
            title = None
            brand = None
            bullet_points: List[str] = []
            attributes: Dict[str, Any] = {}
            try:
                title = item.item_info.title.display_value if item.item_info and item.item_info.title else None
            except AttributeError:
                title = None
            try:
                brand = (
                    item.item_info.by_line_info.brand.display_value
                    if item.item_info and item.item_info.by_line_info and item.item_info.by_line_info.brand
                    else None
                )
            except AttributeError:
                brand = None
            try:
                bullet_attr = item.item_info.features.display_values if item.item_info and item.item_info.features else []
                if bullet_attr:
                    bullet_points.extend(bullet_attr)
            except AttributeError:
                pass
            try:
                attributes = item.item_info.to_dict() if item.item_info else {}
            except AttributeError:
                attributes = {}

            if asin:
                summaries.append(
                    CatalogItemSummary(
                        asin=asin,
                        marketplace_id=marketplace,
                        title=title,
                        brand=brand,
                        attributes=attributes,
                        bullet_points=bullet_points,
                    )
                )
        return summaries

    def get_featured_offer_price(self, asin: str, marketplace: str) -> Optional[PricingInfo]:
        # PA-API already returns offer listings in the lookup response; pricing can be pulled separately.
        # Since PA-API does not support retrieving pricing without an item lookup, this method returns None.
        return None


def create_client(env_path: str | Path = ".env") -> Optional[PAAPIClient]:
    credentials = load_credentials(env_path)
    if not credentials:
        return None
    return PAAPIClient(credentials)
