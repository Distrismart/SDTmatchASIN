from __future__ import annotations

import concurrent.futures
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from sdtmatchasin.cli import Offer, lookup_ean, lookup_eans


def test_lookup_ean_deduplicates_and_prefers_lowest_price(sp_client: MagicMock, pa_client: MagicMock):
    sp_client.search_items.return_value = [
        {"asin": "B001", "price": Decimal("19.99"), "currency": "EUR"},
        {"asin": "B001", "price": Decimal("18.99"), "currency": "EUR"},
        {"asin": "B002", "price": Decimal("11.50"), "currency": "EUR"},
        {"asin": "B002", "price": Decimal("15.00"), "currency": "EUR"},
    ]

    offers = lookup_ean("4006381333931", sp_client, pa_client)

    assert offers == [
        Offer(
            ean="4006381333931",
            asin="B001",
            price=18.99,
            currency="EUR",
            source="sp",
            sources=["sp"],
        ),
        Offer(
            ean="4006381333931",
            asin="B002",
            price=11.5,
            currency="EUR",
            source="sp",
            sources=["sp"],
        ),
    ]
    sp_client.search_items.assert_called_once_with("4006381333931")
    pa_client.search_items.assert_not_called()


def test_lookup_ean_retries_and_falls_back(sp_client: MagicMock, pa_client: MagicMock, caplog: pytest.LogCaptureFixture):
    responses = [RuntimeError("boom"), [], [{"asin": "B003", "price": 9.99, "currency": "EUR"}]]

    def sp_side_effect(_):
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    sp_client.search_items.side_effect = sp_side_effect
    pa_client.search_items.return_value = [{"asin": "B003", "price": 12.99, "currency": "EUR"}]

    with caplog.at_level("WARNING"):
        offers = lookup_ean("0001234567895", sp_client, pa_client, retries=2, retry_delay=0)

    assert offers[0].price == 9.99
    assert "SP-API lookup failed" in caplog.text
    assert sp_client.search_items.call_count == 3
    pa_client.search_items.assert_not_called()


def test_lookup_ean_falls_back_to_pa_on_empty_results(sp_client: MagicMock, pa_client: MagicMock):
    sp_client.search_items.return_value = []
    pa_client.search_items.return_value = [{"asin": "B004", "price": 6.49, "currency": "EUR"}]

    offers = lookup_ean("123", sp_client, pa_client, retries=1)

    assert offers == [
        Offer(
            ean="123",
            asin="B004",
            price=6.49,
            currency="EUR",
            source="pa",
            sources=["pa"],
        )
    ]


def test_lookup_eans_uses_configured_thread_pool(monkeypatch, sp_client: MagicMock, pa_client: MagicMock):
    submitted = []

    class DummyFuture(concurrent.futures.Future):
        def __init__(self, result):
            super().__init__()
            self.set_result(result)

    class DummyExecutor:
        created = []

        def __init__(self, max_workers=None):
            self.max_workers = max_workers
            self.submitted = []
            DummyExecutor.created.append(self)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            submitted.append((fn, args, kwargs))
            return DummyFuture(fn(*args, **kwargs))

    monkeypatch.setattr("sdtmatchasin.cli.concurrent.futures.ThreadPoolExecutor", DummyExecutor)

    sp_client.search_items.return_value = [{"asin": "B005", "price": 5, "currency": "EUR"}]

    offers = lookup_eans(["111", "222"], sp_client, pa_client, max_workers=3)

    assert {offer.asin for offer in offers} == {"B005"}
    assert DummyExecutor.created[0].max_workers == 3
    assert len(submitted) == 2


def test_lookup_eans_deduplicates_across_clients(sp_client: MagicMock, pa_client: MagicMock):
    sp_client.search_items.side_effect = lambda ean: [{"asin": f"ASIN-{ean}", "price": 10.0, "currency": "EUR"}]
    pa_client.search_items.side_effect = lambda ean: [{"asin": f"ASIN-{ean}", "price": 8.0, "currency": "EUR"}]

    offers = lookup_eans(["900", "900", "901"], sp_client, pa_client)

    lookup_map = {offer.asin: offer for offer in offers}
    assert lookup_map["ASIN-900"].price == 10.0
    assert lookup_map["ASIN-900"].sources == ["sp"]
    assert lookup_map["ASIN-901"].price == 10.0
    assert sp_client.search_items.call_count == 2
    # PA client should only be used when SP returns no offers
    assert pa_client.search_items.call_count == 0
