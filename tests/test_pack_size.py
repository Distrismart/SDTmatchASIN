from __future__ import annotations

import pytest

from sdtmatchasin.pack_size import parse_pack_size


@pytest.mark.parametrize(
    "product,expected",
    [
        (
            {
                "attributes": {"item_package_quantity": 12},
                "title": "6er Pack Küchenrollen",
                "locale": "de_DE",
            },
            12,
        ),
        (
            {
                "attributes": {},
                "title": "6er Pack Küchenrollen (6 Stück)",
                "locale": "de_DE",
            },
            6,
        ),
        (
            {
                "attributes": {},
                "title": "Lot de 3 brosses à dents",  # French heuristic
                "locale": "fr_FR",
            },
            3,
        ),
        (
            {
                "attributes": {},
                "title": "Travel bottles pack of 4",
                "locale": "en_GB",
            },
            4,
        ),
    ],
)
def test_parse_pack_size_heuristics(product, expected):
    assert parse_pack_size(product) == expected


def test_structured_attribute_precedence_over_title():
    product = {
        "attributes": {"number_of_items": "24"},
        "title": "Lot de 3 capsules",
        "locale": "fr_FR",
    }
    assert parse_pack_size(product) == 24
