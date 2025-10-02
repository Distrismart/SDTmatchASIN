from __future__ import annotations

import logging
from typing import Iterator
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def sp_client() -> MagicMock:
    client = MagicMock(name="sp_client")
    client.search_items.return_value = []
    return client


@pytest.fixture
def pa_client() -> MagicMock:
    client = MagicMock(name="pa_client")
    client.search_items.return_value = []
    return client


@pytest.fixture
def list_logger() -> Iterator[logging.Logger]:
    logger = logging.getLogger("tests")
    original_level = logger.level
    logger.setLevel(logging.INFO)
    yield logger
    logger.setLevel(original_level)
