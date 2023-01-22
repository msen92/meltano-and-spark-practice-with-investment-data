"""Stream type classes for tap-gold_tl_parity."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_turkish_lira_parities.client import gold_tl_parity_stream,dollar_tl_parity_stream,euro_tl_parity_stream

SCHEMA = th.PropertiesList(
        th.Property(
            "parity",
            th.DateType,
            description="Name of the parity"
        ),
        th.Property(
            "date",
            th.DateType,
            description="Date of the buying price"
        ),
        th.Property(
            "buying_price",
            th.StringType,
            description="Price of the gold for given date"
        )
    ).to_dict()

class gold_tl_parity_stream(gold_tl_parity_stream):
    """Define custom stream."""
    name = "gold_tl_parity_stream"
    schema = SCHEMA

class dollar_tl_parity_stream(dollar_tl_parity_stream):
    """Define custom stream."""
    name = "dollar_tl_parity_stream"
    schema = SCHEMA

class euro_tl_parity_stream(euro_tl_parity_stream):
    """Define custom stream."""
    name = "euro_tl_parity_stream"
    schema = SCHEMA