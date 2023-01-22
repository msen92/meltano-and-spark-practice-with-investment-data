"""turkish_lira_parities tap class."""

from typing import List

from datetime import datetime,timedelta
from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_turkish_lira_parities.streams import (
    gold_tl_parity_stream,
    dollar_tl_parity_stream,
    euro_tl_parity_stream
)

STREAM_TYPES = [
    gold_tl_parity_stream,
    dollar_tl_parity_stream,
    euro_tl_parity_stream
]

class Tapturkish_lira_parities(Tap):
    """turkish_lira_parities tap class."""
    name = "tap-turkish_lira_parities"

    current_date = datetime.strftime(datetime.now(),'%Y-%m-%d')
    yesterday = datetime.strftime(datetime.now() - timedelta(days = 1),'%Y-%m-%d')

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "Minimum Date",
            th.StringType,
            required=False,
            description="Minimum date to start extraction. Default value is yesterdays date.",
            default=yesterday
        ),
        th.Property(
            "Maximum Date",
            th.StringType,
            required=False,
            description="Maximum date to end extraction. Default value is current date.",
            default=current_date
        )
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

if __name__ == "__main__":
    Tapturkish_lira_parities.cli()