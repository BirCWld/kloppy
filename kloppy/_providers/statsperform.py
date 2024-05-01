from typing import Optional, List

from kloppy.config import get_config
from kloppy.domain import TrackingDataset, EventDataset, EventFactory
from kloppy.infra.serializers.tracking.statsperform import (
    StatsPerformDeserializer as StatsPerformTrackingDeserializer,
    StatsPerformInputs as StatsPerformTrackingInputs,
)
from kloppy.infra.serializers.event.statsperform import (
    StatsPerformDeserializer as StatsPerformEventDeserializer,
    StatsPerformInputs as StatsPerformEventInputs,
)
from kloppy.io import FileLike, open_as_file
from kloppy.utils import deprecated


def load_tracking(
    ma1_data: FileLike,
    ma25_data: FileLike,
    sample_rate: Optional[float] = None,
    limit: Optional[int] = None,
    coordinates: Optional[str] = None,
    only_alive: Optional[bool] = False,
) -> TrackingDataset:
    deserializer = StatsPerformTrackingDeserializer(
        sample_rate=sample_rate,
        limit=limit,
        coordinate_system=coordinates,
        only_alive=only_alive,
    )
    with open_as_file(ma1_data) as meta_data_fp, open_as_file(
        ma25_data
    ) as raw_data_fp:
        return deserializer.deserialize(
            inputs=StatsPerformTrackingInputs(
                meta_data=meta_data_fp, raw_data=raw_data_fp
            )
        )


def load_event(
    ma1_data: FileLike,
    ma3_data: FileLike,
    event_types: Optional[List[str]] = None,
    coordinates: Optional[str] = None,
    event_factory: Optional[EventFactory] = None,
) -> EventDataset:
    """
    Load Opta event data into a [`EventDataset`][kloppy.domain.models.event.EventDataset]

    Parameters:
        f7_data: filename of json containing the events
        f24_data: filename of json containing the lineup information
        event_types:
        coordinates:
        event_factory:
    """
    deserializer = StatsPerformEventDeserializer(
        event_types=event_types,
        coordinate_system=coordinates,
        event_factory=event_factory or get_config("event_factory"),
    )
    with open_as_file(ma1_data) as ma1_data_fp, open_as_file(
        ma3_data
    ) as ma3_data_fp:
        return deserializer.deserialize(
            inputs=StatsPerformEventInputs(
                ma1_data=ma1_data_fp, ma3_data=ma3_data_fp
            ),
        )


@deprecated("statsperform.load_tracking should be used")
def load(
    meta_data: FileLike,  # Stats Perform MA1 file - xml or json - single game, live data & lineups
    raw_data: FileLike,  # Stats Perform MA25 file - txt - tracking data
    sample_rate: Optional[float] = None,
    limit: Optional[int] = None,
    coordinates: Optional[str] = None,
    only_alive: Optional[bool] = False,
) -> TrackingDataset:
    deserializer = StatsPerformTrackingDeserializer(
        sample_rate=sample_rate,
        limit=limit,
        coordinate_system=coordinates,
        only_alive=only_alive,
    )
    with open_as_file(meta_data) as meta_data_fp, open_as_file(
        raw_data
    ) as raw_data_fp:
        return deserializer.deserialize(
            inputs=StatsPerformTrackingInputs(
                meta_data=meta_data_fp, raw_data=raw_data_fp
            )
        )
