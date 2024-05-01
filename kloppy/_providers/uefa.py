from typing import Union

from kloppy.config import get_config
from kloppy.infra.serializers.event.uefa import (
    UefaDeserializer,
    UefaInputs,
)
from kloppy.domain import EventDataset, Optional, List, EventFactory
from kloppy.io import open_as_file


def load(
    match_id: Union[str, int] = "15946",
    event_types: Optional[List[str]] = None,
    coordinates: Optional[str] = None,
    event_factory: Optional[EventFactory] = None,
) -> EventDataset:
    deserializer = UefaDeserializer(
        event_types=event_types,
        coordinate_system=coordinates,
        event_factory=event_factory or get_config("event_factory")
    )
    event_data = f"https://match.uefa.com/v5/matches/{match_id}/events?filter=ALL&offset=0&limit=500"
    lineup_data = f"https://match.uefa.com/v5/matches/{match_id}/lineups"
    with open_as_file(event_data) as event_data_fp, open_as_file(lineup_data) as lineup_data_fp:
       return deserializer.deserialize(
            inputs=UefaInputs(
                event_data=event_data_fp,
                lineup_data=lineup_data_fp,
            ),
        )
