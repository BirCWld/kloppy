import json
import math
from typing import Tuple, Dict, List, NamedTuple, IO, Optional
import logging
from datetime import datetime
import pytz

from kloppy.domain import (
    EventDataset,
    Team,
    Period,
    Point,
    Point3D,
    BallState,
    DatasetFlag,
    Orientation,
    PassResult,
    ShotResult,
    TakeOnResult,
    DuelResult,
    DuelType,
    DuelQualifier,
    Ground,
    Score,
    Provider,
    Metadata,
    Player,
    Position,
    InterceptionResult,
    FormationType,
    CardType,
    CardQualifier,
    Qualifier,
    SetPieceQualifier,
    SetPieceType,
    BodyPartQualifier,
    BodyPart,
    PassType,
    PassQualifier,
    GoalkeeperQualifier,
    GoalkeeperActionType,
    CounterAttackQualifier,
)
from kloppy.exceptions import DeserializationError
from kloppy.infra.serializers.event.deserializer import EventDataDeserializer
from kloppy.utils import performance_logging

logger = logging.getLogger(__name__)

EVENT_TYPE_START_PERIOD = "START_PHASE"
EVENT_TYPE_END_PERIOD = "END_PHASE"

EVENT_TYPE_SHOT_WIDE = "SHOT_WIDE"

PHASE_TO_PERIOD = {
    "FIRST_HALF": 1,
    "SECOND_HALF": 2,
}
BALL_OWNING_EVENTS = [EVENT_TYPE_SHOT_WIDE]



def _parse_player(player_elm, team, starting=True) -> Player:
    player = Player(
        player_id=player_elm["player"]["id"],
        name=player_elm["player"]["internationalName"],
        jersey_no=player_elm["jerseyNumber"],
        position=Position(
            position_id=player_elm["player"]["fieldPosition"],
            name=player_elm["player"]["fieldPosition"],
            coordinates=None,
        ),
        starting=starting,
        team=team,
    )
    return player


def _parse_team(team_elm, ground=Ground.HOME) -> Team:
    team = Team(
        team_id=str(team_elm["team"]["id"]),
        name=team_elm["team"]["internationalName"],
        ground=ground,
    )
    team.players = [
        _parse_player(player_elm, team, starting=True)
        for player_elm in team_elm["field"]
    ] + [
        _parse_player(player_elm, team, starting=False)
        for player_elm in team_elm["bench"]
    ]
    return team

def _create_periods(match_result_type: str) -> List[Period]:
    if match_result_type == "AfterExtraTime":
        num_periods = 4
    elif match_result_type == "PenaltyShootout":
        num_periods = 5
    else:
        num_periods = 2

    periods = [
        Period(
            id=period_id,
            start_timestamp=None,
            end_timestamp=None,
        )
        for period_id in range(1, num_periods + 1)
    ]

    return periods


def _parse_pass(raw_qualifiers: Dict[int, str], outcome: int) -> Dict:
    if outcome:
        result = PassResult.COMPLETE
    else:
        result = PassResult.INCOMPLETE
    receiver_coordinates = _get_end_coordinates(raw_qualifiers)
    pass_qualifiers = _get_pass_qualifiers(raw_qualifiers)
    overall_qualifiers = _get_event_qualifiers(raw_qualifiers)

    qualifiers = pass_qualifiers + overall_qualifiers

    return dict(
        result=result,
        receiver_coordinates=receiver_coordinates,
        receiver_player=None,
        receive_timestamp=None,
        qualifiers=qualifiers,
    )


class UefaInputs(NamedTuple):
    event_data: IO[bytes]
    lineup_data: IO[bytes]


class UefaDeserializer(EventDataDeserializer[UefaInputs]):
    @property
    def provider(self) -> Provider:
        return Provider.UEFA

    def deserialize(self, inputs: UefaInputs) -> EventDataset:
        transformer = self.get_transformer(length=105, width=68)

        with performance_logging("load data", logger=logger):
            event_data = json.load(inputs.event_data)
            lineup_data = json.load(inputs.lineup_data)

        with performance_logging("parse data", logger=logger):
            # Parse teams
            home_team = _parse_team(lineup_data["homeTeam"], Ground.HOME)
            away_team = _parse_team(lineup_data["awayTeam"], Ground.AWAY)
            teams = [home_team, away_team]

            if len(home_team.players) == 0 or len(away_team.players) == 0:
                raise DeserializationError("LineUp incomplete")

            events = []
            periods = []
            for idx, event_elm in enumerate(event_data[::-1]):
                event_id = event_elm["id"]
                phase = event_elm["phase"]
                type_id = event_elm["type"]
                try:
                    timestamp = datetime.fromisoformat(event_elm["timestamp"])
                except KeyError:
                    logger.warning(f"Event {event_id} has no timestamp")
                    continue

                if type_id == EVENT_TYPE_START_PERIOD:
                    logger.debug(
                        f"Set start of period {phase} to {timestamp}"
                    )
                    periods.append(Period(id=PHASE_TO_PERIOD[phase], start_timestamp=timestamp, end_timestamp=None))
                elif type_id == EVENT_TYPE_END_PERIOD:
                    logger.debug(
                        f"Set end of period {phase} to {timestamp}"
                    )
                    periods[PHASE_TO_PERIOD[phase] - 1].end_timestamp = timestamp
                else:
                    period = periods[PHASE_TO_PERIOD[phase] - 1]
                    
                    team = None
                    player = None
                    if "primaryActor" in event_elm:
                        if "team" in event_elm["primaryActor"]:
                            if event_elm["primaryActor"]["team"]["id"] == home_team.team_id:
                                team = teams[0]
                            elif event_elm["primaryActor"]["team"]["id"] == away_team.team_id:
                                team = teams[1]
                            else:
                                raise DeserializationError(
                                    f"Unknown team_id {event_elm['primaryActor']['team']['id']}"
                                )
                            if "person" in event_elm["primaryActor"]:
                                player = team.get_player_by_id(
                                    event_elm["primaryActor"]["person"]["id"]
                                )
                    coordinates = None
                    if "fieldPosition" in event_elm:
                        coordinates = Point(
                            x=event_elm["fieldPosition"]["coordinate"]["x"],
                            y=event_elm["fieldPosition"]["coordinate"]["y"],
                        )

                    possession_team = None
                    if type_id in BALL_OWNING_EVENTS:
                        possession_team = team

                    generic_event_kwargs = dict(
                        # from DataRecord
                        period=period,
                        timestamp=timestamp - period.start_timestamp,
                        ball_owning_team=possession_team,
                        ball_state=BallState.ALIVE,
                        # from Event
                        event_id=event_id,
                        team=team,
                        player=player,
                        coordinates=coordinates,
                        raw_event=event_elm,
                    )

                    if type_id in (
                        EVENT_TYPE_SHOT_WIDE,
                    ):
                        event = self.event_factory.build_shot(
                            **generic_event_kwargs,
                            result=ShotResult.OFF_TARGET,
                            qualifiers=None,
                        )
                    else:
                        event = self.event_factory.build_generic(
                            **generic_event_kwargs,
                            result=None,
                            qualifiers=None,
                            event_name=type_id,
                        )

                    if self.should_include_event(event):
                        events.append(transformer.transform_event(event))

        metadata = Metadata(
            teams=teams,
            periods=periods,
            pitch_dimensions=transformer.get_to_coordinate_system().pitch_dimensions,
            score=None,
            frame_rate=None,
            orientation=Orientation.ACTION_EXECUTING_TEAM,
            flags=DatasetFlag.BALL_OWNING_TEAM | DatasetFlag.BALL_STATE,
            provider=Provider.UEFA,
            coordinate_system=transformer.get_to_coordinate_system(),
        )

        return EventDataset(
            metadata=metadata,
            records=events,
        )
