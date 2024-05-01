import json
import logging
from datetime import datetime
from typing import IO, Dict, List, NamedTuple, Tuple

import pytz
from kloppy.domain import (
    BallState,
    BodyPart,
    BodyPartQualifier,
    CardQualifier,
    CardType,
    DatasetFlag,
    EventDataset,
    FormationType,
    Ground,
    Metadata,
    Orientation,
    PassQualifier,
    PassResult,
    PassType,
    Period,
    Player,
    Point,
    Position,
    Provider,
    Qualifier,
    Score,
    SetPieceQualifier,
    SetPieceType,
    ShotResult,
    TakeOnResult,
    Team,
)
from kloppy.exceptions import DeserializationError
from kloppy.infra.serializers.event.deserializer import EventDataDeserializer
from kloppy.utils import performance_logging
from lxml import objectify


logger = logging.getLogger(__name__)

EVENT_TYPE_START_PERIOD = 32
EVENT_TYPE_END_PERIOD = 30

EVENT_TYPE_PASS = 1
EVENT_TYPE_OFFSIDE_PASS = 2
EVENT_TYPE_TAKE_ON = 3
EVENT_TYPE_SHOT_MISS = 13
EVENT_TYPE_SHOT_POST = 14
EVENT_TYPE_SHOT_SAVED = 15
EVENT_TYPE_SHOT_GOAL = 16
EVENT_TYPE_BALL_OUT = 5
EVENT_TYPE_CORNER_AWARDED = 6
EVENT_TYPE_FOUL_COMMITTED = 4
EVENT_TYPE_CARD = 17
EVENT_TYPE_RECOVERY = 49
EVENT_TYPE_FORMATION_CHANGE = 40

BALL_OUT_EVENTS = [EVENT_TYPE_BALL_OUT, EVENT_TYPE_CORNER_AWARDED]

BALL_OWNING_EVENTS = (
    EVENT_TYPE_PASS,
    EVENT_TYPE_OFFSIDE_PASS,
    EVENT_TYPE_TAKE_ON,
    EVENT_TYPE_SHOT_MISS,
    EVENT_TYPE_SHOT_POST,
    EVENT_TYPE_SHOT_SAVED,
    EVENT_TYPE_SHOT_GOAL,
    EVENT_TYPE_RECOVERY,
)

EVENT_QUALIFIER_GOAL_KICK = 124
EVENT_QUALIFIER_FREE_KICK = 5
EVENT_QUALIFIER_THROW_IN = 107
EVENT_QUALIFIER_CORNER_KICK = 6
EVENT_QUALIFIER_PENALTY = 9
EVENT_QUALIFIER_KICK_OFF = 279
EVENT_QUALIFIER_FREE_KICK_SHOT = 26

EVENT_QUALIFIER_HEAD_PASS = 3
EVENT_QUALIFIER_HEAD = 15
EVENT_QUALIFIER_LEFT_FOOT = 72
EVENT_QUALIFIER_RIGHT_FOOT = 20
EVENT_QUALIFIER_OTHER_BODYPART = 21

EVENT_QUALIFIER_LONG_BALL = 1
EVENT_QUALIFIER_CROSS = 2
EVENT_QUALIFIER_THROUGH_BALL = 4
EVENT_QUALIFIER_CHIPPED_BALL = 155
EVENT_QUALIFIER_LAUNCH = 157
EVENT_QUALIFIER_FLICK_ON = 168
EVENT_QUALIFIER_SWITCH_OF_PLAY = 196
EVENT_QUALIFIER_ASSIST = 210
EVENT_QUALIFIER_ASSIST_2ND = 218

EVENT_QUALIFIER_FIRST_YELLOW_CARD = 31
EVENT_QUALIFIER_SECOND_YELLOW_CARD = 32
EVENT_QUALIFIER_RED_CARD = 33

EVENT_QUALIFIER_TEAM_FORMATION = 130

event_type_names = {
    1: "pass",
    2: "offside pass",
    3: "take on",
    4: "foul",
    5: "out",
    6: "corner awarded",
    7: "tackle",
    8: "interception",
    9: "turnover",
    10: "save",
    11: "claim",
    12: "clearance",
    13: "miss",
    14: "post",
    15: "attempt saved",
    16: "goal",
    17: "card",
    18: "player off",
    19: "player on",
    20: "player retired",
    21: "player returns",
    22: "player becomes goalkeeper",
    23: "goalkeeper becomes player",
    24: "condition change",
    25: "official change",
    26: "unknown26",
    27: "start delay",
    28: "end delay",
    29: "unknown29",
    30: "end",
    31: "unknown31",
    32: "start",
    33: "unknown33",
    34: "team set up",
    35: "player changed position",
    36: "player changed jersey number",
    37: "collection end",
    38: "temp_goal",
    39: "temp_attempt",
    40: "formation change",
    41: "punch",
    42: "good skill",
    43: "deleted event",
    44: "aerial",
    45: "challenge",
    46: "unknown46",
    47: "rescinded card",
    48: "unknown46",
    49: "ball recovery",
    50: "dispossessed",
    51: "error",
    52: "keeper pick-up",
    53: "cross not claimed",
    54: "smother",
    55: "offside provoked",
    56: "shield ball opp",
    57: "foul throw in",
    58: "penalty faced",
    59: "keeper sweeper",
    60: "chance missed",
    61: "ball touch",
    62: "unknown62",
    63: "temp_save",
    64: "resume",
    65: "contentious referee decision",
    66: "possession data",
    67: "50/50",
    68: "referee drop ball",
    69: "failed to block",
    70: "injury time announcement",
    71: "coach setup",
    72: "caught offside",
    73: "other ball contact",
    74: "blocked pass",
    75: "delayed start",
    76: "early end",
    77: "player off pitch",
}

formations_num = {
    2: FormationType.FOUR_FOUR_TWO,
    3: FormationType.FOUR_ONE_TWO_ONE_TWO,
    4: FormationType.FOUR_THREE_THREE,
    5: FormationType.FOUR_FIVE_ONE,
    6: FormationType.FOUR_FOUR_ONE_ONE,
    7: FormationType.FOUR_ONE_FOUR_ONE,
    8: FormationType.FOUR_TWO_THREE_ONE,
    9: FormationType.FOUR_THREE_TWO_ONE,
    10: FormationType.FIVE_THREE_TWO,
    11: FormationType.FIVE_FOUR_ONE,
    12: FormationType.THREE_FIVE_TWO,
    13: FormationType.THREE_FOUR_THREE,
    14: FormationType.THREE_ONE_THREE_ONE_TWO,
    15: FormationType.FOUR_TWO_TWO_TWO,
    16: FormationType.THREE_FIVE_ONE_ONE,
    17: FormationType.THREE_FOUR_TWO_ONE,
    18: FormationType.THREE_FOUR_ONE_TWO,
    19: FormationType.THREE_ONE_FOUR_TWO,
    20: FormationType.THREE_ONE_TWO_ONE_THREE,
    21: FormationType.FOUR_ONE_THREE_TWO,
    22: FormationType.FOUR_TWO_FOUR_ZERO,
    23: FormationType.FOUR_THREE_ONE_TWO,
    24: FormationType.THREE_TWO_FOUR_ONE,
    25: FormationType.THREE_THREE_THREE_ONE,
}

formations_str = {
    "442": FormationType.FOUR_FOUR_TWO,
    "41212": FormationType.FOUR_ONE_TWO_ONE_TWO,
    "433": FormationType.FOUR_THREE_THREE,
    "451": FormationType.FOUR_FIVE_ONE,
    "4411": FormationType.FOUR_FOUR_ONE_ONE,
    "4141": FormationType.FOUR_ONE_FOUR_ONE,
    "4231": FormationType.FOUR_TWO_THREE_ONE,
    "4321": FormationType.FOUR_THREE_TWO_ONE,
    "532": FormationType.FIVE_THREE_TWO,
    "541": FormationType.FIVE_FOUR_ONE,
    "352": FormationType.THREE_FIVE_TWO,
    "343": FormationType.THREE_FOUR_THREE,
    "31312": FormationType.THREE_ONE_THREE_ONE_TWO,
    "4222": FormationType.FOUR_TWO_TWO_TWO,
    "3511": FormationType.THREE_FIVE_ONE_ONE,
    "3421": FormationType.THREE_FOUR_TWO_ONE,
    "3412": FormationType.THREE_FOUR_ONE_TWO,
    "3142": FormationType.THREE_ONE_FOUR_TWO,
    "31213": FormationType.THREE_ONE_TWO_ONE_THREE,
    "4132": FormationType.FOUR_ONE_THREE_TWO,
    "4240": FormationType.FOUR_TWO_FOUR_ZERO,
    "4312": FormationType.FOUR_THREE_ONE_TWO,
    "3241": FormationType.THREE_TWO_FOUR_ONE,
    "3331": FormationType.THREE_THREE_THREE_ONE,
}


def try_parsing_date(dt_str: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(dt_str, fmt).replace(tzinfo=pytz.utc)
        except ValueError:
            pass
    raise ValueError('no valid date format found')


def _parse_pass(raw_qualifiers: Dict[int, str], outcome: int) -> Dict:
    if outcome:
        receiver_coordinates = Point(x=float(raw_qualifiers[140]), y=float(raw_qualifiers[141]))
        result = PassResult.COMPLETE
    else:
        result = PassResult.INCOMPLETE
        # receiver_coordinates = None
        receiver_coordinates = Point(x=float(raw_qualifiers[140]), y=float(raw_qualifiers[141]))

    qualifiers = _get_event_qualifiers(raw_qualifiers)

    return dict(
        result=result,
        receiver_coordinates=receiver_coordinates,
        receiver_player=None,
        receive_timestamp=None,
        qualifiers=qualifiers,
    )


def _parse_offside_pass(raw_qualifiers: List) -> Dict:
    qualifiers = _get_event_qualifiers(raw_qualifiers)
    return dict(
        result=PassResult.OFFSIDE,
        receiver_coordinates=Point(x=float(raw_qualifiers[140]), y=float(raw_qualifiers[141])),
        receiver_player=None,
        receive_timestamp=None,
        qualifiers=qualifiers,
    )


def _parse_take_on(outcome: int) -> Dict:
    if outcome:
        result = TakeOnResult.COMPLETE
    else:
        result = TakeOnResult.INCOMPLETE
    return dict(result=result)


def _parse_card(raw_qualifiers: List) -> Dict:
    qualifiers = _get_event_qualifiers(raw_qualifiers)

    if EVENT_QUALIFIER_RED_CARD in qualifiers:
        card_type = CardType.RED
    elif EVENT_QUALIFIER_FIRST_YELLOW_CARD in qualifiers:
        card_type = CardType.FIRST_YELLOW
    elif EVENT_QUALIFIER_SECOND_YELLOW_CARD in qualifiers:
        card_type = CardType.SECOND_YELLOW
    else:
        card_type = None

    return dict(result=None, qualifiers=qualifiers, card_type=card_type)


def _parse_formation_change(raw_qualifiers: List) -> Dict:
    formation_id = int(raw_qualifiers[EVENT_QUALIFIER_TEAM_FORMATION])
    formation = formations_num[formation_id]

    return dict(formation_type=formation)


def _parse_shot(raw_qualifiers: Dict[int, str], type_id: int, coordinates: Point) -> Dict:
    if type_id == EVENT_TYPE_SHOT_GOAL:
        if 28 in raw_qualifiers:
            coordinates = Point(x=100 - coordinates.x, y=100 - coordinates.y)
            result = ShotResult.OWN_GOAL
            # ball_owning_team =
            # timestamp =
        else:
            result = ShotResult.GOAL
    else:
        result = None

    qualifiers = _get_event_qualifiers(raw_qualifiers)

    return dict(coordinates=coordinates, result=result, qualifiers=qualifiers)


def _parse_team_players(f7_root, team_ref: str) -> Tuple[str, Dict[str, Dict[str, str]]]:
    matchdata_path = objectify.ObjectPath("SoccerFeed.SoccerDocument")
    team_elms = list(matchdata_path.find(f7_root).iterchildren("Team"))
    for team_elm in team_elms:
        if team_elm.attrib["uID"] == team_ref:
            team_name = str(team_elm.find("Name"))
            players = {
                player_elm.attrib["uID"]: dict(
                    first_name=str(player_elm.find("PersonName").find("First")),
                    last_name=str(player_elm.find("PersonName").find("Last")),
                )
                for player_elm in team_elm.iterchildren("Player")
            }
            break
    else:
        raise DeserializationError(f"Could not parse players for {team_ref}")

    return team_name, players


def _parse_team(team_data, lineup_data) -> Team:
    # This should not happen here
    team = Team(
        team_id=str(team_data["id"]),
        name=str(team_data["name"]),
        ground=Ground.HOME if team_data["position"] == "home" else Ground.AWAY,
        starting_formation=formations_str[lineup_data["formationUsed"]],
    )
    team.players = []
    for player_data in lineup_data["player"]:
        player = Player(
            player_id=player_data["playerId"],
            team=team,
            jersey_no=int(player_data["shirtNumber"]),
            first_name=player_data["firstName"],
            last_name=player_data["lastName"],
            starting=True if player_data["position"] != "Substitute" else False,
            position=Position(
                position_id=player_data["formationPlace"]
                if player_data["position"] != "Substitute"
                else None,
                name=player_data["position"]
                if player_data["position"] != "Substitute"
                else player_data["subPosition"],
                coordinates=None,
            ),
        )
        team.players.append(player)

    return team


def _get_event_qualifiers(raw_qualifiers: List) -> List[Qualifier]:
    qualifiers = []
    qualifiers.extend(_get_event_setpiece_qualifiers(raw_qualifiers))
    qualifiers.extend(_get_event_bodypart_qualifiers(raw_qualifiers))
    qualifiers.extend(_get_event_pass_qualifiers(raw_qualifiers))
    qualifiers.extend(_get_event_card_qualifiers(raw_qualifiers))
    return qualifiers


def _get_event_pass_qualifiers(raw_qualifiers: List) -> List[Qualifier]:
    qualifiers = []
    if EVENT_QUALIFIER_CROSS in raw_qualifiers:
        qualifiers.append(PassQualifier(value=PassType.CROSS))
    elif EVENT_QUALIFIER_LONG_BALL in raw_qualifiers:
        qualifiers.append(PassQualifier(value=PassType.LONG_BALL))
    elif EVENT_QUALIFIER_CHIPPED_BALL in raw_qualifiers:
        qualifiers.append(PassQualifier(value=PassType.CHIPPED_PASS))
    elif EVENT_QUALIFIER_THROUGH_BALL in raw_qualifiers:
        qualifiers.append(PassQualifier(value=PassType.THROUGH_BALL))
    elif EVENT_QUALIFIER_LAUNCH in raw_qualifiers:
        qualifiers.append(PassQualifier(value=PassType.LAUNCH))
    elif EVENT_QUALIFIER_FLICK_ON in raw_qualifiers:
        qualifiers.append(PassQualifier(value=PassType.FLICK_ON))
    elif EVENT_QUALIFIER_ASSIST in raw_qualifiers:
        qualifiers.append(PassQualifier(value=PassType.ASSIST))
    elif EVENT_QUALIFIER_ASSIST_2ND in raw_qualifiers:
        qualifiers.append(PassQualifier(value=PassType.ASSIST_2ND))
    return qualifiers


def _get_event_setpiece_qualifiers(raw_qualifiers: List) -> List[Qualifier]:
    qualifiers = []
    if EVENT_QUALIFIER_CORNER_KICK in raw_qualifiers:
        qualifiers.append(SetPieceQualifier(value=SetPieceType.CORNER_KICK))
    elif (
        EVENT_QUALIFIER_FREE_KICK in raw_qualifiers
        or EVENT_QUALIFIER_FREE_KICK_SHOT in raw_qualifiers
    ):
        qualifiers.append(SetPieceQualifier(value=SetPieceType.FREE_KICK))
    elif EVENT_QUALIFIER_PENALTY in raw_qualifiers:
        qualifiers.append(SetPieceQualifier(value=SetPieceType.PENALTY))
    elif EVENT_QUALIFIER_THROW_IN in raw_qualifiers:
        qualifiers.append(SetPieceQualifier(value=SetPieceType.THROW_IN))
    elif EVENT_QUALIFIER_KICK_OFF in raw_qualifiers:
        qualifiers.append(SetPieceQualifier(value=SetPieceType.KICK_OFF))
    elif EVENT_QUALIFIER_GOAL_KICK in raw_qualifiers:
        qualifiers.append(SetPieceQualifier(value=SetPieceType.GOAL_KICK))
    return qualifiers


def _get_event_bodypart_qualifiers(raw_qualifiers: List) -> List[Qualifier]:
    qualifiers = []
    if EVENT_QUALIFIER_HEAD_PASS in raw_qualifiers:
        qualifiers.append(BodyPartQualifier(value=BodyPart.HEAD))
    elif EVENT_QUALIFIER_HEAD in raw_qualifiers:
        qualifiers.append(BodyPartQualifier(value=BodyPart.HEAD))
    elif EVENT_QUALIFIER_LEFT_FOOT in raw_qualifiers:
        qualifiers.append(BodyPartQualifier(value=BodyPart.LEFT_FOOT))
    elif EVENT_QUALIFIER_RIGHT_FOOT in raw_qualifiers:
        qualifiers.append(BodyPartQualifier(value=BodyPart.RIGHT_FOOT))
    elif EVENT_QUALIFIER_OTHER_BODYPART in raw_qualifiers:
        qualifiers.append(BodyPartQualifier(value=BodyPart.OTHER))
    return qualifiers


def _get_event_card_qualifiers(raw_qualifiers: List) -> List[Qualifier]:
    qualifiers = []
    if EVENT_QUALIFIER_RED_CARD in raw_qualifiers:
        qualifiers.append(CardQualifier(value=CardType.RED))
    elif EVENT_QUALIFIER_FIRST_YELLOW_CARD in raw_qualifiers:
        qualifiers.append(CardQualifier(value=CardType.FIRST_YELLOW))
    elif EVENT_QUALIFIER_SECOND_YELLOW_CARD in raw_qualifiers:
        qualifiers.append(CardQualifier(value=CardType.SECOND_YELLOW))

    return qualifiers


def _get_event_type_name(type_id: int) -> str:
    return event_type_names.get(type_id, "unknown")


class StatsPerformInputs(NamedTuple):
    ma1_data: IO[bytes]
    ma3_data: IO[bytes]


class StatsPerformDeserializer(EventDataDeserializer[StatsPerformInputs]):
    @property
    def provider(self) -> Provider:
        return Provider.OPTA

    def deserialize(self, inputs: StatsPerformInputs) -> EventDataset:  # noqa: C901
        transformer = self.get_transformer(length=105, width=68)

        with performance_logging("load data", logger=logger):
            metadata = json.load(inputs.ma1_data)
            eventdata = json.load(inputs.ma3_data)

        with performance_logging("parse data", logger=logger):
            home_score = None
            away_score = None
            contestants = metadata['matchInfo']["contestant"]
            for idx, contestant in enumerate(contestants):
                if contestant["position"] == "home":
                    home_score = metadata["liveData"]["matchDetails"]["scores"]["ft"]["home"]
                    # TODO check iof order of contestans are same with lineup
                    home_team = _parse_team(contestant, metadata["liveData"]["lineUp"][idx])
                elif contestant["position"] == "away":
                    away_score = metadata["liveData"]["matchDetails"]["scores"]["ft"]["home"]
                    away_team = _parse_team(contestant, metadata["liveData"]["lineUp"][idx])
                else:
                    raise DeserializationError("Unknown side: " + str(contestant["position"]))
            score = Score(home=home_score, away=away_score)
            teams = [home_team, away_team]

            if len(home_team.players) == 0 or len(away_team.players) == 0:
                raise DeserializationError("LineUp incomplete")

            periods = []
            for per in metadata["liveData"]["matchDetails"]["period"]:
                periods.append(
                    Period(
                        id=per["id"],
                        start_timestamp=try_parsing_date(per["start"]),
                        end_timestamp=try_parsing_date(per["end"]),
                    )
                )
            possession_team = None
            events = []
            for event_data in eventdata["liveData"]["event"]:
                event_id = event_data["id"]
                type_id = int(event_data["typeId"])
                # TODO: check if function is OK (different than non mili)
                timestamp = try_parsing_date(event_data["timeStamp"])
                period_id = int(event_data["periodId"])
                for period in periods:
                    if period.id == period_id:
                        break
                else:
                    logger.debug(
                        f"Skipping event {event_id} because period doesn't match {period_id}"
                    )
                    continue

                if type_id == EVENT_TYPE_START_PERIOD:
                    logger.debug(f"Set start of period {period.id} to {timestamp}")
                    period.start_timestamp = timestamp
                elif type_id == EVENT_TYPE_END_PERIOD:
                    logger.debug(f"Set end of period {period.id} to {timestamp}")
                    period.end_timestamp = timestamp
                else:
                    if not period.start_timestamp:
                        # not started yet
                        continue

                    if event_data["contestantId"] == home_team.team_id:
                        team = teams[0]
                    elif event_data["contestantId"] == away_team.team_id:
                        team = teams[1]
                    else:
                        raise DeserializationError(f"Unknown team_id {event_data['team_id']}")

                    x = float(event_data["x"])
                    y = float(event_data["y"])
                    outcome = int(event_data["outcome"])
                    raw_qualifiers = {
                        int(qualifier_elm["qualifierId"]): qualifier_elm["value"]
                        if "value" in qualifier_elm.keys()
                        else None
                        for qualifier_elm in event_data["qualifier"]
                    }
                    player = None
                    if "playerId" in event_data.keys():
                        player = team.get_player_by_id(event_data["playerId"])

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
                        coordinates=Point(x=x, y=y),
                        raw_event=event_data,
                    )

                    if type_id == EVENT_TYPE_PASS:
                        pass_event_kwargs = _parse_pass(raw_qualifiers, outcome)
                        event = self.event_factory.build_pass(
                            **pass_event_kwargs,
                            **generic_event_kwargs,
                        )
                    elif type_id == EVENT_TYPE_OFFSIDE_PASS:
                        pass_event_kwargs = _parse_offside_pass(raw_qualifiers)
                        event = self.event_factory.build_pass(
                            **pass_event_kwargs,
                            **generic_event_kwargs,
                        )
                    elif type_id == EVENT_TYPE_TAKE_ON:
                        take_on_event_kwargs = _parse_take_on(outcome)
                        event = self.event_factory.build_take_on(
                            qualifiers=None,
                            **take_on_event_kwargs,
                            **generic_event_kwargs,
                        )
                    elif type_id in (
                        EVENT_TYPE_SHOT_MISS,
                        EVENT_TYPE_SHOT_POST,
                        EVENT_TYPE_SHOT_SAVED,
                        EVENT_TYPE_SHOT_GOAL,
                    ):
                        if type_id == EVENT_TYPE_SHOT_GOAL:
                            if 374 in raw_qualifiers.keys():
                                generic_event_kwargs["timestamp"] = (
                                    try_parsing_date(
                                        raw_qualifiers.get(374).replace(" ", "T") + "Z"
                                    )
                                    - period.start_timestamp
                                )
                        shot_event_kwargs = _parse_shot(
                            raw_qualifiers,
                            type_id,
                            coordinates=generic_event_kwargs["coordinates"],
                        )
                        kwargs = {}
                        kwargs.update(generic_event_kwargs)
                        kwargs.update(shot_event_kwargs)
                        event = self.event_factory.build_shot(**kwargs)

                    elif type_id == EVENT_TYPE_RECOVERY:
                        event = self.event_factory.build_recovery(
                            result=None,
                            qualifiers=None,
                            **generic_event_kwargs,
                        )

                    elif type_id == EVENT_TYPE_FOUL_COMMITTED:
                        event = self.event_factory.build_foul_committed(
                            result=None,
                            qualifiers=None,
                            **generic_event_kwargs,
                        )

                    elif type_id in BALL_OUT_EVENTS:
                        generic_event_kwargs["ball_state"] = BallState.DEAD
                        event = self.event_factory.build_ball_out(
                            result=None,
                            qualifiers=None,
                            **generic_event_kwargs,
                        )

                    elif type_id == EVENT_TYPE_FORMATION_CHANGE:
                        formation_change_event_kwargs = _parse_formation_change(raw_qualifiers)
                        event = self.event_factory.build_formation_change(
                            result=None,
                            qualifiers=None,
                            **formation_change_event_kwargs,
                            **generic_event_kwargs,
                        )

                    elif type_id == EVENT_TYPE_CARD:
                        generic_event_kwargs["ball_state"] = BallState.DEAD
                        card_event_kwargs = _parse_card(raw_qualifiers)

                        event = self.event_factory.build_card(
                            **card_event_kwargs,
                            **generic_event_kwargs,
                        )

                    else:
                        event = self.event_factory.build_generic(
                            **generic_event_kwargs,
                            result=None,
                            qualifiers=None,
                            event_name=_get_event_type_name(type_id),
                        )

                    if self.should_include_event(event):
                        events.append(transformer.transform_event(event))

        metadata = Metadata(
            teams=teams,
            periods=periods,
            pitch_dimensions=transformer.get_to_coordinate_system().pitch_dimensions,
            score=score,
            frame_rate=None,
            orientation=Orientation.ACTION_EXECUTING_TEAM,
            flags=DatasetFlag.BALL_OWNING_TEAM,
            provider=Provider.OTHER,
            coordinate_system=transformer.get_to_coordinate_system(),
        )

        return EventDataset(
            metadata=metadata,
            records=events,
        )

