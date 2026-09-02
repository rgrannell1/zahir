"""Integration test: user-supplied handlers get handler wrappers, like built-in handlers."""

from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar, LiteralString

import pytest
from bookman.events import Event
from orbis import Effect

from tests.shared import root_value
from zahir.core.coeffects import CurrentTime
from zahir.core.combinators import wrap
from zahir.core.effects import EStorageAcquire, ZahirJobEffect
from zahir.core.evaluate import JobContext, evaluate, setup
from zahir.core.exceptions import ZahirError
from zahir.core.telemetry import make_telemetry


@dataclass
class EShout(ZahirJobEffect[str]):
    """Test effect: uppercases a word."""

    tag: ClassVar[LiteralString] = "shout"
    word: str


def handle_shout(effect: EShout) -> Generator[Any, Any, str]:
    yield from ()
    return effect.word.upper()


def shouting_root(ctx: JobContext) -> Generator[Any, Any, str]:
    result = yield EShout(word="hi")
    return result


def clock_root(ctx: JobContext) -> Generator[Any, Any, Any]:
    return (yield CurrentTime())


_SCOPE = {"shouting_root": shouting_root}

# Fixed clock value used to prove caller-provider behaviour.
_FIXED_TIME = datetime(2020, 1, 1, tzinfo=UTC)


def provide_fixed_time(_coeffect: CurrentTime) -> datetime:
    return _FIXED_TIME


def require_effect(operation):
    assert isinstance(operation, Effect)
    yield


def test_user_handlers_receive_handler_wrappers():
    """Proves handler wrappers apply to user handlers, so user effects emit telemetry."""

    stream = list(
        evaluate(
            setup(n_workers=1),
            "shouting_root",
            (),
            _SCOPE,
            handler_wrappers=[make_telemetry()],
            handlers={EShout.tag: handle_shout},
        )
    )

    shout_events = [
        event for event in stream if isinstance(event, Event) and event.dim("tag") == "shout"
    ]
    assert shout_events, "no telemetry events for the user effect tag"
    assert root_value(stream) == "HI"


def test_caller_providers_receive_provider_wrappers():
    """Proves provider wrappers apply to caller-supplied coeffect providers."""

    stream = list(
        evaluate(
            setup(n_workers=1),
            "clock_root",
            (),
            {"clock_root": clock_root},
            provider_wrappers=[make_telemetry()],
            providers={CurrentTime.tag: provide_fixed_time},
        )
    )

    clock_events = [
        event
        for event in stream
        if isinstance(event, Event)
        and event.dim("tag") == CurrentTime.tag
        and event.dim("operation_kind") == "coeffect"
    ]
    assert clock_events, "no telemetry events for the coeffect tag"
    assert {event.dim("phase") for event in clock_events} == {"start", "end"}
    assert root_value(stream) == _FIXED_TIME


def test_handler_wrappers_do_not_wrap_providers():
    """Proves effect-specific handler wrappers never receive coeffects."""

    stream = evaluate(
        setup(n_workers=1),
        "clock_root",
        (),
        {"clock_root": clock_root},
        handler_wrappers=[wrap(require_effect)],
    )

    assert isinstance(root_value(list(stream)), datetime)


def test_evaluate_rejects_user_storage_handlers():
    """Proves evaluate refuses a user bag that binds storage-effect tags."""

    bag = {EStorageAcquire.tag: handle_shout}
    with pytest.raises(ZahirError):
        list(evaluate(setup(n_workers=1), "shouting_root", (), _SCOPE, handlers=bag))
