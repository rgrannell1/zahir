"""Integration test: user-supplied handlers get handler wrappers, like built-in handlers."""

from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, ClassVar, LiteralString

import pytest
from bookman.events import Event

from tests.shared import root_value
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


_SCOPE = {"shouting_root": shouting_root}


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


def test_evaluate_rejects_user_storage_handlers():
    """Proves evaluate refuses a user bag that binds storage-effect tags."""

    bag = {EStorageAcquire.tag: handle_shout}
    with pytest.raises(ZahirError):
        list(evaluate(setup(n_workers=1), "shouting_root", (), _SCOPE, handlers=bag))
