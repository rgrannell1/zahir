from collections import deque
from collections.abc import Generator
from typing import Any

from tertius import GenServer

from evaluate.overseer_handlers import CALL_HANDLERS, CAST_HANDLERS, _dispatch
from zahir_types import JobSpec, OverseerState


class Overseer(GenServer[OverseerState]):
    def init(self, initial_fn: str, initial_args: tuple) -> OverseerState:  # type: ignore[override]
        job = JobSpec(fn_name=initial_fn, args=initial_args, reply_to=None)
        return OverseerState(
            queue=deque([job]),
            concurrency={},
            semaphores={},
            pending=1,
        )

    def handle_call(self, state: OverseerState, body: Any) -> tuple[OverseerState, Any]:
        """Dispatch a req-res call to the appropriate handler based on the body."""
        return _dispatch(CALL_HANDLERS, state, body)

    def handle_cast(self, state: OverseerState, body: Any) -> OverseerState:
        """Dispatch a fire-forget cast to the appropriate handler based on the body."""
        return _dispatch(CAST_HANDLERS, state, body)


def run_overseer(initial_fn: str, initial_args: tuple) -> Generator[Any, Any, None]:
    """Run the overseer with the given initial function and arguments."""
    yield from Overseer().loop(initial_fn, initial_args)
