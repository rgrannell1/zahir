from tertius import Pid

from zahir.core.evaluate.suspension import RunningJob

OVERSEER = Pid(node_id=0, id=1)
ME = Pid(node_id=0, id=2)


def mock_mcall(return_value):
    """Canned reply for mcall-style generators; extra kwargs (timeout_ms) are ignored."""

    def _gen(pid, body, **_kwargs):
        return return_value
        yield

    return _gen


# mcall_timeout shares the shape — timeout_ms arrives as an ignored kwarg
mock_mcall_timeout = mock_mcall


def mock_mcast():
    def _gen(pid, body):
        return None
        yield

    return _gen


def make_deadlined_parent(deadline: float) -> RunningJob:
    """Build a minimal parent job carrying a monotonic deadline."""

    return RunningJob(
        fn_name="parent",
        eval_gen=None,
        reply_to=None,
        parent_sequence_number=None,
        deadline=deadline,
    )
