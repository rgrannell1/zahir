from tertius import Pid

OVERSEER = Pid(id=1)
ME = Pid(id=2)


def mock_mcall(return_value):
    def _gen(pid, body):
        return return_value
        yield

    return _gen


def mock_mcast():
    def _gen(pid, body):
        return None
        yield

    return _gen
