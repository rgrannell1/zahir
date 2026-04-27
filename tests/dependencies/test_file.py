import tempfile
import pathlib

import pytest

from tertius import EEmit, ESleep

from zahir.core.dependencies.file import _file_condition, check_file_dependency, file_dependency


def test_existing_file_emits_satisfied():
    """Proves a file that exists emits satisfied immediately."""

    with tempfile.NamedTemporaryFile() as tmp:
        emit = next(file_dependency(tmp.name))
        assert isinstance(emit, EEmit)
        assert emit.body[0] == "satisfied"


def test_missing_file_emits_waiting_then_sleeps():
    """Proves a missing file emits a waiting event then sleeps before retrying."""

    gen = file_dependency("/tmp/zahir_no_such_file.json")
    first = next(gen)
    assert isinstance(first, EEmit)
    second = next(gen)
    assert isinstance(second, ESleep)


def test_missing_file_check_emits_impossible():
    """Proves check_file_dependency returns impossible when the file does not exist."""

    emit = next(check_file_dependency("/tmp/zahir_no_such_file.json"))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "impossible"


def test_existing_file_check_emits_satisfied():
    """Proves check_file_dependency returns satisfied when the file exists."""

    with tempfile.NamedTemporaryFile() as tmp:
        emit = next(check_file_dependency(tmp.name))
        assert isinstance(emit, EEmit)
        assert emit.body[0] == "satisfied"


def test_satisfied_metadata_includes_path():
    """Proves the satisfied body contains the file path."""

    with tempfile.NamedTemporaryFile() as tmp:
        emit = next(file_dependency(tmp.name))
        assert emit.body[1]["path"] == tmp.name


def test_satisfied_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple as its StopIteration value."""

    with tempfile.NamedTemporaryFile() as tmp:
        gen = file_dependency(tmp.name)
        emit = next(gen)           # EEmit(("satisfied", metadata))
        next(gen)                  # EEmit(_satisfied_event)
        with pytest.raises(StopIteration) as exc:
            next(gen)
        assert exc.value.value is emit.body


def test_file_appears_after_check_satisfies_dependency():
    """Proves file_dependency becomes satisfied once the file is created."""

    with tempfile.TemporaryDirectory() as tmpdir:
        fpath = str(pathlib.Path(tmpdir) / "output.json")
        gen = file_dependency(fpath)
        next(gen)  # EEmit(_waiting_event)
        next(gen)  # ESleep

        pathlib.Path(fpath).write_text("{}")

        emit = next(gen)
        assert emit.body[0] == "satisfied"


def test_file_condition_returns_false_for_missing_file():
    """Proves _file_condition returns False when file does not exist."""

    with pytest.raises(StopIteration) as exc:
        next(_file_condition("/tmp/zahir_no_such_file.json"))
    assert exc.value.value is False


def test_file_condition_returns_satisfied_tuple_for_existing_file():
    """Proves _file_condition returns (True, metadata) when file exists."""

    with tempfile.NamedTemporaryFile() as tmp:
        with pytest.raises(StopIteration) as exc:
            next(_file_condition(tmp.name))
        result = exc.value.value
        assert result[0] is True
        assert result[1]["path"] == tmp.name
