import pathlib
import tempfile

from orbis import Orbis
from tertius import EEmit, ESleep

from tests.shared import drain_to, root_value
from zahir import JobContext, evaluate, setup
from zahir.core.coeffects import FileExists, build_default_providers
from zahir.core.dependencies.file import (
    check_file_dependency,
    file_condition,
    file_dependency,
)


def test_file_dependency_requests_file_existence():
    """Proves the file dependency obtains file existence through a coeffect."""

    request = next(file_dependency("/tmp/zahir_no_such_file.json"))
    assert request == FileExists("/tmp/zahir_no_such_file.json")


def interpret_file_program(program):
    """Apply the worker's default contextual providers to a file program."""

    return Orbis(providers=build_default_providers())(program)


def run_file_job(ctx: JobContext, fpath: str):
    """Run one file dependency inside a worker."""

    yield from file_dependency(fpath)
    return "done"


def test_worker_provides_file_existence():
    """Proves each worker supplies the file-existence coeffect."""

    with tempfile.NamedTemporaryFile() as tmp:
        scope = {"run_file_job": run_file_job}
        events = evaluate(setup(n_workers=1), "run_file_job", (tmp.name,), scope)
        assert root_value(events) == "done"


def test_existing_file_emits_satisfied():
    """Proves a file that exists emits satisfied immediately."""

    with tempfile.NamedTemporaryFile() as tmp:
        emit = next(interpret_file_program(file_dependency(tmp.name)))
        assert isinstance(emit, EEmit)
        assert emit.body[0] == "satisfied"


def test_missing_file_emits_waiting_then_sleeps():
    """Proves a missing file emits a waiting event then sleeps before retrying."""

    gen = interpret_file_program(file_dependency("/tmp/zahir_no_such_file.json"))
    first = next(gen)
    assert isinstance(first, EEmit)
    second = next(gen)
    assert isinstance(second, ESleep)


def test_missing_file_check_emits_impossible():
    """Proves check_file_dependency returns impossible when the file does not exist."""

    program = check_file_dependency("/tmp/zahir_no_such_file.json")
    emit = next(interpret_file_program(program))
    assert isinstance(emit, EEmit)
    assert emit.body[0] == "impossible"


def test_existing_file_check_emits_satisfied():
    """Proves check_file_dependency returns satisfied when the file exists."""

    with tempfile.NamedTemporaryFile() as tmp:
        emit = next(interpret_file_program(check_file_dependency(tmp.name)))
        assert isinstance(emit, EEmit)
        assert emit.body[0] == "satisfied"


def test_satisfied_metadata_includes_path():
    """Proves the satisfied body contains the file path."""

    with tempfile.NamedTemporaryFile() as tmp:
        emit = next(interpret_file_program(file_dependency(tmp.name)))
        assert emit.body[1]["path"] == tmp.name


def test_satisfied_returns_tuple_as_generator_value():
    """Proves the generator returns the satisfied tuple as its StopIteration value."""

    with tempfile.NamedTemporaryFile() as tmp:
        program = interpret_file_program(file_dependency(tmp.name))
        emits, return_value = drain_to(program, EEmit)
    assert return_value is emits[0].body


def test_file_appears_after_check_satisfies_dependency():
    """Proves file_dependency becomes satisfied once the file is created."""

    with tempfile.TemporaryDirectory() as tmpdir:
        fpath = str(pathlib.Path(tmpdir) / "output.json")
        gen = interpret_file_program(file_dependency(fpath))
        next(gen)  # advance through one retry: EEmit(waiting)
        next(gen)  # advance through one retry: ESleep

        pathlib.Path(fpath).write_text("{}")

        emits, _ = drain_to(gen, EEmit)
        assert emits[0].body[0] == "satisfied"


def testfile_condition_returns_unsatisfied_for_missing_file():
    """Proves file_condition returns unsatisfied when file does not exist."""

    result = file_condition("/tmp/zahir_no_such_file.json")
    assert result[0] == "unsatisfied"


def testfile_condition_returns_satisfied_tuple_for_existing_file():
    """Proves file_condition returns a satisfied ConditionResult when file exists."""

    with tempfile.NamedTemporaryFile() as tmp:
        result = file_condition(tmp.name)
        assert result[0] == "satisfied"
        assert result[1]["path"] == tmp.name
