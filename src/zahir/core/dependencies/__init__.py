from zahir.core.dependencies.dependency import check, dependency
from zahir.core.dependencies.resources import check_resource_dependency
from zahir.core.dependencies.semaphore import check_semaphore_dependency
from zahir.core.dependencies.sqlite import check_sqlite_dependency
from zahir.core.dependencies.time import check_time_dependency
from zahir.core.zahir_types import DependencyResult, Impossible, Satisfied
from zahir.core.exceptions import ImpossibleError
