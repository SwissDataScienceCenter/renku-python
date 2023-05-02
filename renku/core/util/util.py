# Copyright Swiss Data Science Center (SDSC). A partnership between
# École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""General utility functions."""

import concurrent.futures
import os
import uuid
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple, Union

import deal
from packaging.version import Version


def to_string(value: Optional[Any], strip: bool = False) -> str:
    """Return a string representation of value and return an empty string for None."""
    return "" if value is None else str(value).strip() if strip else str(value)


def to_semantic_version(value: str) -> Optional[Version]:
    """Convert value to SemVer."""
    try:
        return Version(value)
    except ValueError:
        return None


def is_uuid(value):
    """Check if value is UUID4.

    Copied from https://stackoverflow.com/questions/19989481/
    """
    try:
        uuid_obj = uuid.UUID(value, version=4)
    except ValueError:
        return False

    return str(uuid_obj) == value


@deal.pre(lambda _: _.rate > 0, message="Rate must be positive")
def parallel_execute(
    function: Callable[..., List[Any]], *data: Union[Tuple[Any, ...], List[Any]], rate: float = 1, **kwargs
) -> List[Any]:
    """Execute the function using multiple threads.

    Args:
        function(Callable[..., Any]): Function to parallelize. Must accept at least one parameter and returns a list.
        data(Union[Tuple[Any], List[Any]]): List of data where each of its elements is passed to a function's execution.
        rate(float): Number of executions per thread per second.

    Returns:
        List[Any]: A list of return results of all executions.

    """
    from renku.core.util import communication
    from renku.core.util.contexts import wait_for
    from renku.domain_model.project_context import project_context

    listeners = communication.get_listeners()

    def subscribe_communication_listeners(delay: float, path: Path, function, *data, **kwargs):
        try:
            for communicator in listeners:
                communication.subscribe(communicator)
            if not project_context.has_context(path):
                project_context.push_path(path)
            with wait_for(delay):
                return function(*data, **kwargs)
        finally:
            for communicator in listeners:
                communication.unsubscribe(communicator)

    # NOTE: Disable parallelization during tests for easier debugging
    if is_test_session_running():
        max_workers = 1
        delay = 0.0
    else:
        n_cpus = os.cpu_count() or 1
        max_workers = min(n_cpus + 4, 8)
        delay = max_workers / rate if len(data[0]) > max_workers else 0

    files = []
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        futures = {
            executor.submit(subscribe_communication_listeners, delay, project_context.path, function, *d, **kwargs)
            for d in zip(*data)
        }

        for future in concurrent.futures.as_completed(futures):
            files.extend(future.result())

    return files


def is_test_session_running() -> bool:
    """Return if the code is being executed in a test and not called by user."""
    return "RENKU_RUNNING_UNDER_TEST" in os.environ
