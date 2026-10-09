# This file is part of dax_ppdb
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import sentry_sdk

P = ParamSpec("P")
R = TypeVar("R")

__all__ = ["cloud_run_initialize_sentry", "flush_sentry"]


def cloud_run_initialize_sentry() -> None:
    """Initialize Sentry and add tags for Cloud Run workloads."""
    _ = sentry_sdk.init()

    # These are all of the possible env vars that we would want in functions,
    # services, and jobs. They won't all be available in each type of workload,
    # so only set them as tags if they're available.
    cloud_run_job = os.environ.get("CLOUD_RUN_JOB")
    k_revision = os.environ.get("K_REVISION")
    k_service = os.environ.get("K_SERVICE")
    google_cloud_project = os.environ.get("PROJECT_ID")

    # The link to Google Cloud Logs for the workload is different depending on
    # whether it is a function, service, or job. We can tell which of these it
    # is based on which env vars have been set.
    logs_link = "unknown"
    if cloud_run_job:
        logs_link = (
            "https://console.cloud.google.com/logs/query;query="
            "resource.type%3D%22cloud_run_job%22"
            f"%0Aresource.labels.job_name%3D%22{cloud_run_job}%22"
            f"?project={google_cloud_project}"
        )
    elif k_service:
        logs_link = (
            "https://console.cloud.google.com/logs/query;query="
            "resource.type%3D%22cloud_run_revision%22"
            f"%0Aresource.labels.service_name%3D%22{k_service}%22"
            f"?project={google_cloud_project}"
        )

    tags = {
        "cloud_run.configuration": os.environ.get("K_CONFIGURATION"),
        "cloud_run.execution": os.environ.get("CLOUD_RUN_EXECUTION"),
        "cloud_run.function_target": os.environ.get("FUNCTION_TARGET"),
        "cloud_run.job": cloud_run_job,
        "cloud_run.revision": k_revision,
        "cloud_run.service": k_service,
        "cloud_run.task_attempt": os.environ.get("CLOUD_RUN_TASK_ATTEMPT"),
        "cloud_run.task_index": os.environ.get("CLOUD_RUN_TASK_INDEX"),
        "cloud_run.logs_link": logs_link,
    }

    for key, value in tags.items():
        if value is not None:
            sentry_sdk.set_tag(key, value)


# Using type parameters here breaks the docstyle check for this function and
# there is nothing we can do to fix it. But not using type parameters triggers
# a ruff lint. Given that we can ignore the ruff lint, but we can't do anything
# about the docstlye bug, let's not use type parameters and let's ignore the
# ruff lint.
def flush_sentry(func: Callable[P, R]) -> Callable[P, R]:  # noqa: UP047
    """Explicitly flush sentry after function completion.

    Sentry sends captured events in a background thread. These events are
    flushed upon normal exit of the Python interpreter, but Cloud Run does not
    guarantee a normal exit of the python interpreter after a function returns,
    so we have to explicitly flush all captured events before we return.

    Parameters
    ----------
    func
        The function to flush sentry messages after it finishes executing

    Returns
    -------
    `Callable`
        A function that is wrapped to flush Sentry messages after execution
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        """Explicily flush Sentry events after execution."""
        try:
            return func(*args, **kwargs)
        except Exception:
            _ = sentry_sdk.capture_exception()
            raise
        finally:
            sentry_sdk.flush(timeout=5)

    return wrapper
