# This file is part of dax_ppdb.
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

"""Lookup of the Google Cloud project to use."""

__all__ = ["get_project_id"]

import os

import google.auth
from google.auth.exceptions import GoogleAuthError


def get_project_id() -> str:
    """Return the Google Cloud project to use.

    Returns
    -------
    `str`
        The ``PROJECT_ID`` environment variable if it's set, otherwise the
        project of the default credentials.

    Raises
    ------
    RuntimeError
        Raised if the default credentials can't be found, or if neither
        gives a project.
    """
    project_id = os.environ.get("PROJECT_ID")
    if project_id:
        return project_id
    try:
        _, project_id = google.auth.default()
    except GoogleAuthError as e:
        raise RuntimeError("Failed to get the default Google Cloud credentials") from e
    if project_id is None:
        raise RuntimeError(
            "Google Cloud project ID could not be determined from PROJECT_ID or the default credentials"
        )
    return project_id
