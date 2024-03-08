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

from __future__ import annotations

__all__ = ["cli"]

import logging
from typing import Any

import click

from .. import scripts

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
def cli() -> None:
    """Commands for managing APDB-to-PPDB replication."""
    logging.basicConfig(level=logging.INFO)


@cli.command(short_help="List full set of insert ids in APDB.")
@click.argument("apdb-config")
def list_ids_apdb(*args: Any, **kwargs: Any) -> None:
    """Print full list of insert IDs existing on APDB side.

    APDB_CONFIG is the path to the APDB configuration.
    """
    scripts.replication_list_ids_apdb(*args, **kwargs)


@cli.command(short_help="List full set of insert ids in PPDB.")
@click.argument("ppdb-config")
def list_ids_ppdb(*args: Any, **kwargs: Any) -> None:
    """Print full list of insert IDs existing on PPDB side.

    PPDB_CONFIG is the path to the PPDB configuration.
    """
    scripts.replication_list_ids_ppdb(*args, **kwargs)
