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
from . import options

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
def cli() -> None:
    """PPDB command line tools."""
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("alembic").setLevel(logging.WARNING)


@cli.command(short_help="Create new PPDB instance in SQL database.")
@click.argument("db-url")
@click.argument("config-path", type=click.Path(exists=False, dir_okay=False, writable=True))
@options.felis_path
@options.felis_schema
@options.schema_name
@options.no_connection_pool
@options.isolation_level
@options.connection_timeout
@options.drop
def create_sql(*args: Any, **kwargs: Any) -> None:
    """Create new PPDB database and generate its configuration file.

    DB_URL is database URL in SQLAlchemy format for PPDB instance. CONFIG_PATH
    is the name of the new configuration file for created PPDB instance.
    """
    scripts.create_sql(*args, **kwargs)
