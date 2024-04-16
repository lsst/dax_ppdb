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

__all__ = ["main"]

import argparse

from lsst.dax.apdb.cli.logging_cli import LoggingCli

from .. import scripts
from . import options


def main() -> None:
    """PPDB command line tools."""
    parser = argparse.ArgumentParser(description="PPDB command line tools")
    log_cli = LoggingCli(parser)

    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _create_sql_subcommand(subparsers)

    args = parser.parse_args()
    log_cli.process_args(args)

    kwargs = vars(args)
    method = kwargs.pop("method")
    method(**kwargs)


def _create_sql_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("create-sql", help="Create new PPDB instance in SQL database.")
    parser.add_argument("db_url", help="Database URL in SQLAlchemy format for PPDB instance.")
    parser.add_argument("config_path", help="Name of the new configuration file for created PPDB instance.")
    options.felis_schema_options(parser)
    options.sql_db_options(parser)
    parser.add_argument(
        "--drop", help="If True then drop existing tables.", default=False, action="store_true"
    )
    parser.set_defaults(method=scripts.create_sql)
