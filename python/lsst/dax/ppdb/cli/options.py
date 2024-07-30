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

__all__ = ["felis_schema_options", "sql_db_options", "replication_options"]

import argparse


def felis_schema_options(parser: argparse.ArgumentParser) -> None:
    """Define CLI options for Felis schema file."""
    group = parser.add_argument_group("felis schema options")
    group.add_argument(
        "--felis-path",
        help="YAML file with PPDB felis schema (can be same as APDB schema).",
        metavar="PATH",
        default=None,
    )
    group.add_argument(
        "--felis-schema",
        help="Schema name used in felis YAML file.",
        metavar="NAME",
        default=None,
    )


def sql_db_options(parser: argparse.ArgumentParser) -> None:
    """Define CLI options for database connection."""
    group = parser.add_argument_group("database options")
    group.add_argument(
        "-s",
        "--schema",
        help="Optional schema name.",
        metavar="DB_SCHEMA",
        default=None,
    )

    group.add_argument(
        "--connection-pool",
        help="Enable/disable use of connection pool.",
        action=argparse.BooleanOptionalAction,
        default=True,
    )

    group.add_argument(
        "--isolation-level",
        help="Transaction isolation level, allowed values: %(choices)s",
        metavar="STRING",
        choices=["READ_COMMITTED", "READ_UNCOMMITTED", "REPEATABLE_READ", "SERIALIZABLE"],
        default=None,
    )

    group.add_argument(
        "--connection-timeout",
        type=float,
        help="Maximum connection timeout in seconds.",
        metavar="SECONDS",
        default=None,
    )


def replication_options(parser: argparse.ArgumentParser) -> None:
    """Define CLI options for replication."""
    group = parser.add_argument_group("replication options")
    group.add_argument(
        "--single", help="Copy single replication item and stop.", default=False, action="store_true"
    )
    group.add_argument(
        "--update", help="Allow updates to already replicated data.", default=False, action="store_true"
    )
    group.add_argument(
        "--min-wait-time",
        type=int,
        default=300,
        metavar="SECONDS",
        help="Minimum time to wait for replicating a chunk after a next chunk appears, default: %(default)s.",
    )
    group.add_argument(
        "--max-wait-time",
        type=int,
        default=900,
        metavar="SECONDS",
        help="Maximum time to wait for replicating a chunk if no chunk appears, default: %(default)s.",
    )
    group.add_argument(
        "--check-interval",
        type=int,
        default=360,
        metavar="SECONDS",
        help="Time to wait before next check if there was no replicated chunks, default: %(default)s.",
    )
