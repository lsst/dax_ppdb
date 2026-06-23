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
from collections.abc import Sequence

from lsst.dax.apdb.cli.logging_cli import LoggingCli

from .. import scripts
from . import options


def main(argv: Sequence[str] | None = None) -> None:
    """PPDB command line tools.

    Parameters
    ----------
    argv
        Command line arguments to parse. If `None`, ``sys.argv[1:]`` is used.
    """
    parser = argparse.ArgumentParser(description="PPDB command line tools")
    log_cli = LoggingCli(parser)

    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _create_sql_subcommand(subparsers)
    _create_bigquery(subparsers)
    _create_datasets(subparsers)

    args = parser.parse_args(argv)
    log_cli.process_args(args)

    kwargs = vars(args)
    method = kwargs.pop("method")
    method(**kwargs)


def _create_sql_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("create-sql", help="Create new PPDB instance in SQL database.")
    parser.add_argument("db_url", help="Database URL in SQLAlchemy format for PPDB instance.")
    parser.add_argument("output_config", help="Name of the new configuration file for created PPDB instance.")
    options.felis_schema_options(parser)
    options.sql_db_options(parser)
    parser.add_argument(
        "--drop", help="If True then drop existing tables.", default=False, action="store_true"
    )
    parser.set_defaults(method=scripts.create_sql)


def _create_bigquery(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "create-bq", help="Create BigQuery PPDB configuration and database for tracking replica chunks."
    )
    parser.add_argument("db_url", help="Database URL in SQLAlchemy format for PPDB instance.")
    parser.add_argument("output_config", help="Name of the new BigQuery PPDB configuration file.")
    # We don't reuse the SQL options here because we don't need most of them.
    parser.add_argument("--db-drop", help="Drop existing SQL db tables.", default=False, action="store_true")
    parser.add_argument(
        "--db-schema",
        help="Optional schema name for db.",
        metavar="DB_SCHEMA",
        default=None,
    )
    options.felis_schema_options(parser)
    options.bigquery_options(parser)
    parser.set_defaults(method=scripts.create_bigquery)


def _create_datasets(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "create-datasets", help="Create BigQuery datasets for the PPDB from a Felis schema file."
    )
    parser.add_argument("config", help="URI to the PPDB configuration file.")
    parser.add_argument(
        "--exists-ok",
        help="Do not fail if a BigQuery dataset, table, or view already exists; skip creating it.",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--configure-authorized-views",
        help="Configure internal dataset access entries for public authorized views after build.",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--disable-search-indexes",
        help="Skip creating BigQuery search indexes for the datasets.",
        default=False,
        action="store_true",
    )

    parser.set_defaults(method=scripts.create_datasets)
