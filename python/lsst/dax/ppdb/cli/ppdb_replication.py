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

from lsst.dax.apdb import monitor
from lsst.dax.apdb.cli.logging_cli import LoggingCli

from .. import scripts
from . import options

_longLogFmt = "%(asctime)s %(levelname)s %(name)s - %(message)s"


def main() -> None:
    """Commands for managing APDB-to-PPDB replication."""
    parser = argparse.ArgumentParser(description="PPDB command line tools")
    log_cli = LoggingCli(parser)
    parser.add_argument(
        "--mon-logger", help="Name of the logger to output monitoring metrics.", metavar="LOGGER"
    )
    parser.add_argument(
        "--mon-rules",
        help="Comma-separated list of monitoring filter rules.",
        default="",
        metavar="RULE[,RULE...]",
    )

    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _list_chunks_apdb_subcommand(subparsers)
    _list_chunks_ppdb_subcommand(subparsers)
    _run_subcommand(subparsers)
    _export_chunks_subcommand(subparsers)
    _upload_chunks_subcommand(subparsers)

    args = parser.parse_args()
    log_cli.process_args(args)
    kwargs = vars(args)

    # Setup monitoring output.
    mon_logger = kwargs.pop("mon_logger", None)
    mon_rules = kwargs.pop("mon_rules", None)
    if mon_logger is not None:
        mon_handler = monitor.LoggingMonHandler(mon_logger)
        monitor.MonService().add_handler(mon_handler)
        if mon_rules:
            monitor.MonService().set_filters(mon_rules.split(","))

    method = kwargs.pop("method")
    method(**kwargs)


def _list_chunks_apdb_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "list-chunks-apdb", help="Print full list of replic chunks existing on APDB side."
    )
    parser.add_argument("apdb_config", help="Path to the APDB configuration.")
    parser.set_defaults(method=scripts.replication_list_chunks_apdb)


def _list_chunks_ppdb_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("list-chunks-ppdb", help="List full set of replica chunks in PPDB.")
    parser.add_argument("ppdb_config", help="Path to the PPDB configuration.")
    parser.set_defaults(method=scripts.replication_list_chunks_ppdb)


def _run_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("run", help="Run replication from APDB to PPDB.")
    parser.add_argument("apdb_config", help="Path to the APDB configuration.")
    parser.add_argument("ppdb_config", help="Path to the PPDB configuration.")
    options.replication_options(parser)
    parser.set_defaults(method=scripts.replication_run)


def _export_chunks_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("export-chunks", help="Export data from APDB to Parquet files.")
    parser.add_argument("apdb_config", help="Path to the APDB configuration.")
    parser.add_argument("ppdb_config", help="Path to the PPDB configuration.")
    options.replication_options(parser)
    options.export_options(parser)
    parser.set_defaults(method=scripts.export_chunks_run)


def _upload_chunks_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("upload-chunks", help="Upload data from Parquet files to GCS.")
    parser.add_argument("ppdb_config", help="Path to the PPDB configuration.")
    options.upload_options(parser)
    parser.set_defaults(method=scripts.upload_chunks_run)
