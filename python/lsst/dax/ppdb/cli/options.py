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

__all__ = ["felis_schema_options", "replication_options", "sql_db_options"]

import argparse


def felis_schema_options(parser: argparse.ArgumentParser) -> None:
    """Define CLI options for Felis schema file.

    Parameters
    ----------
    parser : `~argparse.ArgumentParser`
        The argument parser to which the options are added.
    """
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
    """Define CLI options for database connection.

    Parameters
    ----------
    parser : `~argparse.ArgumentParser`
        The argument parser to which the options are added.
    """
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
    """Define CLI options for replication.

    Parameters
    ----------
    parser : `~argparse.ArgumentParser`
        The argument parser to which the options are added.
    """
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
    group.add_argument(
        "--exit-on-empty",
        help="Exit if no chunks are found.",
        default=False,
        action="store_true",
    )


def upload_options(parser: argparse.ArgumentParser) -> None:
    """Define CLI options for Google Cloud Storage upload.

    Parameters
    ----------
    parser : `~argparse.ArgumentParser`
        The argument parser to which the options are added.
    """
    group = parser.add_argument_group("upload chunk options")
    group.add_argument(
        "--wait-interval",
        type=int,
        help="Number of seconds to wait between file scans.",
        default=30,
    )
    group.add_argument(
        "--upload-interval",
        type=int,
        help="Number of seconds to wait between uploading chunks.",
        default=0,
    )
    group.add_argument(
        "--exit-on-empty",
        help="Exit if no new files are found after scanning.",
        default=False,
        action="store_true",
    )
    group.add_argument(
        "--exit-on-error",
        help="Exit if an error occurs during upload.",
        default=False,
        action="store_true",
    )


def bigquery_options(parser: argparse.ArgumentParser) -> None:
    """Define CLI options for BigQuery.

    Parameters
    ----------
    parser : `~argparse.ArgumentParser`
        The argument parser to which the options are added.
    """
    group = parser.add_argument_group("BigQuery options")
    group.add_argument(
        "--replication-dir",
        help="Local directory for writing replica chunk data.",
        metavar="DIRECTORY",
        required=True,
    )
    group.add_argument(
        "--delete-existing-dirs",
        help="Delete existing directories for chunks before export.",
        default=False,
        action="store_true",
    )
    group.add_argument(
        "--stage-chunk-topic",
        type=str,
        help="Pub/Sub topic name for triggering chunk staging process.",
        metavar="STRING",
        default=None,
    )
    group.add_argument(
        "--parq-batch-size",
        type=int,
        help="Number of rows to process in each batch when writing parquet files, default: %(default)s.",
        default=None,
    )
    group.add_argument(
        "--parq-compression",
        type=str,
        help="Compression format for Parquet files, default: %(default)s.",
        # List is not comprehensive but provides the most common options and a
        # few zstd levels; more can be added later if needed.
        choices={"none", "snappy", "gzip", "brotli", "lz4", "zstd", "zstd_lvl8", "zstd_lvl15"},
        default=None,
    )
    group.add_argument(
        "--bucket-name",
        type=str,
        help="Name of Google Cloud Storage bucket for uploading chunks.",
        metavar="BUCKET_NAME",
        required=True,
    )
    group.add_argument(
        "--dataset-id",
        type=str,
        help="BigQuery dataset ID, e.g., 'my_dataset'.",
        metavar="DATASET_NAME",
        required=True,
    )
    group.add_argument(
        "--project-id",
        type=str,
        help="Google Cloud project ID containing the dataset.",
        metavar="PROJECT_ID",
        required=True,
    )
    group.add_argument(
        "--object-prefix",
        type=str,
        help="Base prefix for replication objects in cloud storage.",
        metavar="PREFIX",
        required=True,
    )
    group.add_argument(
        "--validate-config",
        help="Enable/disable validation of configuration against GCP resources.",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
