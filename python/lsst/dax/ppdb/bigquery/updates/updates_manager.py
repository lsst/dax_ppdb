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

from __future__ import annotations

__all__ = ["UpdatesManager", "UpdatesManagerError"]

import logging
import posixpath
import urllib
from collections.abc import Sequence

from google.cloud import bigquery, storage

from ..ppdb_bigquery import PpdbBigQueryConfig
from ..ppdb_replica_chunk_extended import PpdbReplicaChunkExtended
from .update_record_expander import UpdateRecordExpander
from .update_records import UpdateRecords
from .updates_merger import (
    DiaForcedSourceUpdatesMerger,
    DiaObjectUpdatesMerger,
    DiaSourceUpdatesMerger,
    UpdatesMerger,
)
from .updates_table import UpdatesTable

_DEFAULT_MERGER_CLASSES: tuple[type[UpdatesMerger], ...] = (
    DiaObjectUpdatesMerger,
    DiaSourceUpdatesMerger,
    DiaForcedSourceUpdatesMerger,
)

_LOG = logging.getLogger(__name__)


class UpdatesManagerError(Exception):
    """Base exception for errors related to the updates process."""


class UpdatesManager:
    """Class responsible for managing the process of applying updates to the
    PPDB database, including expanding them into a generic format from JSON and
    inserting into the updates table, selecting only the latest updates to a
    new table, and, finally, merging the updates into target BigQuery tables.

    Parameters
    ----------
    config : `PpdbBigQueryConfig`
        Configuration for the PPDB BigQuery interface.
    table_name_format : `str`, optional
        Optional format string for the target table names used by the mergers.
    """

    def __init__(
        self,
        config: PpdbBigQueryConfig,
        table_name_format: str | None = None,
    ) -> None:
        # Get some necessary setup information from the config.
        project_id = config.project_id
        dataset_id = config.dataset_id
        bucket_name = config.bucket_name

        # Merger instances for handling each target table.
        self._mergers = tuple(cls(table_name_format=table_name_format) for cls in _DEFAULT_MERGER_CLASSES)

        # Setup the updates table interface.
        self._bq_client = bigquery.Client()
        self._updates_table = UpdatesTable(
            self._bq_client,
            project_id,
            dataset_id,
        )

        # Setup the GCS client and bucket.
        self._gcs_client = storage.Client()
        self._bucket = self._gcs_client.bucket(bucket_name)

        # Set the target dataset FQN for the mergers to use.
        self._target_dataset_fqn = f"{project_id}.{dataset_id}"

    def apply_updates(self, replica_chunks: Sequence[PpdbReplicaChunkExtended]) -> None:
        """Apply update records from replica chunk data to target tables in
        BigQuery.

        Parameters
        ----------
        replica_chunks: `Sequence` [ `PpdbReplicaChunkExtended` ]
            The replica chunks with the update records.

        Raises
        ------
        UpdatesManagerError
            Raised if any step of the updates process fails.
        """
        # Filter the list of chunks to only those with updates and skip
        # processing entirely if there are none. Caller may also already have
        # done this but we don't rely on it.
        update_chunks = [chunk for chunk in replica_chunks if chunk.update_count > 0]
        if not update_chunks:
            _LOG.info("No update records found in the provided replica chunks")
            return

        # Log the total number of update records. We already checked above
        # that there is at least one update record in the batch of chunks.
        total_update_count = sum(chunk.update_count for chunk in update_chunks)
        _LOG.info("Processing %d update records from %d chunks", total_update_count, len(update_chunks))

        # Recreate the updates table.
        try:
            self._updates_table.recreate()
        except Exception as e:
            raise UpdatesManagerError("Failed to recreate updates table") from e

        # Build the table with the expanded update records.
        try:
            self._build_updates_table(update_chunks)
        except Exception as e:
            raise UpdatesManagerError("Failed to build updates table") from e

        # Select only the latest update records into a new table
        try:
            self._updates_table.create_latest_only()
        except Exception as e:
            raise UpdatesManagerError("Failed to create latest-only updates table") from e

        # Merge the latest-only updates into the target tables
        try:
            self._merge_updates(self._updates_table.latest_only_table_fqn)
        except Exception as e:
            raise UpdatesManagerError("Failed to merge updates into target tables") from e

    def _build_updates_table(self, chunks: Sequence[PpdbReplicaChunkExtended]) -> None:
        """Build the updates table by expanding the update records from the
        replica chunks and inserting them.

        Parameters
        ----------
        chunks : `Sequence` [ `PpdbReplicaChunkExtended` ]
            Replica chunks with update_count > 0.
        """
        for chunk in chunks:
            # A null value for the GCS URI should not be possible under
            # normal circumstances but check anyways before proceeding so that
            # strange errors are not encountered later.
            if chunk.gcs_uri is None:
                raise UpdatesManagerError(f"Replica chunk {chunk.id} does not have a GCS URI")

            try:
                # Parse the GCS URI.
                parsed_uri = urllib.parse.urlparse(chunk.gcs_uri)

                # Create the GCS bucket.
                bucket_name = parsed_uri.netloc
                bucket = self._gcs_client.bucket(bucket_name)

                # Get the object prefix from the URI path, stripping any
                # leading slash.
                chunk_prefix = parsed_uri.path.lstrip("/")

                if chunk_prefix == "":
                    raise ValueError(f"GCS URI '{chunk.gcs_uri}' does not contain an object prefix")

            except Exception as e:
                raise UpdatesManagerError(
                    f"Failed to parse GCS URI '{chunk.gcs_uri}' for replica chunk {chunk.id}"
                ) from e

            # Download the parquet file containing the update records from the
            # bucket.
            try:
                object_name = posixpath.join(chunk_prefix, UpdateRecords.PARQUET_FILE_NAME)
                if not bucket.blob(object_name).exists():
                    raise ValueError(f"GCS object '{object_name}' does not exist in bucket '{bucket_name}'")
                blob = bucket.blob(object_name)
                content = blob.download_as_bytes()
            except Exception as e:
                raise UpdatesManagerError(
                    f"Failed to download update records for replica chunk {chunk.id} "
                    f"from GCS URI '{chunk.gcs_uri}'"
                ) from e

            # Expand the update records into the appropriate format and insert
            # them into the updates table.
            try:
                update_records = UpdateRecords.from_parquet_bytes(content)
                if not update_records:
                    raise ValueError(
                        f"Empty file downloaded from GCS URI '{chunk.gcs_uri}' for replica chunk {chunk.id}"
                    )
                expanded_update_records = UpdateRecordExpander.expand_updates(update_records, chunk.id)
                self._updates_table.insert(expanded_update_records)
            except Exception as e:
                raise UpdatesManagerError(
                    f"Failed to build updates table for replica chunk {chunk.id} "
                    f"from GCS URI '{chunk.gcs_uri}'"
                ) from e

    def _merge_updates(self, target_table_fqn: str) -> None:
        """Merge the latest-only updates into the target tables using the
        table-specific merger classes.

        Parameters
        ----------
        target_table_fqn : `str`
            Fully qualified name of the latest-only updates table for the
            merge operation.
        """
        for merger in self._mergers:
            try:
                _LOG.debug(
                    "Merging updates into target table '%s' using %s", target_table_fqn, type(merger).__name__
                )
                merger.merge(
                    client=self._bq_client,
                    updates_table_fqn=target_table_fqn,
                    target_dataset_fqn=self._target_dataset_fqn,
                )
            except Exception as e:
                raise UpdatesManagerError(f"Failed to merge updates using {type(merger).__name__}") from e
