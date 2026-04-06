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

__all__ = ["UpdatesManager"]

import logging
import posixpath
import urllib
from collections.abc import Sequence

from google.cloud import bigquery, storage

from ..manifest import Manifest
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


class UpdatesManager:
    """Class responsible for managing the process of applying updates to the
    PPDB database, including expanding them into a generic format from JSON and
    inserting into the updates table, selecting only the latest updates to a
    new table, and, finally, merging the updates into target BigQuery tables.

    Parameters
    ----------
    config : `PpdbBigQueryConfig`
        Configuration for the PPDB BigQuery interface.
    mergers : `Sequence` [ `UpdatesMerger` ], optional
        Sequence of `UpdatesMerger` instances to use for merging updates into
        target tables. If not provided, a default set of mergers will be used.
    table_name_format : `str`, optional
        Optional format string for the target table names used by the mergers.
    """

    def __init__(
        self,
        config: PpdbBigQueryConfig,
        mergers: Sequence[UpdatesMerger] | None = None,
        table_name_format: str | None = None,
    ) -> None:
        # Get some necessary setup information from the config
        project_id = config.project_id
        dataset_id = config.dataset_id
        bucket_name = config.bucket_name

        # Merger instances for handling each target table
        if mergers and table_name_format:
            raise ValueError("Cannot specify both 'mergers' and 'table_name_format'")
        if mergers:
            self._mergers = mergers
        else:
            self._mergers = tuple(cls(table_name_format=table_name_format) for cls in _DEFAULT_MERGER_CLASSES)

        # Setup the updates table interface
        self._bq_client = bigquery.Client()
        self._updates_table = UpdatesTable(
            self._bq_client,
            project_id,
            dataset_id,
        )

        # GCS setup
        self._gcs_client = storage.Client()
        self._bucket = self._gcs_client.bucket(bucket_name)

        # Dataset containing the target merge tables for the updates
        self._target_dataset_fqn = f"{project_id}.{dataset_id}"

    def apply_updates(self, replica_chunks: Sequence[PpdbReplicaChunkExtended]) -> None:
        """Apply update records from replica chunk data to target tables in
        BigQuery.

        Parameters
        ----------
        replica_chunks: `Sequence` [ `PpdbReplicaChunkExtended` ]
            The replica chunks with the update records.
        """
        # Create the updates table, first dropping if it already exists
        self._updates_table.recreate()

        # Process the replica chunks to build the expanded updates table
        self._process_chunks(replica_chunks)

        # Get a fresh reference to the updates table to check if there were
        # any update records generated from the processed replica chunks
        bq_updates_table = self._bq_client.get_table(self._updates_table.table_fqn)

        # Check if there were any updates in this set of chunks
        if bq_updates_table.num_rows > 0:
            # Select only the latest update records to a new table
            self._updates_table.create_latest_only()

            # Merge the latest-only updates into the target tables
            self._merge_updates(self._updates_table.latest_only_table_fqn)
        else:
            # No updates were present in the processed replica chunks
            _LOG.info("No update records found when processing replica chunks")

    # FIXME: It would be better if there were a flag on the extended replica
    # chunk interface that was read from the db so that this method received a
    # pre-filtered list of only those chunks with updates. This would also
    # make checking the manifests in GCS unnecessary.
    def _process_chunks(self, chunks: Sequence[PpdbReplicaChunkExtended]) -> None:
        for chunk in chunks:
            if chunk.gcs_uri is None:
                raise ValueError(f"Replica chunk {chunk.id} does not have a GCS URI")

            # Parse the GCS URI
            parsed_uri = urllib.parse.urlparse(chunk.gcs_uri)

            # Create the GCS bucket
            bucket_name = parsed_uri.netloc
            bucket = self._gcs_client.bucket(bucket_name)

            # Prefix of the chunk for building URIs
            chunk_prefix = parsed_uri.path.lstrip("/")

            # Load the manifest file for the chunk from GCS
            manifest_uri = posixpath.join(chunk_prefix, Manifest.FILE_NAME)
            manifest_blob = bucket.blob(manifest_uri)
            manifest_content = manifest_blob.download_as_text()
            manifest = Manifest.from_json_str(manifest_content)

            # Read the update records if the chunk was flagged as having them
            if manifest.includes_update_records:
                # Get the update records file contents from the bucket
                object_name = posixpath.join(parsed_uri.path.lstrip("/"), UpdateRecords.FILE_NAME)
                blob = bucket.blob(object_name)
                content = blob.download_as_text()

                # Expand the update records into the appropriate format and
                # insert them into the updates table
                update_records = UpdateRecords.from_json_string(content)
                expanded_update_records = UpdateRecordExpander.expand_updates(update_records)
                self._updates_table.insert(expanded_update_records)

    def _merge_updates(self, target_table_fqn: str) -> None:
        for merger in self._mergers:
            merger.merge(
                client=self._bq_client,
                updates_table_fqn=target_table_fqn,
                target_dataset_fqn=self._target_dataset_fqn,
            )
