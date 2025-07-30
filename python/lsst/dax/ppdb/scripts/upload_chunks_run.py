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

from ..export._chunk_uploader import ChunkUploader
from ..sql._ppdb_sql import PpdbSqlConfig


def upload_chunks_run(
    ppdb_config: str,
    bucket: str,
    prefix: str,
    dataset: str,
    topic: str,
    wait_interval: int,
    upload_interval: int,
    exit_on_empty: bool,
    exit_on_error: bool,
) -> None:
    """Upload chunks to the specified bucket and prefix.

    Parameters
    ----------
    directory : `str`
        Directory containing the chunks to upload.
    bucket : `str`
        Name of the bucket to upload the chunks to.
    prefix : `str`, optional
        Prefix within the bucket for object naming.
    dataset : `str`
        Target BigQuery dataset.
    topic : `str`
        Pub/Sub topic for publishing upload events.
    wait_interval : `int`
        Time in seconds to wait before checking for new chunks to upload.
    upload_interval : `int`
        Time in seconds to wait between uploads of chunks.
    exit_on_empty : `bool`
        If `True`, exit the process if there are no chunks to upload.
    """
    ppdb_sql_config = PpdbSqlConfig.from_uri(ppdb_config)
    chunk_exporter = ChunkUploader(
        ppdb_sql_config,
        bucket,
        prefix,
        dataset,
        topic=topic,
        wait_interval=wait_interval,
        upload_interval=upload_interval,
        exit_on_empty=exit_on_empty,
        exit_on_error=exit_on_error,
    )
    chunk_exporter.run()
