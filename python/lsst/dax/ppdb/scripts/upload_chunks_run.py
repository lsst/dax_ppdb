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

from ..bigquery.chunk_uploader import ChunkUploader
from ..ppdb import PpdbConfig


def upload_chunks_run(
    ppdb_config: str,
    wait_interval: int,
    upload_interval: int,
    exit_on_empty: bool,
    exit_on_error: bool,
) -> None:
    """Upload chunks to the specified bucket and prefix.

    Parameters
    ----------
    ppdb_config : `str`
        PPDB configuration URI.
    wait_interval : `int`
        Time in seconds to wait before checking for new chunks to upload.
    upload_interval : `int`
        Time in seconds to wait between uploads of chunks.
    exit_on_empty : `bool`
        If `True`, exit the process if there are no chunks to upload.
    exit_on_error : `bool`
        If `True`, exit the process if there is an error during upload.
    """
    config = PpdbConfig.from_uri(ppdb_config)
    chunk_exporter = ChunkUploader(
        config,
        wait_interval=wait_interval,
        upload_interval=upload_interval,
        exit_on_empty=exit_on_empty,
        exit_on_error=exit_on_error,
    )
    chunk_exporter.run()
