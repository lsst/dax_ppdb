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


def upload_chunks_run(directory: str, bucket: str, folder: str, wait_interval: int, exit_on_empty: bool):
    """Upload chunks to the specified bucket and folder.

    Parameters
    ----------
    directory : `str`
        Directory containing the chunks to upload.
    bucket_name : `str`
        Name of the bucket to upload the chunks to.
    folder_name : `str`, optional
        Name of the folder within the bucket to upload the chunks to. If not
        provided, the chunks will be uploaded to the root of the bucket.
    """
    chunk_exporter = ChunkUploader(directory, bucket, folder, wait_interval, exit_on_empty)
    chunk_exporter.run()
