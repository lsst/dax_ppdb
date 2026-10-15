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

"""Client for uploading and deleting objects in Google Cloud Storage."""

__all__ = ["DeleteError", "StorageClient", "StorageError", "UploadError"]

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from google.cloud.storage import Client


class StorageError(RuntimeError):
    """Base error for Google Cloud Storage operations."""


class UploadError(StorageError):
    """Raised if a single upload fails."""


class DeleteError(StorageError):
    """Raised if a recursive delete fails."""


class StorageClient:
    """A client for interacting with a Google Cloud Storage bucket.

    Parameters
    ----------
    bucket_name
        The name of the Google Cloud Storage bucket to interact with.
    """

    def __init__(self, bucket_name: str):
        self.client = Client()
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(bucket_name)

    def upload_file(self, blob_name: str, file_path: Path) -> None:
        """Upload a file to the specified blob name in the bucket.

        Parameters
        ----------
        blob_name
            The name of the blob in the bucket.
        file_path
            The local path to the file to upload.

        Raises
        ------
        UploadError
            Raised if the upload fails.
        """
        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_filename(file_path)
        except Exception as e:
            raise UploadError(f"upload failed: {file_path} -> {blob_name}") from e

    def upload_files(self, gcs_names: dict[Path, str]) -> None:
        """Upload files in parallel.

        Parameters
        ----------
        gcs_names
            A dictionary mapping local file paths to their corresponding GCS
            object names.

        Raises
        ------
        ExceptionGroup
            Raised if any uploads fail, containing an `UploadError` for each
            failed upload.
        """
        failures: list[UploadError] = []

        with ThreadPoolExecutor() as executor:
            # Map the futures to their source paths and object names to
            # handle exceptions later.
            future_to_upload = {
                executor.submit(self.upload_file, object_name, src_path): (src_path, object_name)
                for src_path, object_name in gcs_names.items()
            }

            for future in as_completed(future_to_upload):
                try:
                    # Trigger exception for the upload, if any.
                    future.result()
                except UploadError as e:
                    failures.append(e)

        if failures:
            raise ExceptionGroup("One or more uploads failed.", failures)

    def upload_from_string(self, blob_name: str, file_content: str | bytes) -> None:
        """Upload a string or bytes as a blob to the bucket.

        Parameters
        ----------
        blob_name
            The name of the blob in the bucket.
        file_content
            The content to upload.

        Raises
        ------
        UploadError
            Raised if the upload fails.
        """
        try:
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(file_content)
        except Exception as e:
            raise UploadError(f"upload failed: <content> -> {blob_name}") from e

    def delete_recursive(self, gcs_prefix: str) -> None:
        """Recursively delete all of the objects under a GCS prefix.

        Parameters
        ----------
        gcs_prefix
            The prefix of the objects to delete.

        Raises
        ------
        DeleteError
            Raised if listing or deleting the objects fails.
        """
        # The listing is fetched lazily, so it is read fully here to keep
        # listing errors separate from delete errors.
        try:
            blobs = list(self.bucket.list_blobs(prefix=gcs_prefix))
        except Exception as e:
            raise DeleteError(f"Failed to list objects under prefix: {gcs_prefix}") from e
        try:
            for blob in blobs:
                blob.delete()
        except Exception as e:
            raise DeleteError(f"Failed to delete objects under prefix: {gcs_prefix}") from e
