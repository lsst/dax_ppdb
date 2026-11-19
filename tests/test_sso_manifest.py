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

import json
import tempfile
import unittest
from pathlib import Path

import pyarrow
from pyarrow import parquet

from lsst.dax.ppdb.bigquery.manifest import InvalidManifestError, ParquetFileEntry
from lsst.dax.ppdb.bigquery.sso.manifest import FileEntry, Manifest


def _write_parquet(path: Path, row_count: int) -> None:
    """Write a parquet file with an ``ssObjectId`` column."""
    table = pyarrow.table({"ssObjectId": list(range(21165641428803584, 21165641428803584 + row_count))})
    parquet.write_table(table, path)


class FileEntryTestCase(unittest.TestCase):
    """Test building FileEntry from a local parquet file."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.path = Path(self.tempdir.name) / "SSObservation-0.parquet"

    def test_from_path(self) -> None:
        """Test that the row count comes from the parquet metadata, with the
        source path, size and checksum of the file.
        """
        # Write a file with five rows.
        _write_parquet(self.path, row_count=5)

        # Build the entry.
        entry = FileEntry.from_path(self.path)

        # Check the row count, source path, size and checksum.
        self.assertEqual(entry.row_count, 5)
        self.assertEqual(entry.source_path, str(self.path))
        self.assertEqual(entry.size_bytes, self.path.stat().st_size)
        self.assertEqual(entry.checksum, ParquetFileEntry.compute_checksum(self.path))


class ManifestTestCase(unittest.TestCase):
    """Test reading a Manifest from a folder."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.folder = Path(self.tempdir.name)
        self.manifest_path = self.folder / Manifest.FILE_NAME

        # Set up a manifest with SSObservation in two parts.
        self.manifest = Manifest(
            tables={
                "SSObservation": {
                    "part-00000.parquet": FileEntry(
                        row_count=4,
                        checksum="9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
                        size_bytes=1024,
                        source_path="/data/sso/SSObservation/part-0.parquet",
                    ),
                    "part-00001.parquet": FileEntry(
                        row_count=2,
                        checksum="60303ae22b998861bce3b28f33eec1be758a213c86c93c076dbe9f558c11c752",
                        size_bytes=512,
                        source_path="/data/sso/SSObservation/part-1.parquet",
                    ),
                },
            },
        )

    def test_from_uri_reads_manifest(self) -> None:
        """Test that a manifest written to a folder is read back
        unchanged.
        """
        # Write the manifest.
        self.manifest_path.write_text(self.manifest.model_dump_json(indent=2))

        # Check that reading it gives the same manifest.
        self.assertEqual(Manifest.from_uri(self.folder), self.manifest)

    def test_from_uri_errors(self) -> None:
        """Test that reading fails with FileNotFoundError when the manifest
        is missing, and with InvalidManifestError when it isn't valid JSON or
        has an unknown field.
        """
        # Check that reading the empty folder fails.
        with self.assertRaises(FileNotFoundError):
            Manifest.from_uri(self.folder)

        # Make an invalid manifest for each case: one truncated, and one with
        # an extra field, such as a chunk manifest's.
        with_extra_field = self.manifest.model_dump(mode="json")
        with_extra_field["replica_chunk_id"] = "1609459200"
        cases = {
            "invalid JSON": self.manifest.model_dump_json()[:40],
            "unknown field": json.dumps(with_extra_field),
        }

        # Check that reading each one fails.
        for case, content in cases.items():
            with self.subTest(case):
                self.manifest_path.write_text(content)
                with self.assertRaises(InvalidManifestError):
                    Manifest.from_uri(self.folder)


if __name__ == "__main__":
    unittest.main()
