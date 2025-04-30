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

import apache_beam
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
from google.cloud import storage
import argparse
import json
import logging


class BeamSuppressUpdateDestinationSchemaWarning(logging.Filter):
    """Suppresses the UpdateDestinationSchema warning from Apache Beam."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Suppress the UpdateDestinationSchema warning.

        Parameters
        ----------
        record : `logging.LogRecord`
            The log record to filter.
        """
        if record.name == "apache_beam.transforms.core":
            message = str(record.getMessage())
            if "No iterator is returned by the process method" in message:
                return False
        return True


logging.getLogger("apache_beam.transforms.core").addFilter(BeamSuppressUpdateDestinationSchemaWarning())


class CustomOptions(PipelineOptions):
    """Custom options for the pipeline."""

    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser):
        """Add custom arguments to the parser.

        Parameters
        ----------
        parser : `argparse.ArgumentParser`
            The argument parser to add arguments to.
        """
        parser.add_argument("--dataset_id", required=True, help="BigQuery dataset ID")
        parser.add_argument(
            "--input_path", required=True, help="GCS path to directory containing Parquet files"
        )


def read_parquet(pipeline: apache_beam.Pipeline, input_path: str, name: str):
    """Read Parquet files from GCS.

    Parameters
    ----------
    pipeline : `apache_beam.Pipeline`
        The Apache Beam pipeline.
    input_path : `str`
        The GCS path to the directory containing Parquet files.
    name : `str`
        The name of the Parquet file to read (without extension).
    """
    return pipeline | f"Read{name}" >> ReadFromParquet(f"{input_path}/{name}")


def write_to_bigquery(
    pcoll: apache_beam.PCollection, project_id: str, dataset_id: str, table_name: str, temp_location: str
):
    """Write PCollection to BigQuery.

    Parameters
    ----------
    pcoll : `apache_beam.PCollection`
        The PCollection to write to BigQuery.
    project_id : `str`
        The GCP project ID.
    dataset_id : `str`
        The BigQuery dataset ID.
    table_name : `str`
        The name of the BigQuery table.
    temp_location : `str`
        The GCS path for temporary files.

    Returns
    -------
    `apache_beam.PTransform`
        The transform to write the PCollection to BigQuery.
    """
    return pcoll | f"Write{table_name}" >> WriteToBigQuery(
        table=f"{project_id}:{dataset_id}.{table_name}",
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location=temp_location,
    )


def read_manifest_from_gcs(input_path: str) -> dict:
    """
    Read the manifest.json file from GCS and return it as a Python dictionary.

    Parameters
    ----------
    input_path : str
        The GCS path to the directory containing the manifest.json file.

    Returns
    -------
    dict
        The contents of the manifest.json file as a Python dictionary.
    """
    # Parse the bucket name and object path from the input_path
    if not input_path.endswith("/"):
        input_path += "/"
    bucket_name, object_path = input_path[5:].split("/", 1)
    manifest_path = f"{object_path}manifest.json"

    # Initialize the GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(manifest_path)

    # Download and parse the manifest file
    manifest_content = blob.download_as_text()
    return json.loads(manifest_content)


def run(argv=None):
    """Run the pipeline."""
    options = PipelineOptions(argv)
    custom_options = options.view_as(CustomOptions)

    gcp_options = options.view_as(GoogleCloudOptions)
    options.view_as(SetupOptions).save_main_session = True

    temp_location = gcp_options.temp_location
    if not temp_location:
        raise ValueError("GCP temp_location must be set in pipeline options.")

    project_id = gcp_options.project

    dataset_id = custom_options.dataset_id
    input_path = custom_options.input_path

    try:
        manifest = read_manifest_from_gcs(input_path)
        logging.info(f"Manifest content: {json.dumps(manifest, indent=2)}")
    except Exception:
        logging.exception("Failed to read manifest.json from GCS")
        raise

    if not manifest.get("table_files"):
        raise ValueError("Manifest is missing 'table_files' key or it is empty.")

    logging.info(f"Loading table files: {manifest['table_files']}")

    with apache_beam.Pipeline(options=options) as p:
        for table_name, table_file in manifest["table_files"].items():
            data = read_parquet(p, input_path, table_file)
            write_to_bigquery(data, project_id, dataset_id, table_name, temp_location)


if __name__ == "__main__":
    run()
