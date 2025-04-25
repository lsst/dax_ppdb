#!/usr/bin/env python


import apache_beam as beam
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions
import logging


# Suppress only the broken UpdateDestinationSchema warning
class BeamSuppressUpdateDestinationSchemaWarning(logging.Filter):
    def filter(self, record):
        if record.name == "apache_beam.transforms.core":
            message = str(record.getMessage())
            if "No iterator is returned by the process method" in message:
                return False
        return True


logging.getLogger("apache_beam.transforms.core").addFilter(BeamSuppressUpdateDestinationSchemaWarning())


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--dataset_id", required=True, help="BigQuery dataset ID")
        parser.add_argument(
            "--input_path", required=True, help="GCS path to directory containing Parquet files"
        )


def read_parquet(pipeline, input_path, name):
    return pipeline | f"Read{name}" >> ReadFromParquet(f"{input_path}/{name}.parquet")


def write_to_bigquery(pcoll, project_id, dataset_id, table_name, temp_location):
    return pcoll | f"Write{table_name}" >> WriteToBigQuery(
        table=f"{project_id}:{dataset_id}.{table_name}",
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location=temp_location,
    )


def run(argv=None):
    options = PipelineOptions(argv)
    custom_options = options.view_as(CustomOptions)

    gcp_options = options.view_as(GoogleCloudOptions)
    options.view_as(SetupOptions).save_main_session = True

    temp_location = gcp_options.temp_location
    project_id = gcp_options.project

    dataset_id = custom_options.dataset_id
    input_path = custom_options.input_path

    with beam.Pipeline(options=options) as p:
        diaobject = read_parquet(p, input_path, "DiaObject")
        diasource = read_parquet(p, input_path, "DiaSource")
        diaforcedsource = read_parquet(p, input_path, "DiaForcedSource")

        write_to_bigquery(diaobject, project_id, dataset_id, "DiaObject", temp_location)
        write_to_bigquery(diasource, project_id, dataset_id, "DiaSource", temp_location)
        write_to_bigquery(diaforcedsource, project_id, dataset_id, "DiaForcedSource", temp_location)


if __name__ == "__main__":
    run()
