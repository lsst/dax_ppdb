##################
Uploading SSO data
##################

The Solar System Object (SSO) tables in the PPDB, such as ``SSObject`` and
``SSObservation``, are loaded separately from the replica chunks. A set of SSO
parquet files is uploaded to Cloud Storage with ``ppdb-cli upload-sso-data``,
which then triggers a cloud function that loads them into BigQuery, replacing
the contents of the internal SSO tables.

Configuration
=============

The uploader reads a YAML configuration file that names the bucket and lists
the parquet files for each table:

.. code-block:: yaml

   bucket_name: ppdb-test-bucket
   allow_partial_upload: false
   files:
     SSObject: SSObject.parquet
     SSObservation: SSObservation/*.parquet
     NearbySSO: NearbySSO.parquet
     mpc_orbits: mpc_orbits/*.parquet
     current_identifications: current_identifications.parquet
     numbered_identifications: numbered_identifications.parquet

``bucket_name``
    The Cloud Storage bucket for SSO uploads.

``files``
    The parquet files of each table, by table name. Each entry is a path or a
    glob pattern. A relative path is relative to the directory given on the
    command line, and an absolute one is used as it is. A table stored as a
    single file uses its path, and a table split into several files a
    pattern. File names don't need to match the table names, as the key
    names the table. There are no defaults, so every table to upload must be
    listed.

``allow_partial_upload``
    Whether tables may be left out of ``files``. If ``false``, the default,
    every SSO table must be listed. If ``true``, missing tables are skipped
    with a warning, and only the uploaded tables are replaced when the data is
    loaded.

Before anything is uploaded, the uploader checks that each table name is an
SSO table, that each path or pattern matches at least one file, and that no
file is listed for more than one table. It also reads each file's row count,
and fails if a file can't be read or has no rows.

The parquet files are uploaded as they are, without any cleanup. Their columns
must match the BigQuery tables: a column of the wrong type will cause the load
to fail. Extra columns, such as a pandas index column, are ignored.

Uploading
=========

Run the command with the configuration file and the directory holding the
parquet files, both of which are required:

.. code-block:: bash

   ppdb-cli upload-sso-data sso_uploader.yaml /data/sso/2026-11-18

The command uses the Google Cloud default credentials, which must have
permission to upload SSO data and wait for its load.

Each upload goes under a new prefix in the bucket, named after the time of the
upload as a UTC timestamp to the millisecond, such as ``20261118T120000123``. If
anything is already stored under the prefix, such as from another upload
started at the same moment, the upload fails before writing any file. Whatever
the local file names, each table's files are stored as numbered parts, followed
by a manifest:

.. code-block:: text

   <prefix>/SSObject/part-00000.parquet
   <prefix>/SSObservation/part-00000.parquet
   <prefix>/SSObservation/part-00001.parquet
   ...
   <prefix>/manifest.json

The manifest lists each table's parts with the local path each came from, its
row count, size and SHA-256 checksum. The manifest is written last, so its
presence means that the upload is complete. If any step fails, the files already
uploaded are deleted.

Finally, the command publishes a message to Pub/Sub to trigger the BigQuery
load job.

Loading
=======

The cloud function triggered by the message loads the files into BigQuery from
Cloud Storage with `lsst.dax.ppdb.bigquery.sso.Loader`. All of the Parquet
file partitions are loaded into a landing table. Then a single BigQuery
transaction is used to replace the rows of each internal table with those of
its corresponding landing table. The resulting row counts are checked against
the manifest for correctness. If any step fails, none of the internal tables
are changed.

After uploading, the command waits for the load to complete and reports its
result. An error message will be printed if the load fails or times out.

A failed load isn't retried automatically. To retry it, upload the files again.

Don't upload while another load is running. If you do, the new upload may not
be loaded.
