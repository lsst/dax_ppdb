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

from __future__ import annotations

__all__ = ["PpdbReplicaChunkSql"]

import datetime
import os

import astropy.time
import felis
import sqlalchemy
import yaml
from felis.datamodel import Schema as FelisSchema
from lsst.dax.apdb import ReplicaChunk, VersionTuple, schema_model
from lsst.dax.apdb.sql import ModelToSql
from lsst.resources import ResourcePath
from sqlalchemy import sql

from ..ppdb import ChunkStatus, PpdbReplicaChunk
from ._ppdb_sql import PpdbSql

VERSION = VersionTuple(0, 1, 1)
"""Version for the code defined in this module. This needs to be updated
(following compatibility rules) when schema produced by this code changes.
"""


class PpdbReplicaChunkSql(PpdbSql):
    """Extension to the `PpdbSql` class that provides additional functionality
    for managing PPDB replica chunks.
    """

    @classmethod
    def _read_schema(
        cls, schema_file: str | None, schema_name: str | None, felis_schema: str | None, db_url: str
    ) -> tuple[sqlalchemy.schema.MetaData, VersionTuple]:
        # Docstring is inherited from `PpdbSql`.
        # This method adds several fields to the original ``PpdbReplicaChunk``
        # table including ``directory`` and ``status``.
        if schema_file is None:
            schema_file = os.path.expandvars(cls.default_felis_schema_file)

        res = ResourcePath(schema_file)
        schemas_list = list(yaml.load_all(res.read(), Loader=yaml.SafeLoader))
        if not schemas_list:
            raise ValueError(f"Schema file {schema_file!r} does not define any schema")
        if felis_schema is not None:
            schemas_list = [schema for schema in schemas_list if schema.get("name") == felis_schema]
            if not schemas_list:
                raise ValueError(f"Schema file {schema_file!r} does not define schema {felis_schema!r}")
        elif len(schemas_list) > 1:
            raise ValueError(f"Schema file {schema_file!r} defines multiple schemas")
        schema_dict = schemas_list[0]

        # In case we use APDB schema drop tables that are not needed in PPDB.
        filtered_tables = [
            table for table in schema_dict["tables"] if table["name"] not in ("DiaObjectLast",)
        ]
        schema_dict["tables"] = filtered_tables
        dm_schema: FelisSchema = felis.datamodel.Schema.model_validate(schema_dict)
        schema = schema_model.Schema.from_felis(dm_schema)

        # Replace schema name with a configured one, just in case it may be
        # used by someone.
        if schema_name:
            schema.name = schema_name

        # Add replica chunk table.
        table_name = "PpdbReplicaChunk"
        columns = [
            schema_model.Column(
                name="apdb_replica_chunk",
                id=f"#{table_name}.apdb_replica_chunk",
                datatype=felis.datamodel.DataType.long,
            ),
            schema_model.Column(
                name="last_update_time",
                id=f"#{table_name}.last_update_time",
                datatype=felis.datamodel.DataType.timestamp,
                nullable=False,
            ),
            schema_model.Column(
                name="unique_id",
                id=f"#{table_name}.unique_id",
                datatype=schema_model.ExtraDataTypes.UUID,
                nullable=False,
            ),
            schema_model.Column(
                name="replica_time",
                id=f"#{table_name}.replica_time",
                datatype=felis.datamodel.DataType.timestamp,
                nullable=False,
            ),
            schema_model.Column(
                name="status",
                id=f"#{table_name}.status",
                datatype=felis.datamodel.DataType.string,
                nullable=True,
            ),
            schema_model.Column(
                name="directory",
                id=f"#{table_name}.directory",
                datatype=felis.datamodel.DataType.string,
                nullable=True,
            ),
        ]
        indices = [
            schema_model.Index(
                name="PpdbInsertId_idx_last_update_time",
                id="#PpdbInsertId_idx_last_update_time",
                columns=[columns[1]],
            ),
            schema_model.Index(
                name="PpdbInsertId_idx_replica_time",
                id="#PpdbInsertId_idx_replica_time",
                columns=[columns[3]],
            ),
        ]

        # Add table for replication support.
        chunks_table = schema_model.Table(
            name=table_name,
            id=f"#{table_name}",
            columns=columns,
            primary_key=[columns[0]],
            indexes=indices,
            constraints=[],
        )
        schema.tables.append(chunks_table)

        if schema.version is not None:
            version = VersionTuple.fromString(schema.version.current)
        else:
            # Missing schema version is identical to 0.1.0
            version = VersionTuple(0, 1, 0)

        metadata = sqlalchemy.schema.MetaData(schema=schema_name)

        converter = ModelToSql(metadata=metadata)
        converter.make_tables(schema.tables)

        # Add an additional index to DiaObject table to speed up replication.
        # This is a partial index (Postgres-only), we do not have support for
        # partial indices in ModelToSql, so we have to do it using sqlalchemy.
        url = sqlalchemy.engine.make_url(db_url)
        if url.get_backend_name() == "postgresql":
            table: sqlalchemy.schema.Table | None = None
            for table in metadata.tables.values():
                if table.name == "DiaObject":
                    name = "IDX_DiaObject_diaObjectId_validityEnd_IS_NULL"
                    sqlalchemy.schema.Index(
                        name,
                        table.columns["diaObjectId"],
                        postgresql_where=table.columns["validityEnd"].is_(None),
                    )
                    break
            else:
                # Cannot find table, odd, but what do I know.
                pass

        return metadata, version

    def get_replica_chunks_by_status(self, status: ChunkStatus) -> list[PpdbReplicaChunk]:
        """Return collection of replica chunks known to the database with a
        given status. This is an alternative to `get_replica_chunks` that
        allows to filter chunks by status. The original method is left for
        backward compatibility.

        Parameters
        ----------
        status : `ChunkStatus`
            Status of the replica chunks to return.

        Returns
        -------
        chunks : `list` [`PpdbReplicaChunk`] or `None`
            List of chunks with the specified status.
        """
        table = self._get_table("PpdbReplicaChunk")
        query = sql.select(
            table.columns["apdb_replica_chunk"],
            table.columns["last_update_time"],
            table.columns["unique_id"],
            table.columns["replica_time"],
            table.columns["status"],
            table.columns["directory"],
        ).order_by(table.columns["last_update_time"])
        query = query.where(table.columns["status"] == status.value)
        ids: list[PpdbReplicaChunk] = []
        with self._engine.connect() as conn:
            result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
            for row in result:
                # When we store these timestamps we convert astropy Time to
                # unix_tai and then to `datetime` in UTC. This conversion
                # reverses that process,
                last_update_time = astropy.time.Time(row[1], format="datetime", scale="tai")
                replica_time = astropy.time.Time(row[3], format="datetime", scale="tai")
                ids.append(
                    PpdbReplicaChunk(
                        id=row[0],
                        last_update_time=last_update_time,
                        unique_id=row[2],
                        replica_time=replica_time,
                        status=row[4],
                        directory=row[5],
                    )
                )
        return ids

    def _store_insert_id(
        self,
        replica_chunk: ReplicaChunk,
        connection: sqlalchemy.engine.Connection,
        update: bool,
        status: ChunkStatus | None = None,
        directory: str | None = None,
    ) -> None:
        """Insert or replace single record in PpdbReplicaChunk table, including
        the status and directory of the replica chunk.
        """
        # `astropy.Time.datetime` returns naive datetime, even though all
        # astropy times are in UTC. Add UTC timezone to timestampt so that
        # database can store a correct value.
        insert_dt = datetime.datetime.fromtimestamp(
            replica_chunk.last_update_time.unix_tai, tz=datetime.timezone.utc
        )
        now = datetime.datetime.fromtimestamp(astropy.time.Time.now().unix_tai, tz=datetime.timezone.utc)

        table = self._get_table("PpdbReplicaChunk")

        values = {"last_update_time": insert_dt, "unique_id": replica_chunk.unique_id, "replica_time": now}
        if status is not None:
            values["status"] = status.value
        if directory is not None:
            values["directory"] = directory
        row = {"apdb_replica_chunk": replica_chunk.id} | values
        if update:
            # We need UPSERT which is dialect-specific construct
            if connection.dialect.name == "sqlite":
                insert_sqlite = sqlalchemy.dialects.sqlite.insert(table)
                insert_sqlite = insert_sqlite.on_conflict_do_update(
                    index_elements=table.primary_key, set_=values
                )
                connection.execute(insert_sqlite, row)
            elif connection.dialect.name == "postgresql":
                insert_pg = sqlalchemy.dialects.postgresql.dml.insert(table)
                insert_pg = insert_pg.on_conflict_do_update(constraint=table.primary_key, set_=values)
                connection.execute(insert_pg, row)
            else:
                raise TypeError(f"Unsupported dialect {connection.dialect.name} for upsert.")
        else:
            insert = table.insert()
            connection.execute(insert, row)
