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

import click

schema_name = click.option(
    "-s",
    "--schema",
    type=click.STRING,
    help="Optional schema name.",
    metavar="DB_SCHEMA",
    default=None,
)

felis_path = click.option(
    "-f",
    "--felis-path",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="YAML file with PPDB felis schema (can be same as APDB schema).",
    metavar="PATH",
    default=None,
)

felis_schema = click.option(
    "--felis-schema",
    type=click.STRING,
    help="Schema name used in felis YAML file.",
    metavar="NAME",
    default=None,
)

no_connection_pool = click.option(
    "--no-connection-pool",
    help="Disable use of connection pool.",
    is_flag=True,
    default=False,
)

isolation_level = click.option(
    "--isolation-level",
    help="Transaction isolation level.",
    metavar="STRING",
    default=None,
)

connection_timeout = click.option(
    "--connection-timeout",
    type=click.FLOAT,
    help="Maximum connection timeout in seconds.",
    metavar="SECONDS",
    default=None,
)

drop = click.option(
    "--drop",
    help="If True then drop existing tables.",
    is_flag=True,
)
