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

import logging
from pathlib import Path

from ..bigquery.sso.uploader import Uploader, UploaderConfig

_LOG = logging.getLogger(__name__)


def upload_sso_data(config: str, directory: str, wait: bool) -> None:
    """Upload SSO parquet files from a directory to Google Cloud Storage,
    and wait for them to be loaded.

    Parameters
    ----------
    config
        URI to the YAML configuration file for the SSO uploader.
    directory
        Path to the directory containing the SSO parquet files.
    wait
        Whether to wait for the load to finish.
    """
    sso_config = UploaderConfig.from_uri(config)
    uploader = Uploader.from_directory(Path(directory), sso_config)
    _LOG.info("Starting SSO data upload from directory: %s", directory)
    uploader.upload()
    _LOG.info("SSO data upload complete.")
    if not wait:
        return
    _LOG.info(
        "Waiting up to %d minutes for the SSO data to be loaded.",
        Uploader.WAIT_TIMEOUT.total_seconds() // 60,
    )
    job_id = uploader.wait_for_load()
    _LOG.info("SSO data loaded by BigQuery job %s.", job_id)
