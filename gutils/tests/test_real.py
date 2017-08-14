#!python
# coding=utf-8
import os
import shutil
from glob import glob

import pytest

from gutils.nc import create_dataset
from gutils.slocum import SlocumMerger, SlocumReader
from gutils.tests import setup_testing_logger, resource

import logging
L = logging.getLogger(__name__)  # noqa
setup_testing_logger()


@pytest.mark.long
@pytest.mark.parametrize("deployment", [
    'bass-20160909T1733',
    'modena-20160909T1758',
    'ramses-20160909T2028',
    'ramses-20170516T1345',
    'salacia-20160919T2028',
    'salacia-20170710T1942',
])
def test_real_deployments(deployment):
    binary_path = resource('slocum', 'real', 'binary', deployment)
    ascii_path = resource('slocum', 'real', 'ascii', deployment)
    netcdf_path = resource('slocum', 'real', 'netcdf', deployment)
    config_path = resource('slocum', 'real', 'config', deployment)

    # Static args
    args = dict(
        reader_class=SlocumReader,
        config_path=config_path,
        output_path=netcdf_path,
        subset=False,
        filter_distance=1,
        filter_points=5,
        filter_time=10,
        filter_z=1
    )

    try:
        merger = SlocumMerger(
            binary_path,
            ascii_path
        )
        for p in merger.convert(): 
            args['file'] = p['ascii']
            create_dataset(**args)
    finally:
        # Cleanup
        shutil.rmtree(ascii_path, ignore_errors=True)  # Remove generated ASCII
        shutil.rmtree(netcdf_path, ignore_errors=True)  # Remove generated netCDF
        # Remove any cached .cac files
        for cac in glob(os.path.join(binary_path, '*.cac')):
            os.remove(cac)
