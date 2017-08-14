#!python
# coding=utf-8
import os
import shutil
import unittest
from glob import glob
from collections import namedtuple

import netCDF4 as nc4

from gutils import safe_makedirs
from gutils.nc import check_dataset, create_dataset
from gutils.slocum import SlocumReader
from gutils.tests import resource, output, setup_testing_logger

import logging
L = logging.getLogger(__name__)  # noqa
setup_testing_logger()


def decoder(x):
    return str(x.decode('utf-8'))


class TestCreateGliderScript(unittest.TestCase):

    def tearDown(self):
        outputs = [
            resource('slocum', 'real', 'netcdf')
        ]
        for d in outputs:
            try:
                shutil.rmtree(d)
            except (IOError, OSError):
                pass

    def test_defaults(self):
        out_base = resource('slocum', 'real', 'netcdf', 'bass-20160909T1733')
        args = dict(
            file=resource('slocum', 'usf_bass_2016_253_0_6_sbd.dat'),
            reader_class=SlocumReader,
            config_path=resource('slocum', 'real', 'config', 'bass-20160909T1733'),
            output_path=out_base,
            subset=False,
            filter_distance=1,
            filter_points=5,
            filter_time=10,
            filter_z=1
        )
        create_dataset(**args)

        output_files = sorted(os.listdir(out_base))
        output_files = [ os.path.join(out_base, o) for o in output_files ]
        assert len(output_files) == 32

        # First profile
        with nc4.Dataset(output_files[0]) as ncd:
            assert ncd.variables['profile_id'][0] == 1473517507

        # Last profile
        with nc4.Dataset(output_files[-1]) as ncd:
            assert ncd.variables['profile_id'][0] == 1473527118

        # Check netCDF file for compliance
        ds = namedtuple('Arguments', ['file'])
        for o in output_files:
            assert check_dataset(ds(file=o)) == 0

    def test_all_ascii(self):
        out_base = resource('slocum', 'real', 'netcdf', 'bass-20160909T1733')
        safe_makedirs(out_base)

        for f in glob(resource('slocum', 'usf_bass*.dat')):
            args = dict(
                file=f,
                reader_class=SlocumReader,
                config_path=resource('slocum', 'real', 'config', 'bass-20160909T1733'),
                output_path=out_base,
                subset=False,
                filter_distance=1,
                filter_points=5,
                filter_time=10,
                filter_z=1
            )
            create_dataset(**args)

        output_files = sorted(os.listdir(out_base))
        output_files = [ os.path.join(out_base, o) for o in output_files ]

        # First profile
        with nc4.Dataset(output_files[0]) as ncd:
            assert ncd.variables['profile_id'][0] == 1473446494

        # Last profile
        with nc4.Dataset(output_files[-1]) as ncd:
            assert ncd.variables['profile_id'][0] == 1473527118

        # Check netCDF file for compliance
        ds = namedtuple('Arguments', ['file'])
        for o in output_files:
            assert check_dataset(ds(file=o)) == 0

    def test_delayed(self):
        out_base = resource('slocum', 'real', 'netcdf', 'modena-2015')

        args = dict(
            file=resource('slocum', 'modena_2015_175_0_9_dbd.dat'),
            reader_class=SlocumReader,
            config_path=resource('slocum', 'real', 'config', 'modena-2015'),
            output_path=out_base,
            subset=False,
            filter_distance=1,
            filter_points=5,
            filter_time=10,
            filter_z=1
        )
        create_dataset(**args)

        output_files = sorted(os.listdir(out_base))
        output_files = [ os.path.join(out_base, o) for o in output_files ]
        assert len(output_files) == 6

        # First profile
        with nc4.Dataset(output_files[0]) as ncd:
            assert ncd.variables['profile_id'][0] == 1435275435

        # Last profile
        with nc4.Dataset(output_files[-1]) as ncd:
            assert ncd.variables['profile_id'][0] == 1435282145

        # Check netCDF file for compliance
        ds = namedtuple('Arguments', ['file'])
        for o in output_files:
            assert check_dataset(ds(file=o)) == 0


class TestGliderCheck(unittest.TestCase):

    def setUp(self):
        self.args = namedtuple('Check_Arguments', ['file'])

    def test_passing_testing_compliance(self):
        args = self.args(file=resource('should_pass.nc'))
        assert check_dataset(args) == 0

    def test_failing_testing_compliance(self):
        args = self.args(file=resource('should_fail.nc'))
        assert check_dataset(args) == 1
