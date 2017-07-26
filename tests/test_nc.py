#!python
# coding=utf-8
import os
import shutil
import unittest
from collections import namedtuple

import netCDF4 as nc4

from gutils.scripts.create_glider_netcdf import process_dataset

import logging
L = logging.getLogger()
L.handlers = [logging.StreamHandler()]
L.setLevel(logging.DEBUG)


def decoder(x):
    return str(x.decode('utf-8'))


def resource(*args):
    return os.path.join(
        os.path.dirname(__file__),
        'resources',
        *args
    )


def output(*args):
    return os.path.join(
        os.path.dirname(__file__),
        'output',
        *args
    )


class TestCreateGliderScript(unittest.TestCase):

    def setUp(self):
        self.nt = namedtuple('Arguments', [
            'file',
            'glider_config_path',
            'output_path',
            'subset',
            'filter_distance',
            'filter_points',
            'filter_time',
            'filter_z',
        ])

    def tearDown(self):
        outputs = [
            #output('bass-20150407T1300Z'),
            #output('bass-20160909T1733Z'),
            #output('usf-2016'),
            #output('modena-2015')
        ]

        for d in outputs:
            try:
                shutil.rmtree(d)
            except (IOError, OSError):
                pass

    def test_defaults(self):
        args = self.nt(
            file=resource('slocum', 'usf-2016', 'usf_bass_2016_253_0_6_sbd.dat'),
            glider_config_path=resource('slocum', 'usf-2016'),
            output_path=output('usf-2016'),
            subset=False,
            filter_distance=1,
            filter_points=5,
            filter_time=10,
            filter_z=1
        )

        process_dataset(args)

        output_folders = os.listdir(output('usf-2016'))
        assert len(output_folders) == 1
        assert 'bass-20160909T1733Z' in output_folders

        out_base = output('usf-2016', 'bass-20160909T1733Z')

        output_files = sorted(os.listdir(out_base))
        output_files = [ os.path.join(out_base, o) for o in output_files ]
        assert len(output_files) == 32

        # First profile
        with nc4.Dataset(output_files[0]) as ncd:
            assert ncd.variables['profile_id'][0] == 1

        # Last profile
        with nc4.Dataset(output_files[-1]) as ncd:
            assert ncd.variables['profile_id'][0] == 32

    def test_delayed(self):
        args = self.nt(
            file=resource('slocum', 'modena-2015', 'modena_2015_175_0_9_dbd.dat'),
            glider_config_path=resource('slocum', 'modena-2015'),
            output_path=output('modena-2015'),
            subset=False,
            filter_distance=1,
            filter_points=5,
            filter_time=10,
            filter_z=1
        )

        process_dataset(args)

        output_folders = os.listdir(output('modena-2015'))
        assert len(output_folders) == 1
        assert 'modena-20160909T1758' in output_folders

        out_base = output('modena-2015', 'modena-20160909T1758')

        output_files = sorted(os.listdir(out_base))
        output_files = [ os.path.join(out_base, o) for o in output_files ]
        assert len(output_files) == 6

        # First profile
        with nc4.Dataset(output_files[0]) as ncd:
            assert ncd.variables['profile_id'][0] == 1

        # Last profile
        with nc4.Dataset(output_files[-1]) as ncd:
            assert ncd.variables['profile_id'][0] == 6
