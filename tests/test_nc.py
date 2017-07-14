#!python
# coding=utf-8
import os
import shutil
import unittest
from types import SimpleNamespace
from collections import namedtuple

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
            'segment_id',
            'filter_points',
            'filter_distance',
            'filter_time',
            'filter_z',
            'glider_config_path',
            'output_path',
            'subset'
        ])

    def tearDown(self):
        outputs = [
            #output('bass-20150407T1300Z'),
            #output('bass-20160909T1733Z'),
            output('modena-20160909T1758Z')
        ]

        for d in outputs:
            try:
                shutil.rmtree(d)
            except (IOError, OSError):
                pass

    def test_defaults(self):
        args = SimpleNamespace(
            file=resource('slocum', 'usf-2016', 'usf_bass_2016_253_0_6_sbd.dat'),
            glider_config_path=resource('slocum', 'usf-2016'),
            output_path=output('usf-2016'),
            subset=False
        )

        process_dataset(args)

        output_files = os.listdir(output('usf-2016'))
        assert len(output_files) == 1
        assert 'bass_20140303T145556Z_rt.nc' in output_files


if __name__ == '__main__':
    unittest.main()
