#!/usr/bin/env python

import os
import json
import unittest
from collections import namedtuple

import numpy as np
import netCDF4 as nc4

from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)

from gutils.nc import open_glider_netcdf
from gutils.scripts.create_glider_netcdf import process_dataset

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


def decoder(x):
    return str(x.decode('utf-8'))


def resource(nm):
    return os.path.join(
        os.path.dirname(__file__),
        'resources',
        'usf-bass',
        nm
    )


def output(*args):
    return os.path.join(
        os.path.dirname(__file__),
        'output',
        *args
    )


class TestCreateGliderScript(unittest.TestCase):

    def test_script(self):
        nt = namedtuple('Arguments', [
            'flight',
            'science',
            'time',
            'depth',
            'gps_prefix',
            'segment_id',
            'mode',
            'glider_config_path',
            'output_path'
        ])

        args = nt(
            flight=resource('usf-bass-2014-061-1-0.sbd'),
            science=resource('usf-bass-2014-061-1-0.tbd'),
            time='timestamp',
            depth='m_depth-m',
            gps_prefix='m_gps_',
            segment_id=None,
            mode='rt',
            glider_config_path=resource('.'),
            output_path=os.path.join(os.path.dirname(__file__), 'output')
        )

        process_dataset(args)

        assert len(os.listdir(output('bass-20150407T1300Z'))) == 5
        assert os.path.isfile(output('bass-20150407T1300Z', 'bass_20140303T095556Z_rt.nc'))
        assert os.path.isfile(output('bass-20150407T1300Z', 'bass_20140303T100510Z_rt.nc'))
        assert os.path.isfile(output('bass-20150407T1300Z', 'bass_20140303T101015Z_rt.nc'))
        assert os.path.isfile(output('bass-20150407T1300Z', 'bass_20140303T101624Z_rt.nc'))
        assert os.path.isfile(output('bass-20150407T1300Z', 'bass_20140303T102040Z_rt.nc'))


class TestMergedGliderDataReader(unittest.TestCase):

    def setUp(self):
        # Load NetCDF Configs
        with open(resource('deployment.json'), 'r') as f:
            self.deployment = json.loads(f.read())

        with open(resource('global_attributes.json'), 'r') as f:
            self.global_attributes = json.loads(f.read())
        self.global_attributes.update(self.deployment['global_attributes'])

        with open(resource('instruments.json'), 'r') as f:
            self.instruments = json.loads(f.read())

        self.test_path = os.path.join(
            os.path.dirname(__file__),
            'output',
            'test.nc'
        )
        try:
            os.makedirs(os.path.dirname(self.test_path))
        except OSError:
            pass  # Already exists

    def test_creation(self):

        with open_glider_netcdf(self.test_path, 'w') as glider_nc:

            # Set global attributes
            glider_nc.set_global_attributes(self.global_attributes)

            # Set Trajectory
            glider_nc.set_trajectory_id(
                self.deployment['glider'],
                self.deployment['trajectory_date']
            )

            traj_str = "{}-{}".format(
                self.deployment['glider'],
                self.deployment['trajectory_date']
            )

            assert 'trajectory' in glider_nc.nc.variables
            vfunc = np.vectorize(decoder)
            self.assertEqual(
                vfunc(nc4.chartostring(glider_nc.nc.variables['trajectory'][:])),
                traj_str
            )

            # Set Platform
            glider_nc.set_platform(self.deployment['platform'])
            self.assertEqual(
                glider_nc.nc.variables['platform'].getncattr('wmo_id'),
                4801516
            )

            # Set Instruments
            glider_nc.set_instruments(self.instruments)
            self.assertIn('instrument_ctd', glider_nc.nc.variables)

            # Set Segment ID
            glider_nc.set_segment_id(3)
            self.assertEqual(
                glider_nc.nc.variables['segment_id'].getValue(),
                3
            )

            # Set Profile ID
            glider_nc.set_profile_id(4)
            self.assertEqual(
                glider_nc.nc.variables['profile_id'].getValue(),
                4
            )

            flightReader = GliderBDReader(
                [resource('usf-bass-2014-061-1-0.sbd')]
            )
            scienceReader = GliderBDReader(
                [resource('usf-bass-2014-061-1-0.tbd')]
            )
            reader = MergedGliderBDReader(flightReader, scienceReader)

            for line in reader:
                glider_nc.stream_dict_insert(line)

            glider_nc.update_profile_vars()
            glider_nc.calculate_salinity()
            glider_nc.calculate_density()
            glider_nc.update_bounds()


if __name__ == '__main__':
    unittest.main()
