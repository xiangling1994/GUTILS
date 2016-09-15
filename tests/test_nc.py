#!/usr/bin/env python

import os
import json
import unittest

import numpy as np
import netCDF4 as nc4

from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)

from gutils.nc import open_glider_netcdf

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


def decoder(x):
    return str(x.decode('utf-8'))


class TestMergedGliderDataReader(unittest.TestCase):

    def setUp(self):
        # Load NetCDF Configs
        contents = ''
        global_attr_path = (
            os.path.join(
                os.path.dirname(__file__),
                'resources',
                'usf-bass',
                'global_attributes.json'
            )
        )
        with open(global_attr_path, 'r') as f:
            self.global_attributes = json.loads(f.read())

        bass_global_attr_path = os.path.join(
            os.path.dirname(__file__),
            'resources',
            'usf-bass',
            'deployment.json'
        )
        with open(bass_global_attr_path, 'r') as f:
            contents = f.read()
        self.deployment = json.loads(contents)
        self.global_attributes.update(self.deployment['global_attributes'])

        instruments_config_path = os.path.join(
            os.path.dirname(__file__),
            'resources',
            'usf-bass',
            'instruments.json'
        )
        with open(instruments_config_path, 'r') as f:
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
                [os.path.join(
                    os.path.dirname(__file__),
                    'resources',
                    'usf-bass',
                    'usf-bass-2014-061-1-0.sbd'
                )]
            )
            scienceReader = GliderBDReader(
                [os.path.join(
                    os.path.dirname(__file__),
                    'resources',
                    'usf-bass',
                    'usf-bass-2014-061-1-0.tbd'
                )]
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
