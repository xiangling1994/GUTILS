#!/usr/bin/env python

import os
import json
import unittest

from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)

from gutils.nc import open_glider_netcdf

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


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

        if os.path.isfile(self.test_path):
            self.mode = 'a'
        else:
            self.mode = 'w'

    def test_with(self):
        with open_glider_netcdf(self.test_path, self.mode) as glider_nc:
            glider_nc.set_global_attributes(self.global_attributes)
        self.assertTrue(os.path.isfile(self.test_path))

    def test_set_trajectory_id(self):
        with open_glider_netcdf(self.test_path, self.mode) as glider_nc:
            glider_nc.set_trajectory_id(
                self.deployment['glider'],
                self.deployment['trajectory_date']
            )
            nc = glider_nc.nc
            traj_str = "%s-%s" % (
                self.deployment['glider'],
                self.deployment['trajectory_date']
            )

            self.assertEqual(
                nc.variables['trajectory'][:].tostring().decode('utf-8'), traj_str
            )

    def test_segment_id(self):
        with open_glider_netcdf(self.test_path, self.mode) as glider_nc:
            glider_nc.set_segment_id(3)
            nc = glider_nc.nc
            self.assertEqual(nc.variables['segment_id'].getValue(), 3)

    def test_profile_ids(self):
        with open_glider_netcdf(self.test_path, self.mode) as glider_nc:
            glider_nc.set_profile_id(4)
            nc = glider_nc.nc
            self.assertEqual(nc.variables['profile_id'].getValue(), 4)

    def test_set_platform(self):
        with open_glider_netcdf(self.test_path, self.mode) as glider_nc:
            glider_nc.set_platform(self.deployment['platform'])
            nc = glider_nc.nc
            self.assertEqual(
                nc.variables['platform'].getncattr('wmo_id'),
                4801516
            )

    def test_set_instruments(self):
        with open_glider_netcdf(self.test_path, self.mode) as glider_nc:
            glider_nc.set_instruments(self.instruments)
            nc = glider_nc.nc
            self.assertIn('instrument_ctd', nc.variables)

    def test_data_insert(self):
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

        with open_glider_netcdf(self.test_path, self.mode) as glider_nc:
            for line in reader:
                glider_nc.stream_dict_insert(line)


if __name__ == '__main__':
    unittest.main()
