#!/usr/bin/env python

import os
import unittest

from gutils import get_decimal_degrees

from gutils.yo import (
    find_yo_extrema
)

from gutils.yo.filters import (
    filter_profile_depth,
    filter_profile_time,
    filter_profile_distance,
    filter_profile_number_of_points
)

from gutils.gps import (
    interpolate_gps
)

from gutils.ctd import (
    calculate_practical_salinity,
    calculate_density
)

import numpy as np

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


def is_continuous(profiled_dataset):
    last_profile_id = 0
    for i, row in enumerate(profiled_dataset):
        profile_diff = abs(last_profile_id - row[2])

        if profile_diff == 1:
            last_profile_id = row[2]
        elif profile_diff > 1:
            print(
                "Inconsistency @: %d, Last Profile: %d, Current: %d"
                % (i, last_profile_id, row[2])
            )
            return False

    return True


def is_complete(profiled_dataset, dataset):
    return len(profiled_dataset) == len(dataset)


ctd_filepath = os.path.join(
    os.path.dirname(__file__),
    'resources',
    'ctd_dataset.csv'
)


class TestFindProfile(unittest.TestCase):

    def setUp(self):
        self.dataset = np.loadtxt(ctd_filepath, delimiter=',')
        self.profiled_dataset = find_yo_extrema(
            self.dataset[:, 0], self.dataset[:, 3]
        )

    def test_find_profile(self):
        self.assertNotEqual(
            len(self.profiled_dataset),
            0
        )
        self.assertTrue(is_complete(self.profiled_dataset, self.dataset))

    def test_extreme_depth_filter(self):
        filtered_profiled_dataset = filter_profile_depth(
            self.profiled_dataset, 10000
        )
        uniques = np.unique(filtered_profiled_dataset[:, 2])
        self.assertEqual(len(uniques), 1)

    def test_filter_profile_depth(self):
        filtered_profiled_dataset = filter_profile_depth(
            self.profiled_dataset, 36
        )
        self.assertNotEqual(
            len(np.unique(self.profiled_dataset[:, 2])),
            len(np.unique(filtered_profiled_dataset[:, 2]))
        )
        self.assertTrue(is_continuous(filtered_profiled_dataset))
        self.assertTrue(is_complete(filtered_profiled_dataset, self.dataset))

    def test_filter_profile_time(self):
        filtered_profiled_dataset = filter_profile_time(
            self.profiled_dataset, 300
        )
        self.assertNotEqual(
            len(np.unique(self.profiled_dataset[:, 2])),
            len(np.unique(filtered_profiled_dataset[:, 2]))
        )
        self.assertTrue(is_continuous(filtered_profiled_dataset))
        self.assertTrue(is_complete(filtered_profiled_dataset, self.dataset))

    def test_filter_profile_distance(self):
        filtered_profiled_dataset = filter_profile_distance(
            self.profiled_dataset, 150
        )
        self.assertNotEqual(
            len(np.unique(self.profiled_dataset[:, 2])),
            len(np.unique(filtered_profiled_dataset[:, 2]))
        )
        self.assertTrue(is_continuous(filtered_profiled_dataset))
        self.assertTrue(is_complete(filtered_profiled_dataset, self.dataset))

    def test_filter_profile_number_of_points(self):
        filtered_profiled_dataset = filter_profile_number_of_points(
            self.profiled_dataset, 20
        )
        self.assertNotEqual(
            len(np.unique(self.profiled_dataset[:, 2])),
            len(np.unique(filtered_profiled_dataset[:, 2]))
        )
        self.assertTrue(is_continuous(filtered_profiled_dataset))
        self.assertTrue(is_complete(filtered_profiled_dataset, self.dataset))

    def test_default_filter_composite(self):
        filtered_profiled_dataset = filter_profile_depth(self.profiled_dataset)
        filtered_profiled_dataset = filter_profile_number_of_points(
            filtered_profiled_dataset, 20
        )
        filtered_profiled_dataset = filter_profile_time(
            filtered_profiled_dataset
        )
        filtered_profiled_dataset = filter_profile_distance(
            filtered_profiled_dataset
        )
        self.assertNotEqual(
            len(np.unique(self.profiled_dataset[:, 2])),
            len(np.unique(filtered_profiled_dataset[:, 2]))
        )
        self.assertTrue(is_continuous(filtered_profiled_dataset))
        self.assertTrue(is_complete(filtered_profiled_dataset, self.dataset))

        #pp = pprint.PrettyPrinter(indent=4)
        #pp.pprint(filtered_profiled_dataset)


class TestInterpolateGPS(unittest.TestCase):

    def setUp(self):
        self.ctd_dataset = np.loadtxt(ctd_filepath, delimiter=',')

    def test_interpolate_gps(self):
        est_lat, est_lon = interpolate_gps(
            self.ctd_dataset[:, 0],
            self.ctd_dataset[:, 4], self.ctd_dataset[:, 5]
        )
        self.assertEqual(len(est_lat), len(est_lon))
        self.assertEqual(len(self.ctd_dataset[:, 0]), len(est_lat))


class TestSalinity(unittest.TestCase):

    def setUp(self):
        self.ctd_dataset = np.loadtxt(ctd_filepath, delimiter=',')

    def test_practical_salinity(self):
        salinity = calculate_practical_salinity(
            self.ctd_dataset[:, 0],
            self.ctd_dataset[:, 1],
            self.ctd_dataset[:, 2],
            self.ctd_dataset[:, 3]
        )
        self.assertEqual(len(self.ctd_dataset[:, 0]), len(salinity))


class TestDensity(unittest.TestCase):

    def setUp(self):
        self.ctd_dataset = np.loadtxt(ctd_filepath, delimiter=',')
        self.lat, self.lon = interpolate_gps(
            self.ctd_dataset[:, 0],
            self.ctd_dataset[:, 4], self.ctd_dataset[:, 5]
        )

    def test_density(self):
        salinity = calculate_practical_salinity(
            self.ctd_dataset[:, 0],
            self.ctd_dataset[:, 1],
            self.ctd_dataset[:, 2],
            self.ctd_dataset[:, 3]
        )
        density = calculate_density(
            self.ctd_dataset[:, 0],
            self.ctd_dataset[:, 2],
            self.ctd_dataset[:, 3],
            salinity,
            self.lat, self.lon
        )
        self.assertEqual(len(self.ctd_dataset[:, 0]), len(density))


class TestUtility(unittest.TestCase):

    def test_decimal_degrees(self):
        decimal_degrees = get_decimal_degrees(-8330.567)
        self.assertEqual(
            decimal_degrees,
            -83.50945
        )

        decimal_degrees = get_decimal_degrees(3731.9404)
        self.assertEqual(
            decimal_degrees,
            37.53234
        )

        decimal_degrees = get_decimal_degrees(10601.6986)
        self.assertEqual(
            decimal_degrees,
            106.02831
        )
