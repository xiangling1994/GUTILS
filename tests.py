import unittest

from glider_utils.yo import (
    find_yo_extrema
)

from glider_utils.yo.filters import (
    filter_profile_depth,
    filter_profile_time,
    filter_profile_distance,
    filter_profile_number_of_points
)

from glider_utils.gps import (
    interpolate_gps
)

from glider_utils.ctd import (
    calculate_practical_salinity,
    calculate_density
)

import numpy as np
import pprint
import csv


def read_depth_dataset():
    times = []
    depth = []
    with open('depth_data.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            times.append(float(row[0]))
            depth.append(float(row[1]))

    return np.column_stack((times, depth))


def read_gps_dataset():
    times = []
    lat = []
    lon = []
    with open('gps_data.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            times.append(float(row[0]))
            lat.append(float(row[1]))
            lon.append(float(row[2]))

    return np.column_stack((times, lat, lon))


def read_ctd_dataset():
    times = []
    cond = []
    temp = []
    pres = []
    with open('ctd_data.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            times.append(float(row[0]))
            cond.append(float(row[1]))
            temp.append(float(row[2]))
            pres.append(float(row[3]))

    return np.column_stack((times, cond, temp, pres))


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


class TestFindProfile(unittest.TestCase):
    def setUp(self):
        self.dataset = read_depth_dataset()
        self.profiled_dataset = find_yo_extrema(self.dataset)

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

        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(filtered_profiled_dataset)


class TestInterpolateGPS(unittest.TestCase):
    def setUp(self):
        self.dataset = read_gps_dataset()

    def test_interpolate_gps(self):
        self.assertNotEqual(
            len(interpolate_gps(self.dataset)),
            0
        )


class TestSalinity(unittest.TestCase):
    def setUp(self):
        self.dataset = read_ctd_dataset()

    def test_practical_salinity(self):
        salinity_dataset = calculate_practical_salinity(self.dataset)
        self.assertNotEqual(
            len(salinity_dataset),
            0,
        )


class TestDensity(unittest.TestCase):
    def setUp(self):
        self.ctd_dataset = read_ctd_dataset()
        self.gps_dataset = read_gps_dataset()
        self.gps_dataset = interpolate_gps(self.gps_dataset)

    def test_density(self):
        salinity_dataset = calculate_practical_salinity(self.ctd_dataset)
        density_dataset = calculate_density(
            salinity_dataset,
            self.gps_dataset[:, 1], self.gps_dataset[:, 2]
        )
        print density_dataset
        self.AssertNotEqual(
            len(density_dataset),
            0
        )

if __name__ == '__main__':
    unittest.main()
