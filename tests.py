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


def is_continuous(profiles):
    start_index = 0
    for profile in profiles:
        if profile['index_bounds'][0] == start_index:
            start_index = profile['index_bounds'][1]
        else:
            print(
                "Start: %d, Profile: (%d, %d)"
                % (start_index,
                   profile['index_bounds'][0],
                   profile['index_bounds'][1])
            )
            return False

    return True


def is_complete(profiles, dataset):
    start_index = profiles[0]['index_bounds'][0]
    end_index = profiles[-1]['index_bounds'][1]
    return start_index == 0 and end_index == len(dataset) - 1


class TestFindProfile(unittest.TestCase):
    def setUp(self):
        self.dataset = read_depth_dataset()
        self.profiles = find_yo_extrema(self.dataset)

    def test_find_profile(self):
        self.assertNotEqual(
            len(self.profiles),
            0
        )
        self.assertTrue(is_continuous(self.profiles))
        self.assertTrue(is_complete(self.profiles, self.dataset))

    def test_filter_profile_depth(self):
        filtered_profiles = filter_profile_depth(self.profiles, self.dataset)
        self.assertNotEqual(len(self.profiles), len(filtered_profiles))
        self.assertTrue(is_continuous(filtered_profiles))
        self.assertTrue(is_complete(filtered_profiles, self.dataset))

    def test_filter_profile_time(self):
        filtered_profiles = filter_profile_time(self.profiles, self.dataset)
        self.assertNotEqual(len(self.profiles), len(filtered_profiles))
        self.assertTrue(is_continuous(filtered_profiles))
        self.assertTrue(is_complete(filtered_profiles, self.dataset))

    def test_filter_profile_distance(self):
        filtered_profiles = filter_profile_distance(
            self.profiles, self.dataset
        )
        self.assertNotEqual(len(self.profiles), len(filtered_profiles))
        self.assertTrue(is_continuous(filtered_profiles))
        self.assertTrue(is_complete(filtered_profiles, self.dataset))

    def test_filter_profile_number_of_points(self):
        filtered_profiles = filter_profile_number_of_points(
            self.profiles, self.dataset
        )
        self.assertNotEqual(len(self.profiles), len(filtered_profiles))
        self.assertTrue(is_continuous(filtered_profiles))
        self.assertTrue(is_complete(filtered_profiles, self.dataset))

    def test_default_filter_composite(self):
        filtered_profiles = filter_profile_depth(self.profiles, self.dataset)
        filtered_profiles = filter_profile_number_of_points(
            filtered_profiles, self.dataset
        )
        filtered_profiles = filter_profile_time(
            filtered_profiles, self.dataset
        )
        filtered_profiles = filter_profile_distance(
            filtered_profiles, self.dataset
        )
        self.assertTrue(is_continuous(filtered_profiles))
        self.assertTrue(is_complete(filtered_profiles, self.dataset))

        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(filtered_profiles)


if __name__ == '__main__':
    unittest.main()
