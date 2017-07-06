#!/usr/bin/env python

import os
import unittest

from gutils import get_decimal_degrees, interpolate_gps, masked_epoch

from gutils.yo import (
    assign_profiles
)

from gutils.yo.filters import (
    default_filter,
    filter_profile_depth,
    filter_profile_timeperiod,
    filter_profile_distance,
    filter_profile_number_of_points
)

from gutils.ctd import (
    calculate_practical_salinity,
    calculate_density
)

from gutils.slocum import SlocumReader

import logging
L = logging.getLogger()
L.handlers = [logging.StreamHandler()]
L.setLevel(logging.DEBUG)


def is_continuous(profiled_dataset):
    last_profile_id = 0
    for i, row in enumerate(profiled_dataset.itertuples()):
        profile_diff = abs(last_profile_id - row.profile)

        if profile_diff == 1:
            last_profile_id = row.profile
        elif profile_diff > 1:
            print(
                "Inconsistency @: %d, Last Profile: %d, Current: %d"
                % (i, last_profile_id, row.profile)
            )
            return False

    return True


ctd_filepath = os.path.join(
    os.path.dirname(__file__),
    'resources',
    'slocum',
    'usf-2016',
    'usf_bass_2016_253_0_6_sbd.dat'
)


class TestFindProfile(unittest.TestCase):

    def setUp(self):
        sr = SlocumReader(ctd_filepath)
        self.df = sr.standardize()

        self.profiled_dataset = assign_profiles(
            self.df
        )

    def test_find_profile(self):
        assert len(self.profiled_dataset) != 0
        assert len(self.profiled_dataset) == len(self.df)

    def test_extreme_depth_filter(self):
        # All profiles have at least 1m of depth, so this should filter all profiles with at least
        # one depth (44 of them)
        fdf = filter_profile_depth(
            self.profiled_dataset,
            below=1
        )
        profiles = fdf.profile.unique()
        # 63 total profiles but only 44 have any depths (the rest are all NaN) and are valid
        assert len(profiles) == 44
        assert is_continuous(fdf) is True

        fdf = filter_profile_depth(
            self.profiled_dataset,
            below=10000
        )
        profiles = fdf.profile.unique()
        # No profiles are deeper than 10000m
        assert len(profiles) == 0
        assert is_continuous(fdf) is True
        assert fdf.empty

    def test_filter_profile_timeperiod(self):
        fdf = filter_profile_timeperiod(
            self.profiled_dataset,
            300
        )
        profiles = fdf.profile.unique()
        # 18 profiles are longer than 300 seconds
        assert len(profiles) == 18
        assert is_continuous(fdf) is True

        fdf = filter_profile_timeperiod(
            self.profiled_dataset,
            10
        )
        profiles = fdf.profile.unique()
        # 32 profiles are longer than 10 seconds
        assert len(profiles) == 32
        assert is_continuous(fdf) is True

        fdf = filter_profile_timeperiod(
            self.profiled_dataset,
            10000
        )
        profiles = fdf.profile.unique()
        # 0 profiles are longer than 10000
        assert len(profiles) == 0
        assert is_continuous(fdf) is True
        assert fdf.empty

    def test_filter_profile_distance(self):
        fdf = filter_profile_distance(
            self.profiled_dataset,
            0
        )
        profiles = fdf.profile.unique()
        # 63 total profiles but only 44 have any depths (the rest are all NaN) and are valid
        assert len(profiles) == 44
        assert is_continuous(fdf) is True

        fdf = filter_profile_distance(
            self.profiled_dataset,
            10000
        )
        profiles = fdf.profile.unique()
        # 0 profiles span 10000m
        assert len(profiles) == 0
        assert is_continuous(fdf) is True
        assert fdf.empty

    def test_filter_profile_number_of_points(self):
        fdf = filter_profile_number_of_points(
            self.profiled_dataset,
            0
        )
        profiles = fdf.profile.unique()
        # 63 total profiles but only 47 have more than 0 points
        assert len(profiles) == 47
        assert is_continuous(fdf) is True

        fdf = filter_profile_number_of_points(
            self.profiled_dataset,
            10000
        )
        profiles = fdf.profile.unique()
        # 0 profiles have 10000 points
        assert len(profiles) == 0
        assert is_continuous(fdf) is True
        assert fdf.empty

        fdf = filter_profile_number_of_points(
            self.profiled_dataset,
            130
        )
        profiles = fdf.profile.unique()
        # 1 profiles have 130 points (thats the largest)
        assert len(profiles) == 1
        assert is_continuous(fdf) is True
        assert profiles == [0]  # Make sure it reindexed to the "0" profile

    def test_default_filter_composite(self):
        fdf = filter_profile_depth(self.profiled_dataset)
        fdf = filter_profile_number_of_points(fdf)
        fdf = filter_profile_timeperiod(fdf)
        fdf = filter_profile_distance(fdf)

        # Make sure something was subset
        assert len(self.df) != len(fdf)
        assert is_continuous(fdf) is True

        default_df = default_filter(self.profiled_dataset)
        # Make surethe default filter works as intended
        assert len(self.df) != len(default_df)
        assert is_continuous(default_df) is True
        assert default_df.equals(fdf)


class TestInterpolateGPS(unittest.TestCase):

    def setUp(self):
        sr = SlocumReader(ctd_filepath)
        self.df = sr.standardize()

    def test_interpolate_gps(self):
        est_lat, est_lon = interpolate_gps(
            timestamps=masked_epoch(self.df.t),
            latitude=self.df.y,
            longitude=self.df.x
        )
        assert len(est_lat) == len(est_lon)
        assert len(est_lat) == self.df.y.size
        assert len(est_lon) == self.df.x.size


class TestSalinity(unittest.TestCase):

    def test_practical_salinity(self):
        sr = SlocumReader(ctd_filepath)
        salinity = calculate_practical_salinity(
            sr.data.sci_m_present_time,
            sr.data.sci_water_cond,
            sr.data.sci_water_temp,
            sr.data.sci_water_pressure,
        )
        assert sr.data.sci_m_present_time.size == salinity.size


class TestDensity(unittest.TestCase):

    def test_density(self):
        sr = SlocumReader(ctd_filepath)
        df = sr.standardize()

        salinity = calculate_practical_salinity(
            sr.data.sci_m_present_time,
            sr.data.sci_water_cond,
            sr.data.sci_water_temp,
            sr.data.sci_water_pressure,
        )
        assert sr.data.sci_m_present_time.size == salinity.size

        est_lat, est_lon = interpolate_gps(
            timestamps=masked_epoch(df.t),
            latitude=df.y,
            longitude=df.x
        )

        density = calculate_density(
            sr.data.sci_m_present_time,
            sr.data.sci_water_temp,
            sr.data.sci_water_pressure,
            salinity,
            est_lat,
            est_lon
        )
        assert sr.data.sci_m_present_time.size == density.size


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
