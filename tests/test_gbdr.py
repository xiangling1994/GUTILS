#!/usr/bin/env python

import os
import unittest
from glob import glob

from gutils.gbdr.methods import (
    create_glider_BD_ASCII_reader,
    find_glider_BD_headers,
    get_decimal_degrees
)
from gutils.gbdr import GliderBDReader, MergedGliderBDReader

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

testdata_path = os.path.join(os.path.dirname(__file__), 'resources', 'usf-bass')


class TestASCIIReader(unittest.TestCase):

    def setUp(self):
        self.reader = create_glider_BD_ASCII_reader(
            glob(os.path.join(testdata_path, '*.sbd'))
        )

    def test_single_read(self):
        line = self.reader.readline()
        self.assertEqual(
            line,
            'dbd_label: DBD_ASC(dinkum_binary_data_ascii)file\n'
        )

    def test_find_headers(self):
        headers = find_glider_BD_headers(self.reader)
        self.assertEqual(
            'c_heading',
            headers[0]['name']
        )


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


class TestBDReader(unittest.TestCase):

    def setUp(self):
        self.reader = GliderBDReader(
            glob(os.path.join(testdata_path, '*.tbd'))
        )

    def test_iteration(self):
        for value in self.reader:
            self.assertIn(
                'sci_m_present_secs_into_mission-sec',
                value
            )


class TestMergedGliderDataReader(unittest.TestCase):

    def setUp(self):
        flightReader = GliderBDReader(
            glob(os.path.join(testdata_path, '*.sbd'))
        )
        scienceReader = GliderBDReader(
            glob(os.path.join(testdata_path, '*.tbd'))
        )
        self.reader = MergedGliderBDReader(flightReader, scienceReader)

    def test_iteration(self):
        for value in self.reader:
            time_present = (
                'sci_m_present_secs_into_mission-sec' in value or
                'm_present_secs_into_mission-sec' in value
            )
            self.assertTrue(time_present)


class TestNoCacheAvailable(unittest.TestCase):

    def setUp(self):
        # Remove all cache files
        for fileName in glob("/tmp/*.cac"):
            os.remove(fileName)

        # Create readers
        flightReader = GliderBDReader(
            [os.path.join(testdata_path, 'usf-bass-2014-048-2-3.sbd')]
        )
        scienceReader = GliderBDReader(
            [os.path.join(testdata_path, 'usf-bass-2014-048-2-3.tbd')]
        )
        self.reader = MergedGliderBDReader(flightReader, scienceReader)

    def test_iteration(self):
        for value in self.reader:
            time_present = (
                'sci_m_present_secs_into_mission-sec' in value or
                'm_present_secs_into_mission-sec' in value
            )
            self.assertTrue(time_present)


if __name__ == '__main__':
    unittest.main()
