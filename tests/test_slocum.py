#!/usr/bin/env python

import os
import shutil
import tempfile
import unittest
from glob import glob

from gutils.slocum import SlocumMerger, SlocumReader

import logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class TestMergedASCIICreator(unittest.TestCase):

    def setUp(self):
        self.binary_path = os.path.join(os.path.dirname(__file__), 'resources', 'slocum', 'usf-bass')
        self.ascii_path = os.path.join(self.binary_path, 'ascii')

    def tearDown(self):
        shutil.rmtree(self.ascii_path)  # Remove generated ASCII

        # Remove any cached .cac files
        for cac in glob(os.path.join(self.binary_path, '*.cac')):
            os.remove(cac)

    def test_convert_default_cache_directory(self):
        merger = SlocumMerger(
            self.binary_path,
            self.ascii_path,
            globs=['*.tbd', '*.sbd']
        )
        p = merger.convert()
        assert len(p) > 0
        assert len(glob(os.path.join(self.ascii_path, '*.dat'))) > 0

    def test_convert_empty_cache_directory(self):
        merger = SlocumMerger(
            self.binary_path,
            self.ascii_path,
            cache_directory=tempfile.mkdtemp(),
            globs=['*.tbd', '*.sbd']
        )
        p = merger.convert()
        assert len(p) > 0
        assert len(glob(os.path.join(self.ascii_path, '*.dat'))) > 0

    def test_convert_single_pair(self):
        merger = SlocumMerger(
            self.binary_path,
            self.ascii_path,
            globs=['usf-bass-2014-048-0-0.tbd', 'usf-bass-2014-048-0-0.sbd']
        )
        p = merger.convert()
        assert p == [{
            'ascii': os.path.join(self.ascii_path, 'usf_bass_2014_048_0_0_sbd.dat'),
            'binary': sorted([
                os.path.join(self.binary_path, 'usf-bass-2014-048-0-0.sbd'),
                os.path.join(self.binary_path, 'usf-bass-2014-048-0-0.tbd')
            ]),
        }]
        assert len(glob(os.path.join(self.ascii_path, '*.dat'))) == 1


class TestMergedASCIIReader(unittest.TestCase):

    def setUp(self):
        self.binary_path = os.path.join(os.path.dirname(__file__), 'resources', 'slocum', 'usf-bass')
        self.ascii_path = os.path.join(self.binary_path, 'ascii')

    def tearDown(self):
        shutil.rmtree(self.ascii_path)  # Remove generated ASCII

        # Remove any cached .cac files
        for cac in glob(os.path.join(self.binary_path, '*.cac')):
            os.remove(cac)

    def test_convert_single_pair(self):
        merger = SlocumMerger(
            self.binary_path,
            self.ascii_path,
            globs=['usf-bass-2014-048-0-0.tbd', 'usf-bass-2014-048-0-0.sbd']
        )
        p = merger.convert()
        af = p[0]['ascii']

        ar = SlocumReader(af)
        headers, columns, df = ar.read()

        logger.info(df.head())
