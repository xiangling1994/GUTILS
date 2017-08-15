#!python
# coding=utf-8
import os
import time
import shutil
import unittest
from glob import glob

from pyinotify import (
    IN_CLOSE_WRITE,
    IN_MOVED_TO,
    ThreadedNotifier,
    WatchManager
)

from gutils import safe_makedirs
from gutils.tests import resource, output, setup_testing_logger
from gutils.slocum import SlocumMerger
from gutils.watch.binary import Slocum2AsciiProcessor
from gutils.watch.ascii import Slocum2NetcdfProcessor
from gutils.watch.netcdf import Netcdf2ErddapProcessor

import logging
L = logging.getLogger(__name__)  # noqa
setup_testing_logger()

config_path = resource('slocum', 'real', 'config', 'bass-20160909T1733')
original_binary = resource('slocum', 'real', 'binary', 'bass-20160909T1733')
binary_path = output('binary', 'bass-20160909T1733')
ascii_path = output('ascii', 'bass-20160909T1733')
netcdf_path = output('netcdf', 'bass-20160909T1733')
erddap_content_path = output('erddap', 'content')
erddap_flag_path = output('erddap', 'flag')
ftp_path = output('ftp')


class TestWatchClasses(unittest.TestCase):

    def setUp(self):
        safe_makedirs(binary_path)
        safe_makedirs(ascii_path)
        safe_makedirs(netcdf_path)
        safe_makedirs(ftp_path)
        safe_makedirs(erddap_content_path)
        safe_makedirs(erddap_flag_path)

    def tearDown(self):
        shutil.rmtree(output())

    def test_gutils_binary_to_ascii_watch(self):

        wm = WatchManager()
        mask = IN_MOVED_TO | IN_CLOSE_WRITE

        # Convert binary data to ASCII
        processor = Slocum2AsciiProcessor(
            outputs_path=os.path.dirname(ascii_path)
        )
        notifier = ThreadedNotifier(wm, processor)
        notifier.coalesce_events()
        notifier.start()

        wdd = wm.add_watch(
            binary_path,
            mask,
            rec=True,
            auto_add=True
        )

        # Wait 5 seconds for the watch to start
        time.sleep(5)

        gpath = os.path.join(original_binary, '*.*bd')
        # Sort the files so the .cac files are generated in the right order
        for g in sorted(glob(gpath)):
            shutil.copy2(g, binary_path)

        # Wait for ASCII to be processed
        time.sleep(30)
        wm.rm_watch(wdd.values(), rec=True)
        notifier.stop()

        # Should output 32 ASCII files
        assert len(os.listdir(ascii_path)) == 32

    def test_gutils_ascii_to_netcdf_watch(self):

        wm = WatchManager()
        mask = IN_MOVED_TO | IN_CLOSE_WRITE

        # Convert ASCII data to NetCDF
        processor = Slocum2NetcdfProcessor(
            outputs_path=os.path.dirname(netcdf_path),
            configs_path=os.path.dirname(config_path),
            subset=False,
            filter_distance=1,
            filter_points=5,
            filter_time=10,
            filter_z=1
        )
        notifier = ThreadedNotifier(wm, processor)
        notifier.coalesce_events()
        notifier.start()

        wdd = wm.add_watch(
            ascii_path,
            mask,
            rec=True,
            auto_add=True
        )

        # Wait 5 seconds for the watch to start
        time.sleep(5)

        # Make the ASCII we are watching for
        merger = SlocumMerger(
            original_binary,
            ascii_path,
            globs=['*.tbd', '*.sbd']
        )
        merger.convert()

        # Wait for NetCDF to be created
        time.sleep(30)
        wm.rm_watch(wdd.values(), rec=True)
        notifier.stop()

        # Should outout 230 NetCDF files
        assert len(os.listdir(netcdf_path)) == 230

    def test_gutils_netcdf_to_erddap_watch(self):

        wm = WatchManager()
        mask = IN_MOVED_TO | IN_CLOSE_WRITE

        # Convert ASCII data to NetCDF
        processor = Netcdf2ErddapProcessor(
            outputs_path=os.path.dirname(netcdf_path),
            erddap_content_path=erddap_content_path,
            erddap_flag_path=erddap_flag_path
        )
        notifier = ThreadedNotifier(wm, processor, read_freq=5)
        notifier.coalesce_events()
        notifier.start()

        wdd = wm.add_watch(
            netcdf_path,
            mask,
            rec=True,
            auto_add=True
        )

        # Wait 5 seconds for the watch to start
        time.sleep(5)

        dummy_netcdf = os.path.join(netcdf_path, 'hello.dummy.nc')
        with open(dummy_netcdf, 'wt') as f:
            f.write('nothing to see here')
            f.write('\n')
        L.debug("Wrote dummy file")

        # Wait for NetCDF to be created
        time.sleep(10)
        wm.rm_watch(wdd.values(), rec=True)
        notifier.stop()

        # Should output 1 ERDDAP datasets.xml file
        assert len(os.listdir(erddap_content_path)) == 1

        # Should try to flag the dataset
        assert len(os.listdir(erddap_flag_path)) == 1
