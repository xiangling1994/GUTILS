#!/usr/bin/env python
from __future__ import division  # always return floats when dividing

import os
import re
import math
import subprocess
from six import StringIO

import numpy as np
import pandas as pd
from scipy.signal import boxcar, convolve

import logging
L = logging.getLogger(__name__)


def clean_dataset(dataset):
    # Get rid of NaNs
    dataset = dataset[~np.isnan(dataset[:, 1:]).any(axis=1), :]

    return dataset


def boxcar_smooth_dataset(dataset, window_size):
    window = boxcar(window_size)
    return convolve(dataset, window, 'same') / window_size


def validate_glider_args(*args):
    """Validates a glider dataset

    Performs the following changes and checks:
    * Makes sure that there are at least 2 points in the dataset
    * Checks for netCDF4 fill types and changes them to NaNs
    * Tests for finite values in time and depth arrays
    """

    arg_length = len(args[0])

    # Time is assumed to be the first dataset
    if arg_length < 2:
        raise IndexError('The time series must have at least two values')

    # Skipping first (time) argument
    for arg in args[1:]:
        # Make sure all arguments have the same length
        if len(arg) != arg_length:
            raise ValueError('Arguments must all be the same length')

        # Test for finite values
        if len(arg[np.isfinite(arg)]) == 0:
            raise ValueError('Data array has no finite values')


def get_decimal_degrees(lat_lon):
    """Converts NMEA GPS format (DDDmm.mmmm) to decimal degrees (DDD.dddddd)

    Parameters
    ----------
    lat_lon : str
        NMEA GPS coordinate (DDDmm.mmmm)

    Returns
    -------
    float
        Decimal degree coordinate (DDD.dddddd) or math.nan
    """

    # Absolute value of the coordinate
    try:
        pos_lat_lon = abs(lat_lon)
    except (TypeError, ValueError):
        return math.nan

    if math.isnan(pos_lat_lon):
        return lat_lon

    # Calculate NMEA degrees as an integer
    nmea_degrees = int(pos_lat_lon // 100) * 100

    # Subtract the NMEA degrees from the absolute value of lat_lon and divide by 60
    # to get the minutes in decimal format
    gps_decimal_minutes = (pos_lat_lon - nmea_degrees) / 60

    # Divide NMEA degrees by 100 and add the decimal minutes
    decimal_degrees = (nmea_degrees // 100) + gps_decimal_minutes

    # Round to 6 decimal places
    decimal_degrees = round(decimal_degrees, 6)

    if lat_lon < 0:
        return -decimal_degrees

    return decimal_degrees


def masked_epoch(timeseries):
    tmask = pd.isnull(timeseries)
    epochs = np.ma.MaskedArray(timeseries.astype(np.int64) // 1e9)
    epochs.mask = tmask
    return pd.Series(epochs)


def interpolate_gps(timestamps, latitude, longitude):
    """Calculates interpolated GPS coordinates between the two surfacings
    in a single glider binary data file.
    Parameters:
        'dataset': An N by 3 numpy array of time, lat, lon pairs
    Returns interpolated gps dataset over entire time domain of dataset
    """

    validate_glider_args(timestamps, latitude, longitude)

    est_lat = np.array([np.nan] * latitude.size)
    est_lon = np.array([np.nan] * longitude.size)

    anynull = (timestamps.isnull()) | (latitude.isnull()) | (longitude.isnull())
    newtimes = timestamps.loc[~anynull]
    latitude = latitude.loc[~anynull]
    longitude = longitude.loc[~anynull]

    if latitude.size == 0 or longitude.size == 0:
        L.warning('GPS time-seies contains no valid GPS fixes for interpolation')
        return est_lat, est_lon

    # If only one GPS point, make it the same for the entire dataset
    if latitude.size == 1 and longitude.size == 1:
        est_lat[:] = latitude.iloc[0]
        est_lon[:] = longitude.iloc[0]
    else:
        # Interpolate data
        est_lat = np.interp(
            timestamps,
            newtimes,
            latitude,
            left=latitude.iloc[0],
            right=latitude.iloc[-1]
        )
        est_lon = np.interp(
            timestamps,
            newtimes,
            longitude,
            left=longitude.iloc[0],
            right=longitude.iloc[-1]
        )

    return est_lat, est_lon


def parse_glider_filename(filename):
    """
    Parses a glider filename and returns details in a dictionary

    Parameters
    ----------
    filename : str
        A filename to parse

    Returns
    -------
    dict
        Returns dictionary with the following keys:

            * 'glider': glider name
            * 'year': data file year created
            * 'day': data file julian date created
            * 'mission': data file mission id
            * 'segment': data file segment id
            * 'type': data file type
    """
    head, tail = os.path.split(filename)

    matches = re.search(r"([\w\d\-]+)-(\d+)-(\d+)-(\d+)-(\d+)\.(\w+)$", tail)

    if matches is not None:
        return {
            'path': head,
            'glider': matches.group(1),
            'year': int(matches.group(2)),
            'day': int(matches.group(3)),
            'mission': int(matches.group(4)),
            'segment': int(matches.group(5)),
            'type': matches.group(6)
        }
    else:
        raise ValueError(
            "Filename ({}) not in usual glider format: "
            "<glider name>-<year>-<julian day>-"
            "<mission>-<segment>.<extenstion>".format(filename)
        )


def generate_stream(processArgs):
    """ Runs a given process and outputs the resulting text as a StringIO

    Parameters
    ----------
    processArgs : list
        Arguments to run in a process

    Returns
    -------
    StringIO
        Resulting text
    """
    process = subprocess.Popen(
        processArgs,
        universal_newlines=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    stdout, _ = process.communicate()
    return StringIO(stdout), process.returncode
