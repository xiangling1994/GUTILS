#!/usr/bin/env python
import os
import math
import shutil
from collections import OrderedDict
from glob import glob
from tempfile import mkdtemp

import numpy as np
import pandas as pd
from gsw import z_from_p

from gutils import generate_stream, get_decimal_degrees, interpolate_gps, masked_epoch
from gutils.ctd import calculate_practical_salinity, calculate_density

import logging
L = logging.getLogger(__name__)


MODE_MAPPING = {
    "rt": ["sbd", "tbd", "mbd", "nbd"],
    "delayed": ["dbd", "ebd"]
}


class SlocumReader(object):

    TIMESTAMP_SENSORS = ['m_present_time', 'sci_m_present_time']
    PRESSURE_SENSORS = ['sci_water_pressure', 'm_water_pressure', 'm_pressure']

    def __init__(self, ascii_file):
        self.ascii_file = ascii_file
        self.metadata, self.data = self.read()

        # Set the mode to 'rt' or 'delayed'
        self.mode = None
        if 'filename_extension' in self.metadata:
            filemode = self.metadata['filename_extension']
            for m, extensions in MODE_MAPPING.items():
                if filemode in extensions:
                    self.mode = m
                    break

    def read(self):
        metadata = OrderedDict()
        headers = None
        with open(self.ascii_file, 'rt') as af:
            for li, al in enumerate(af):
                if 'm_present_time' in al:
                    headers = al.strip().split(' ')
                elif headers is not None:
                    data_start = li + 2  # Skip units line and the interger row after that
                    break
                else:
                    title, value = al.split(':', maxsplit=1)
                    metadata[title.strip()] = value.strip()

        df = pd.read_csv(
            self.ascii_file,
            index_col=False,
            skiprows=data_start,
            header=None,
            names=headers,
            sep=' ',
            skip_blank_lines=True,
        )
        return metadata, df

    def standardize(self, gps_prefix=None):

        df = self.data.copy()

        # Convert NMEA coordinates to decimal degrees
        for col in df.columns:
            # Ignore if the m_gps_lat and/or m_gps_lon value is the default masterdata value
            if '_lat' in col:
                df[col] = df[col].map(lambda x: get_decimal_degrees(x) if x <= 9000 else math.nan)
            elif '_lon' in col:
                df[col] = df[col].map(lambda x: get_decimal_degrees(x) if x < 18000 else math.nan)

        # Standardize 'time' to the 't' column
        for t in self.TIMESTAMP_SENSORS:
            if t in df.columns:
                df['t'] = pd.to_datetime(df[t], unit='s')

        # Interpolate GPS coordinates
        if 'm_gps_lat' in df.columns and 'm_gps_lon' in df.columns:

            df['y'] = df.m_gps_lat.copy()
            df['x'] = df.m_gps_lon.copy()

            try:
                y_interp, x_interp = interpolate_gps(
                    masked_epoch(df.t),
                    df.y,
                    df.x
                )
            except ValueError:
                L.warning("Raw GPS values no found!")
                y_interp = np.empty(df.y.size) * math.nan
                x_interp = np.empty(df.x.size) * math.nan

            # TODO: Do we will x and y forward? Or at NetCDF creation time?
            df['y_interp'] = y_interp
            df['x_interp'] = x_interp

        # Standardize 'pressure' to the 'z' column
        for p in self.PRESSURE_SENSORS:
            if p in df.columns:
                # Calculate depth from pressure (multiplied by 10 to get to decibars) and latitude
                # Negate the results so that increasing values note increasing depths
                depths = -z_from_p(df[p] * 10, df.y_interp)
                df['z'] = depths
                break

        standard_columns = {
            'm_altitude': 'altitude',
            'm_pitch': 'pitch',
            'm_roll': 'roll',
            'm_water_vy': 'v',
            'm_water_vx': 'u',
            'm_heading': 'heading',
            'sci_water_temp': 'temperature',
            'sci_bbfl2s_chlor_scaled': 'chlorophyll',
            'sci_water_cond': 'conductivity',
            'sci_water_pressure': 'pressure',
            'sci_bbfl2s_bb_scaled': 'backscatter',
            'sci_oxy3835_oxygen': 'dissolved_oxygen',
            'sci_bbfl2s_cdom_scaled': 'cdom',
        }

        # Standardize columns
        df = df.rename(columns=standard_columns)

        # Compute additional columns
        df = self.compute(df)

        return df

    def compute(self, df):
        try:
            # Compute salinity
            df['salinity'] = calculate_practical_salinity(
                time=masked_epoch(df.t),
                conductivity=df.conductivity.values,
                temperature=df.temperature.values,
                pressure=df.pressure.values,
            )
        except (ValueError, AttributeError) as e:
            L.error("Could not compute salinity: {}".format(e))

        try:
            # Compute density
            df['density'] = calculate_density(
                time=masked_epoch(df.t),
                temperature=df.temperature.values,
                pressure=df.pressure.values,
                salinity=df.salinity.values,
                latitude=df.y_interp,
                longitude=df.x_interp,
            )
        except (ValueError, AttributeError) as e:
            L.error("Could not compute density: {}".format(e))

        return df


class SlocumMerger(object):
    """
    Merges flight and science data files into an ASCII file.

    Copies files matching the regex in source_directory to their own temporary directory
    before processing since the Rutgers supported script only takes foldesr as input

    Returns a list of flight/science files that were processed into ASCII files
    """

    def __init__(self, source_directory, destination_directory, cache_directory=None, globs=None):

        globs = globs or []

        self.tmpdir = mkdtemp(prefix='gutils_convert_')
        self.matched_files = []
        self.cache_directory = cache_directory or source_directory
        self.destination_directory = destination_directory
        self.source_directory = source_directory

        for g in globs:
            self.matched_files += list(glob(
                os.path.join(
                    source_directory,
                    g
                )
            ))

    def __del__(self):
        # Remove tmpdir
        shutil.rmtree(self.tmpdir)

    def convert(self):
        # Copy to tempdir
        for f in self.matched_files:
            fname = os.path.basename(f)
            tmpf = os.path.join(self.tmpdir, fname)
            shutil.copy2(f, tmpf)

        if not os.path.isdir(self.destination_directory):
            os.makedirs(self.destination_directory)

        # Run conversion script
        convert_binary_path = os.path.join(
            os.path.dirname(__file__),
            'bin',
            'convertDbds.sh'
        )
        pargs = [
            convert_binary_path,
            '-q',
            '-p',
            '-c', self.cache_directory
        ]

        pargs.append(self.tmpdir)
        pargs.append(self.destination_directory)

        command_output, return_code = generate_stream(pargs)

        # Return
        processed = []
        output_files = command_output.read().split('\n')
        # iterate and every time we hit a .dat file we return the cache
        binary_files = []
        for x in output_files:
            fname = os.path.basename(x)
            _, suff = os.path.splitext(fname)
            if suff == '.dat':
                processed.append({
                    'ascii': os.path.join(self.destination_directory, fname),
                    'binary': sorted(binary_files)
                })
                binary_files = []
            else:
                binary_files.append(os.path.join(self.source_directory, fname))

        return processed
