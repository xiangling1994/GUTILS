#!/usr/bin/env python

# create_glider_netcdf.py - A command line script for generating NetCDF files
# from a subset of glider binary data files.
#
# By: Michael Lindemuth <mlindemu@usf.edu>
# University of South Florida
# College of Marine Science
# Ocean Technology Group

import os
import sys
import json
import argparse
from datetime import datetime

import numpy as np

from gutils.gbdr import (
    GliderBDReader,
    MergedGliderBDReader
)


from gutils.yo import find_yo_extrema
from gutils.gps import interpolate_gps
from gutils.yo.filters import default_filter
from gutils.gbdr.methods import parse_glider_filename

from gutils.nc import open_glider_netcdf, GLIDER_UV_DATATYPE_KEYS

import logging
logger = logging.getLogger('gutils.nc')


def create_reader(flight_path, science_path):
    if flight_path is not None:
        flight_reader = GliderBDReader(
            [flight_path]
        )
        if science_path is None:
            return flight_reader
    if science_path is not None:
        science_reader = GliderBDReader(
            [science_path]
        )
        if flight_path is None:
            return science_reader

    return MergedGliderBDReader(flight_reader, science_reader)


def find_profiles(flight_path, science_path, time_name, depth_name):
    profile_values = []
    reader = create_reader(flight_path, science_path)
    for line in reader:
        if depth_name in line:
            profile_values.append([line[time_name], line[depth_name]])

    if not profile_values:
        raise ValueError('Not enough profiles found')

    try:
        profile_values = np.array(profile_values)
        timestamps = profile_values[:, 0]
        depths = profile_values[:, 1]
    except IndexError:
        raise ValueError('Not enough timestamps or depths found')
    else:
        profile_dataset = find_yo_extrema(timestamps, depths)
        return default_filter(profile_dataset)


def get_file_set_gps(flight_path, science_path, time_name, gps_prefix):
    gps_values = []
    reader = create_reader(flight_path, science_path)
    lat_name = gps_prefix + 'lat-lat'
    lon_name = gps_prefix + 'lon-lon'
    for line in reader:
        if lat_name in line:
            gps_values.append(
                [line[time_name], line[lat_name], line[lon_name]]
            )
        else:
            gps_values.append([line[time_name], np.nan, np.nan])

    if not gps_values:
        raise ValueError('Not enough gps posistions found')

    try:
        gps_values = np.array(gps_values)
        timestamps = gps_values[:, 0]
        latitudes = gps_values[:, 1]
        longitudes = gps_values[:, 2]
    except IndexError:
        raise ValueError('Not enough timestamps, latitudes, or longitudes found')
    else:
        gps_values[:, 1], gps_values[:, 2] = interpolate_gps(
            timestamps, latitudes, longitudes
        )

    return gps_values


def fill_gps(line, interp_gps, time_name, gps_prefix):
    lat_name = gps_prefix + 'lat-lat'
    lon_name = gps_prefix + 'lon-lon'
    if lat_name not in line:
        timestamp = line[time_name]
        line[lat_name] = interp_gps[interp_gps[:, 0] == timestamp, 1][0]
        line[lon_name] = interp_gps[interp_gps[:, 0] == timestamp, 2][0]

    return line


def init_netcdf(file_path, attrs, segment_id, profile_id):
    # Check if the output path already exists, remove old file
    mode = 'w'
    if os.path.isfile(file_path):
        os.remove(file_path)

    try:
        os.makedirs(os.path.dirname(file_path))
    except OSError:
        pass  # destination folder exists

    with open_glider_netcdf(file_path, mode) as glider_nc:
        # Set global attributes
        glider_nc.set_global_attributes(attrs['global'])

        # Set Trajectory
        glider_nc.set_trajectory_id(
            attrs['deployment']['glider'],
            attrs['deployment']['trajectory_date']
        )

        # Set Platform
        glider_nc.set_platform(attrs['deployment']['platform'])

        # Set Instruments
        glider_nc.set_instruments(attrs['instruments'])

        # Set Segment ID
        glider_nc.set_segment_id(segment_id)

        # Set Profile ID
        glider_nc.set_profile_id(profile_id)


def find_segment_id(flight_path, science_path):
    if flight_path is None:
        filename = science_path
    else:
        filename = flight_path

    details = parse_glider_filename(filename)
    return details['segment']


def fill_uv_variables(dst_glider_nc, uv_values):
    for key, value in uv_values.items():
        dst_glider_nc.set_scalar(key, value)


def backfill_uv_variables(src_glider_nc, empty_uv_processed_paths):
    uv_values = {}
    for key_name in GLIDER_UV_DATATYPE_KEYS:
        uv_values[key_name] = src_glider_nc.get_scalar(key_name)

    for file_path in empty_uv_processed_paths:
        with open_glider_netcdf(file_path, 'a') as dst_glider_nc:
            fill_uv_variables(dst_glider_nc, uv_values)

    return uv_values


def create_arg_parser():
    parser = argparse.ArgumentParser(
        description='Parses a set of glider binary data files to a '
                    'single NetCDF file according to configurations '
                    'for institution, deployment, glider, and datatypes.'
    )

    parser.add_argument(
        'glider_config_path',
        help='Path to configuration files for this specific glider deployment.'
    )

    parser.add_argument(
        'output_path',
        help='Path to folder to save NetCDF output. A directory named after '
             'the deployment will be created here'
    )

    parser.add_argument(
        '-m', '--mode',
        help="Set the mode for the file naming convention (rt or delayed?)",
        default="delayed"
    )

    parser.add_argument(
        '--segment_id', nargs=1,
        help='Set the segment ID',
        default=None
    )

    parser.add_argument(
        '-t', '--time',
        help="Set time parameter to use for profile recognition",
        default="timestamp"
    )

    parser.add_argument(
        '-d', '--depth',
        help="Set depth parameter to use for profile recognition",
        default="m_depth-m"
    )

    parser.add_argument(
        '-g', '--gps_prefix',
        help="Set prefix for gps parameters to use for location estimation",
        default="m_gps_"
    )

    parser.add_argument(
        '-f', '--flight',
        help="Flight data file to process",
        default=None
    )

    parser.add_argument(
        '-s', '--science',
        help="Science data file to process",
        default=None
    )

    return parser


def read_attrs(glider_config_path):
    # Load in configurations
    attrs = {}

    def cfg_file(name):
        return os.path.join(
            glider_config_path,
            name
        )

    # Load institute global attributes
    global_attrs_path = cfg_file("global_attributes.json")
    with open(global_attrs_path, 'r') as f:
        attrs['global'] = json.load(f)

    # Load deployment attributes (including global attributes)
    deployment_attrs_path = cfg_file("deployment.json")
    with open(deployment_attrs_path, 'r') as f:
        attrs['deployment'] = json.load(f)

    # Load instruments
    instruments_attrs_path = cfg_file("instruments.json")
    with open(instruments_attrs_path, 'r') as f:
        attrs['instruments'] = json.load(f)

    # Fill in global attributes
    attrs['global'].update(attrs['deployment']['global_attributes'])

    return attrs


def process_dataset(args, attrs):
    flight_path = args.flight
    science_path = args.science

    glider_name = attrs['deployment']['glider']
    deployment_name = '{}-{}'.format(
        glider_name,
        attrs['deployment']['trajectory_date']
    )

    try:
        # Find profile breaks
        profiles = find_profiles(flight_path, science_path, args.time, args.depth)

        # Interpolate GPS
        interp_gps = get_file_set_gps(
            flight_path, science_path, args.time, args.gps_prefix
        )
    except ValueError as e:
        logger.error('{} - Skipping'.format(e))
        return 1

    # Create NetCDF Files for Each Profile
    profile_id = 0
    profile_end = 0
    file_path = None
    uv_values = None
    empty_uv_processed_paths = []
    reader = create_reader(flight_path, science_path)
    for line in reader:
        if profile_end < line['timestamp']:
            # Open new NetCDF
            begin_time = datetime.fromtimestamp(line['timestamp'])
            filename = "%s_%s_%s.nc" % (
                glider_name,
                begin_time.isoformat(),
                args.mode
            )
            file_path = os.path.join(
                args.output_path,
                deployment_name,
                filename
            )

            profile = profiles[profiles[:, 2] == profile_id]

            # NOTE: Store 1 based profile id
            init_netcdf(file_path, attrs, args.segment_id, profile_id + 1)
            profile = profiles[profiles[:, 2] == profile_id]
            profile_end = max(profile[:, 0])

        with open_glider_netcdf(file_path, 'a') as glider_nc:
            while line['timestamp'] <= profile_end:
                line = fill_gps(line, interp_gps, args.time, args.gps_prefix)
                glider_nc.stream_dict_insert(line)
                try:
                    line = reader.__next__()
                except StopIteration:
                    break

            # Handle UV Variables
            if glider_nc.contains('time_uv'):
                uv_values = backfill_uv_variables(
                    glider_nc, empty_uv_processed_paths
                )
            elif uv_values is not None:
                fill_uv_variables(glider_nc, uv_values)
                del empty_uv_processed_paths[:]
            else:
                empty_uv_processed_paths.append(file_path)

            glider_nc.update_profile_vars()
            try:
                glider_nc.calculate_salinity()
                glider_nc.calculate_density()
            except BaseException:
                logger.error('{}: '.format(
                    'Could not compute salinity or density')
                )

        profile_id += 1

    return 0


def main():
    parser = create_arg_parser()
    args = parser.parse_args()

    # Check filenames
    if args.flight is None and args.science is None:
        raise ValueError('Must specify flight, science or both paths')

    if args.flight is not None and args.science is not None:
        flight_prefix = os.path.split(args.flight)[1].rsplit('.')[0]
        science_prefix = os.path.split(args.science)[1].rsplit('.')[0]
        if flight_prefix != science_prefix:
            raise ValueError('Flight and science file names must match')

    # Fill in segment ID
    if args.segment_id is None:
        args.segment_id = find_segment_id(args.flight, args.science)

    attrs = read_attrs(args.glider_config_path)

    return process_dataset(args, attrs)


if __name__ == '__main__':
    sys.exit(main())
