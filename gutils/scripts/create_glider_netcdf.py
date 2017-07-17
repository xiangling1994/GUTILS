#!python
# coding=utf-8
from __future__ import division

import os
import sys
import math
import shutil
import argparse
import tempfile

import pandas as pd

from gutils import parse_glider_filename
from gutils.yo import assign_profiles
from gutils.yo.filters import (
    filter_profile_depth,
    filter_profile_timeperiod,
    filter_profile_distance,
    filter_profile_number_of_points
)

from gutils.slocum import SlocumReader

from nco import Nco
import netCDF4 as nc4
from pocean.utils import dict_update, get_fill_value
from pocean.meta import MetaInterface
from pocean.dataset import EnhancedDataset
from pocean.dsg.trajectory.im import IncompleteMultidimensionalTrajectory

import logging
logger = logging.getLogger('gutils.nc')


def create_glider_filepath(attrs, begin_time, mode):
    glider_name = attrs['glider']

    deployment_name = '{}-{}'.format(
        glider_name,
        attrs['trajectory_date']
    )

    filename = "{}_{:%Y%m%dT%H%M%S}Z_{}.nc".format(
        glider_name,
        begin_time,
        mode
    )

    return os.path.join(
        deployment_name,
        filename
    )


def read_attrs(glider_config_path):

    def cfg_file(name):
        return os.path.join(
            glider_config_path,
            name
        )

    # Load in configurations
    default_attrs_path = os.path.join(os.path.dirname(__file__), '..', 'trajectory_template.json')
    defaults = dict(MetaInterface.from_jsonfile(default_attrs_path))

    # Load global attributes
    globs = {}
    global_attrs_path = cfg_file("global_attributes.json")
    if os.path.isfile(global_attrs_path):
        globs = dict(MetaInterface.from_jsonfile(global_attrs_path))

    # Load instruments
    ins = {}
    ins_attrs_path = cfg_file("instruments.json")
    if os.path.isfile(ins_attrs_path):
        ins = dict(MetaInterface.from_jsonfile(ins_attrs_path))

    # Load deployment attributes (including some global attributes)
    deps = {}
    deps_attrs_path = cfg_file("deployment.json")
    if os.path.isfile(deps_attrs_path):
        deps = dict(MetaInterface.from_jsonfile(deps_attrs_path))

    # Update, highest precedence updates last
    one = dict_update(defaults, globs)
    two = dict_update(one, ins)
    three = dict_update(two, deps)
    return three


def set_profile_data(ncd, profile):
    # Skipping profile_ variables for now
    prof_t = ncd.variables['profile_time']
    prof_y = ncd.variables['profile_lat']
    prof_x = ncd.variables['profile_lon']

    # T,X,Y: MIDDLE INDEX
    # This should be changed so null values are taken into account
    # middle_index = len(profile) // 2
    # prof_t[:] = nc4.date2num(
    #     profile.t.iloc[middle_index].to_pydatetime(),
    #     units=prof_t.units,
    #     calendar=getattr(prof_t, 'calendar', 'standard')
    # )
    # prof_y[:] = profile.y.iloc[middle_index] or prof_y.fill_value
    # prof_x[:] = profile.x.iloc[middle_index] or prof_x.fill_value

    # T: MIN
    # X,Y: AVERAGE
    # prof_t_min = nc4.date2num(
    #     profile.t.min().to_pydatetime(),
    #     units=prof_t.units,
    #     calendar=getattr(prof_t, 'calendar', 'standard')
    # )
    # prof_t[:] = prof_t_min

    # prof_y_avg = profile.y.mean()
    # if math.isnan(prof_y_avg):
    #     prof_y_avg = get_fill_value(prof_y)
    # prof_y[:] = prof_y_avg

    # prof_x_avg = profile.x.mean()
    # if math.isnan(prof_x_avg):
    #     prof_x_avg = get_fill_value(prof_x)
    # prof_x[:] = prof_x_avg

    # ncd.sync()


def set_uv_data(ncd, profile):
    # The uv index is the first row where v (originally m_water_vx) is not null
    uv_t = ncd.variables['time_uv']
    uv_x = ncd.variables['lon_uv']
    uv_y = ncd.variables['lat_uv']

    # uv_index = profile.v.first_valid_index()
    # if uv_index is None:
    #     uv_t[:] = get_fill_value(uv_t)
    #     uv_y[:] = get_fill_value(uv_y)
    #     uv_x[:] = get_fill_value(uv_x)
    # else:
    #     uv_t = uv_t
    #     uv_t_first = nc4.date2num(
    #         profile.t.loc[uv_index].to_pydatetime(),
    #         units=uv_t.units,
    #         calendar=getattr(uv_t, 'calendar', 'standard')
    #     )
    #     if math.isnan(uv_t_first):
    #         uv_t_first = get_fill_value(uv_t)
    #     uv_t[:] = uv_t_first

    #     uv_y_first = profile.y.loc[uv_index]
    #     if math.isnan(uv_y_first):
    #         uv_y_first = get_fill_value(uv_x)
    #     uv_y[:] = uv_y_first

    #     uv_x_first = profile.x.loc[uv_index]
    #     if math.isnan(uv_x_first):
    #         uv_x_first = get_fill_value(uv_x)
    #     uv_x[:] = uv_x_first
    # ncd.sync()


def create_netcdf(attrs, data, output_path, mode):
    # Create NetCDF Files for Each Profile
    for pi, profile in data.groupby('profile_id'):
        try:
            # Path to hold file while we create it
            tmp_handle, tmp_path = tempfile.mkstemp(suffix='.nc', prefix='gutils_glider_netcdf_')

            # Create final filename
            begin_time = profile.iloc[0].t
            relative_file = create_glider_filepath(attrs, begin_time, mode)
            output_file = os.path.join(output_path, relative_file)

            # Add in the trajectory dimension to make pocean happy
            traj_name = os.path.basename(os.path.dirname(output_file))
            profile = profile.assign(trajectory=traj_name)

            # Use pocean to create NetCDF file
            with IncompleteMultidimensionalTrajectory.from_dataframe(
                    profile,
                    tmp_path,
                    reduce_dims=True,
                    mode='a') as ncd:

                ncd.renameVariable('latitude', 'lat')
                ncd.renameVariable('longitude', 'lon')
                ncd.renameVariable('z', 'depth')

                # Load metadata from config files to create the skeleton
                # This will create scalar variables but not assign the data
                ncd.__apply_meta_interface__(attrs)

                # Set trajectory value
                ncd.variables['trajectory'][0] = traj_name

                # TODO: Calculate bounds variables and attributes?

                # Set profile_* data
                set_profile_data(ncd, profile)

                # Set *_uv data
                set_uv_data(ncd, profile)
                ncd.sync()

                # ncd.renameVariable('t', 'drv_timestamp')
                # ncd.renameVariable('y', 'drv_m_gps_lat')
                # ncd.renameVariable('x', 'drv_m_gps_lon')
                # ncd.renameVariable('z', 'drv_depth')

            # Move to final destination
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            shutil.move(tmp_path, output_file)
            logger.debug('Created: {}'.format(output_file))
        finally:
            os.close(tmp_handle)
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


def process_dataset(args):

    attrs = read_attrs(args.glider_config_path)
    ascii_path = args.file

    try:
        sr = SlocumReader(ascii_path)
        data = sr.standardize()

        # Optionally, remove any variables from the dataframe that do not have metadata assigned
        if args.subset is True:
            orphans = set(data.columns) - set(attrs.get('variables', {}).keys())
            logger.info(
                "Excluded from output because there was no metadata: {}".format(orphans)
            )
            data = data.drop(orphans, axis=1)

        # Find profile breaks
        profiles = assign_profiles(data)

        # Filter data
        filtered = filter_profile_depth(profiles, below=args.filter_z)
        filtered = filter_profile_number_of_points(filtered, points_condition=args.filter_points)
        filtered = filter_profile_timeperiod(filtered, timespan_condition=args.filter_time)
        filtered = filter_profile_distance(filtered, distance_condition=args.filter_distance)

        # Downscale profile
        # filtered['profile'] = pd.to_numeric(filtered.profile, downcast='integer')
        filtered['profile'] = filtered.profile.astype('int32')
        # Profiles are 1-indexed, so add one to each
        filtered['profile'] = filtered.profile.values + 1
        # Rename the profile column to profile_id
        filtered = filtered.rename(columns={'profile': 'profile_id'})

        # TODO: Backfill U/V?
        # TODO: Backfill X/Y?

    except ValueError as e:
        logger.exception('{} - Skipping'.format(e))
        return 1

    return create_netcdf(attrs, filtered, args.output_path, sr.mode)


def create_arg_parser():
    parser = argparse.ArgumentParser(
        description='Parses a single combined ASCII file into a set of '
                    'NetCDFs file according to JSON configurations '
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
        '-f', '--file',
        help="Combined ASCII file to process into NetCDF",
        default=None
    )
    parser.add_argument(
        '-fp', '--filter_points',
        help="Filter out profiles that do not have at least this number of points",
        default=5
    )
    parser.add_argument(
        '-fd', '--filter_distance',
        help="Filter out profiles that do not span at least this vertical distance (meters)",
        default=1
    )
    parser.add_argument(
        '-ft', '--filter_time',
        help="Filter out profiles that last less than this numer of seconds",
        default=10
    )
    parser.add_argument(
        '-fz', '--filter_z',
        help="Filter out profiles that are not completely below this depth (meters)",
        default=1
    )
    parser.add_argument(
        '--no-subset',
        dest='subset',
        action='store_false',
        help='Process all variables - not just those available in a datatype mapping JSON file'
    )
    parser.set_defaults(subset=True)

    return parser


def main():
    parser = create_arg_parser()
    args = parser.parse_args()

    # Check filenames
    if args.file is None:
        raise ValueError('Must specify path to combined ASCII file')

    return process_dataset(args)


if __name__ == '__main__':
    sys.exit(main())
