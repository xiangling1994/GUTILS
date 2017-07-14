#!python
# coding=utf-8
from __future__ import division

import os
import sys
import json
import shutil
import argparse
import tempfile

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
from pocean.utils import dict_update
from pocean.meta import MetaInterface
from pocean.dataset import EnhancedDataset
from pocean.dsg.trajectory.im import IncompleteMultidimensionalTrajectory

import logging
logger = logging.getLogger('gutils.nc')


def create_glider_filepath(attrs, begin_time, mode):
    glider_name = attrs['deployment']['glider']

    deployment_name = '{}-{}'.format(
        glider_name,
        attrs['deployment']['trajectory_date']
    )

    filename = "{}_{:%Y%m%dT%H%M%S}Z_{}.nc" % (
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


def create_netcdf(attrs, data, output_path, mode, segment_id):
    # Create NetCDF Files for Each Profile
    for pi, profile in data.groupby('profile'):
        try:
            # Path to hold file while we create it
            tmp_handle, tmp_path = tempfile.mkstemp(suffix='.nc', prefix='gutils_glider_netcdf_')

            # Create final filename
            begin_time = profile.iloc[0].t
            relative_file = create_glider_filepath(attrs, begin_time, mode)
            output_file = os.path.join(output_path, relative_file)

            # Add in the trajectory dimension to make pocean happy
            traj_name = os.path.basename(os.path.dirname(output_file))
            profile['trajectory'] = traj_name

            # Use pocean to create NetCDF file
            im = IncompleteMultidimensionalTrajectory.from_dataframe(profile, tmp_path)
            im.close()

            # Remove the trajectory dimension
            nco = Nco()
            options = [
                '-h'  # no history
                '-a trajectory'  # average over dimension size of 1 will remove it
            ]
            nco.ncwa(input=tmp_path, output=tmp_path, options=options)

            # Open back up
            with EnhancedDataset(tmp_path, 'a') as ncd:
                # Load metadata from config files to create the skeleton
                # This will create scalar variables but not assign the data
                ncd.__apply_meta_interface__(attrs)

                # Set the values of any scalar variables
                ncd.variables['profile_id'][:] = pi

                # We set the profile time/lat/lon to the middle index
                middle_index = len(profile) // 2
                ncd.variables['profile_time'][:] = profile.t.iloc[middle_index]
                ncd.variables['profile_lat'][:] = profile.y.iloc[middle_index]
                ncd.variables['profile_lon'][:] = profile.x.iloc[middle_index]

                # The uv index is the first row where v (originally m_water_vx) is not null
                uv_index = profile.v[profile.v.notnull()].iloc[0].index
                ncd.variables['time_uv'][:] = profile.t.iloc[uv_index]
                ncd.variables['lat_uv'][:] = profile.y.iloc[uv_index]
                ncd.variables['lon_uv'][:] = profile.x.iloc[uv_index]

                # Set trajectory value
                ncd.variables['trajectory'][:] = traj_name

                # Set platform value
                ncd.variables['platform'] = traj_name

                ncd.variables['segment_id'] = segment_id

                # TODO: Calculate bounds variables and attributes?

            # Move to final destination
            shutil.move(tmp_path, output_file)
            logger.info('Created: {}'.format(output_file))
        finally:
            os.close(tmp_handle)
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


def process_dataset(args):

    attrs = read_attrs(args.glider_config_path)
    ascii_path = args.file

    try:
        sr = SlocumReader(ascii_path)
        sr.standardize()
        data = sr.data

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

        # TODO: Backfill U/V?
        # TODO: Backfill X/Y?

    except ValueError as e:
        logger.exception('{} - Skipping'.format(e))
        return 1

    return create_netcdf(attrs, filtered, args.output_path, sr.mode, args.segment_id)


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
        '--segment_id',
        nargs=1,
        help='Set the segment ID',
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

    # Fill in segment ID
    if args.segment_id is None:
        args.segment_id = parse_glider_filename(args.file)['segment']

    return process_dataset(args)


if __name__ == '__main__':
    sys.exit(main())
