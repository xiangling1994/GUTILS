#!python
# coding=utf-8
from __future__ import division

import os
import sys
import math
import errno
import shutil
import argparse
import tempfile
from datetime import datetime

import numpy as np

from gutils.yo import assign_profiles
from gutils.yo.filters import (
    filter_profile_depth,
    filter_profile_timeperiod,
    filter_profile_distance,
    filter_profile_number_of_points
)

from gutils.slocum import SlocumReader

import netCDF4 as nc4
from pocean.utils import dict_update, get_fill_value
from pocean.meta import MetaInterface
from pocean.dsg.trajectory.im import IncompleteMultidimensionalTrajectory

import logging
L = logging.getLogger('gutils.nc')


PROFILE_MEDIAN = 0
PROFILE_MINIMUM = 1


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
    one = dict_update(defaults, ins)
    two = dict_update(one, deps)
    return two


def set_profile_data(ncd, profile, method=None):

    if method is None:
        method = 0

    # Skipping profile_ variables for now
    prof_t = ncd.variables['profile_time']
    prof_y = ncd.variables['profile_lat']
    prof_x = ncd.variables['profile_lon']

    if method == PROFILE_MEDIAN:
        # T,X,Y: MIDDLE INDEX (median)
        amedian = np.nanmedian(profile.y.values)
        middle_index = np.nanargmin(np.abs(profile.y.values - amedian))
        prof_t[:] = nc4.date2num(
            profile.t.iloc[middle_index].to_pydatetime(),
            units=prof_t.units,
            calendar=getattr(prof_t, 'calendar', 'standard')
        )
        prof_y[:] = profile.y.iloc[middle_index] or prof_y.fill_value
        prof_x[:] = profile.x.iloc[middle_index] or prof_x.fill_value

    elif method == PROFILE_MINIMUM:
        # T: MIN
        # X,Y: AVERAGE
        prof_t_min = nc4.date2num(
            profile.t.min().to_pydatetime(),
            units=prof_t.units,
            calendar=getattr(prof_t, 'calendar', 'standard')
        )
        prof_t[:] = prof_t_min

        prof_y_avg = profile.y.mean()
        if math.isnan(prof_y_avg):
            prof_y_avg = get_fill_value(prof_y)
        prof_y[:] = prof_y_avg

        prof_x_avg = profile.x.mean()
        if math.isnan(prof_x_avg):
            prof_x_avg = get_fill_value(prof_x)
        prof_x[:] = prof_x_avg

    ncd.sync()


def set_uv_data(ncd, profile):
    # The uv index should be the second row where v (originally m_water_vx) is not null
    uv_t = ncd.variables['time_uv']
    uv_x = ncd.variables['lon_uv']
    uv_y = ncd.variables['lat_uv']

    # Find the second row where U and V are not null
    uvslice = (~profile.u.isnull()) & (~profile.v.isnull())
    uv_index = profile[uvslice].index[:2]
    if uv_index.size == 0:
        uv_t[:] = get_fill_value(uv_t)
        uv_y[:] = get_fill_value(uv_y)
        uv_x[:] = get_fill_value(uv_x)
    else:
        uv_index = uv_index[-1]
        uv_t_first = nc4.date2num(
            profile.t.loc[uv_index].to_pydatetime(),
            units=uv_t.units,
            calendar=getattr(uv_t, 'calendar', 'standard')
        )
        if math.isnan(uv_t_first):
            uv_t_first = get_fill_value(uv_t)
        uv_t[:] = uv_t_first

        uv_y_first = profile.y.loc[uv_index]
        if math.isnan(uv_y_first):
            uv_y_first = get_fill_value(uv_x)
        uv_y[:] = uv_y_first

        uv_x_first = profile.x.loc[uv_index]
        if math.isnan(uv_x_first):
            uv_x_first = get_fill_value(uv_x)
        uv_x[:] = uv_x_first
    ncd.sync()


def update_geographic_attributes(ncd, profile):
    miny = profile.y.min().round(5)
    maxy = profile.y.max().round(5)
    minx = profile.x.min().round(5)
    maxx = profile.y.max().round(5)
    ncd.setncattr('geospatial_lat_min', miny)
    ncd.setncattr('geospatial_lat_max', maxy)
    ncd.setncattr('geospatial_lon_min', minx)
    ncd.setncattr('geospatial_lon_max', maxx)

    polygon_wkt = 'POLYGON ((' \
        '{maxy:.6f} {minx:.6f}, '  \
        '{maxy:.6f} {maxx:.6f}, '  \
        '{miny:.6f} {maxx:.6f}, '  \
        '{miny:.6f} {minx:.6f}, '  \
        '{maxy:.6f} {minx:.6f}'    \
        '))'.format(
            miny=miny,
            maxy=maxy,
            minx=minx,
            maxx=maxx
        )
    ncd.setncattr('geospatial_bounds', polygon_wkt)


def update_vertical_attributes(ncd, profile):
    ncd.setncattr('geospatial_vertical_min', profile.z.min().round(6))
    ncd.setncattr('geospatial_vertical_max', profile.z.max().round(6))
    ncd.setncattr('geospatial_vertical_units', 'm')


def update_temporal_attributes(ncd, profile):
    mint = profile.t.min()
    maxt = profile.t.max()
    ncd.setncattr('time_coverage_start', mint.strftime('%Y-%m-%dT%H:%M:%SZ'))
    ncd.setncattr('time_coverage_end', maxt.strftime('%Y-%m-%dT%H:%M:%SZ'))
    ncd.setncattr('time_coverage_duration', (maxt - mint).isoformat())


def update_creation_attributes(ncd, profile):
    nc_create_ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    ncd.setncattr('date_created', nc_create_ts)
    ncd.setncattr('date_issued', nc_create_ts)
    ncd.setncattr('date_modified', nc_create_ts)

    ncd.history = '{} - {}'.format(
        nc_create_ts,
        'Created with the GUTILS package: "{}"'.format(sys.argv[0])
    )


def create_netcdf(attrs, data, output_path, mode):
    # Create NetCDF Files for Each Profile
    for pi, profile in data.groupby('profile_id'):
        try:
            # Path to hold file while we create it
            tmp_handle, tmp_path = tempfile.mkstemp(suffix='.nc', prefix='gutils_glider_netcdf_')

            # Create final filename
            begin_time = profile.t.dropna().iloc[0]
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

                # BEFORE RENAMING VARIABLES
                # Calculate some geographic global attributes
                update_geographic_attributes(ncd, profile)

                # Calculate some vertical global attributes
                update_vertical_attributes(ncd, profile)

                # Calculate some temporal global attributes
                update_temporal_attributes(ncd, profile)

                # Set the creation dates and history
                update_creation_attributes(ncd, profile)

                # Standardize some variable names before applying the metadata dict
                ncd.renameVariable('latitude', 'lat')
                ncd.renameVariable('longitude', 'lon')
                ncd.renameVariable('z', 'depth')

                # AFTER RENAMING VARIABLES
                # Load metadata from config files to create the skeleton
                # This will create scalar variables but not assign the data
                ncd.__apply_meta_interface__(attrs)

                # Set trajectory value
                ncd.id = traj_name
                ncd.variables['trajectory'][0] = traj_name

                # TODO: Calculate bounds variables and attributes?

                # Set profile_* data
                set_profile_data(ncd, profile, method=PROFILE_MEDIAN)

                # Set *_uv data
                set_uv_data(ncd, profile)

            # Move to final destination
            try:
                os.makedirs(os.path.dirname(output_file))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
            shutil.move(tmp_path, output_file)
            L.debug('Created: {}'.format(output_file))
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
            L.info(
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
        L.exception('{} - Skipping'.format(e))
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

    # If running on command line, add a console handler
    L.addHandler(logging.StreamHandler())

    return process_dataset(args)


if __name__ == '__main__':
    sys.exit(main())
