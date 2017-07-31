#!python
# coding=utf-8
from __future__ import division

import os
import sys
import json
import math
import errno
import shutil
import argparse
import tempfile
from datetime import datetime

import netCDF4 as nc4
from compliance_checker.runner import ComplianceChecker, CheckSuite
from pocean.utils import dict_update, get_fill_value
from pocean.meta import MetaInterface
from pocean.dsg.trajectory.im import IncompleteMultidimensionalTrajectory

from gutils import get_uv_data, get_profile_data
from gutils.filters import process_dataset
from gutils.slocum import SlocumReader

import logging
L = logging.getLogger(__name__)


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
    default_attrs_path = os.path.join(
        os.path.dirname(__file__),
        'templates',
        'trajectory.json'
    )
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


def set_scalar_value(value, ncvar):
    if value is None or math.isnan(value):
        ncvar[:] = get_fill_value(ncvar)
    else:
        ncvar[:] = value


def set_profile_data(ncd, profile, method=None):
    prof_t = ncd.variables['profile_time']
    prof_y = ncd.variables['profile_lat']
    prof_x = ncd.variables['profile_lon']

    txy = get_profile_data(profile, method=method)

    t_value = txy.t
    if isinstance(t_value, datetime):
        t_value = nc4.date2num(
            t_value,
            units=prof_t.units,
            calendar=getattr(prof_t, 'calendar', 'standard')
        )
    set_scalar_value(t_value, prof_t)
    set_scalar_value(txy.y, prof_y)
    set_scalar_value(txy.x, prof_x)

    ncd.sync()


def set_uv_data(ncd, profile):
    # The uv index should be the second row where v (originally m_water_vx) is not null
    uv_t = ncd.variables['time_uv']
    uv_x = ncd.variables['lon_uv']
    uv_y = ncd.variables['lat_uv']

    txy = get_uv_data(profile)

    t_value = txy.t
    if isinstance(t_value, datetime):
        t_value = nc4.date2num(
            t_value,
            units=uv_t.units,
            calendar=getattr(uv_t, 'calendar', 'standard')
        )
    set_scalar_value(t_value, uv_t)
    set_scalar_value(txy.y, uv_y)
    set_scalar_value(txy.x, uv_x)

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

                # Set profile_* data
                set_profile_data(ncd, profile)

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


def create_dataset(args):
    try:
        dict_args = vars(args)  # argparser
    except TypeError:
        dict_args = args._asdict()  # namedtuple

    glider_config_path = dict_args.pop('glider_config_path')
    output_path = dict_args.pop('output_path')

    # TODO: Support additional Reader classes if needed
    dict_args['reader_class'] = SlocumReader

    processed_df, mode = process_dataset(**dict_args)

    attrs = read_attrs(glider_config_path)
    return create_netcdf(attrs, processed_df, output_path, mode)


def main_create():
    # If running on command line, add a console handler
    L.addHandler(logging.StreamHandler())

    parser = create_arg_parser()
    args = parser.parse_args()

    return create_dataset(args)


# CHECKER

def check_dataset(args):
    check_suite = CheckSuite()
    check_suite.load_all_available_checkers()

    outhandle, outfile = tempfile.mkstemp()

    try:
        return_value, errors = ComplianceChecker.run_checker(
            ds_loc=args.file,
            checker_names=['gliderdac'],
            verbose=True,
            criteria='normal',
            output_format='json',
            output_filename=outfile
        )
        assert errors is False
        return 0
    except AssertionError:
        with open(outfile, 'rt') as f:
            ers = json.loads(f.read())
            for k, v in ers.items():
                if isinstance(v, list):
                    for x in v:
                        if 'msgs' in x and x['msgs']:
                            L.debug(x['msgs'])
        return 1
    except BaseException as e:
        L.warning(e)
        return 1
    finally:
        os.close(outhandle)
        if os.path.isfile(outfile):
            os.remove(outfile)


def check_arg_parser():
    parser = argparse.ArgumentParser(
        description='Verifies that a glider NetCDF file from a provider '
                    'contains all the required global attributes, dimensions,'
                    'scalar variables and dimensioned variables.'
    )

    parser.add_argument(
        'file',
        help='Path to Glider NetCDF file.'
    )
    return parser


def main_check():

    parser = check_arg_parser()
    args = parser.parse_args()

    # Check filenames
    if args.file is None:
        raise ValueError('Must specify path to NetCDF file')

    # If running on command line, add a console handler
    L.addHandler(logging.StreamHandler())

    return check_dataset(args)
