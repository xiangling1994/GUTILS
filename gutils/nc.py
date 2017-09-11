#!python
# coding=utf-8
from __future__ import division

import os
import sys
import json
import math
import shutil
import argparse
import calendar
import tempfile
from datetime import datetime
from collections import OrderedDict

import netCDF4 as nc4
from compliance_checker.runner import ComplianceChecker, CheckSuite
from pocean.utils import dict_update, get_fill_value
from pocean.meta import MetaInterface
from pocean.dsg.trajectory.im import IncompleteMultidimensionalTrajectory

from gutils import get_uv_data, get_profile_data, safe_makedirs, setup_cli_logger
from gutils.filters import process_dataset
from gutils.slocum import SlocumReader

import logging
L = logging.getLogger(__name__)


def read_attrs(config_path):

    def cfg_file(name):
        return os.path.join(
            config_path,
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


def set_profile_data(ncd, profile, profile_index, method=None):
    prof_t = ncd.variables['profile_time']
    prof_y = ncd.variables['profile_lat']
    prof_x = ncd.variables['profile_lon']
    prof_id = ncd.variables['profile_id']

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
    set_scalar_value(profile_index, prof_id)

    ncd.sync()


def set_uv_data(ncd, profile):
    # The uv index should be the second row where v (originally m_water_vx) is not null
    uv_t = ncd.variables['time_uv']
    uv_x = ncd.variables['lon_uv']
    uv_y = ncd.variables['lat_uv']
    uv_u = ncd.variables['u']
    uv_v = ncd.variables['v']

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
    set_scalar_value(txy.u, uv_u)
    set_scalar_value(txy.v, uv_v)

    ncd.sync()


def update_geographic_attributes(ncd, profile):
    miny = profile.y.min().round(5)
    maxy = profile.y.max().round(5)
    minx = profile.x.min().round(5)
    maxx = profile.x.max().round(5)
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

            profile_time = profile.t.dropna().iloc[0]
            profile_index = calendar.timegm(profile_time.utctimetuple())
            # Create final filename
            filename = "{0}_{1:%Y%m%dT%H%M%S}Z_{2}_{3}.nc".format(
                attrs['glider'],
                profile_time,
                profile_index,
                mode
            )
            output_file = os.path.join(output_path, filename)

            # We are using the epoch as the profile_index!
            # # Get all existing netCDF outputs and find out the index of this netCDF file. That
            # # will be the profile_id of this file. This is effectively keeping a tally of netCDF
            # # files that have been created and only works if NETCDF FILES ARE WRITTEN IN
            # # ASCENDING ORDER
            # netcdf_files_same_mode = list(glob(
            #     os.path.join(
            #         output_path,
            #         '*_{}.nc'.format(mode)
            #     )
            # ))
            # netcdf_files_same_mode = np.asarray(netcdf_files_same_mode)
            # profile_index = np.searchsorted(netcdf_files_same_mode, filename)

            # Add in the trajectory dimension to make pocean happy
            traj_name = '{}-{}'.format(
                attrs['glider'],
                attrs['trajectory_date']
            )
            profile = profile.assign(trajectory=traj_name)

            # We add this back in later as seconds since epoch
            profile.drop('profile_id', axis=1, inplace=True)

            axis_names = {
                't': 'time',
                'z': 'depth',
                'x': 'lon',
                'y': 'lat'
            }

            # Use pocean to create NetCDF file
            with IncompleteMultidimensionalTrajectory.from_dataframe(
                    profile,
                    tmp_path,
                    axis_names=axis_names,
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

                # AFTER RENAMING VARIABLES
                # Load metadata from config files to create the skeleton
                # This will create scalar variables but not assign the data

                # We only want to apply metadata from the `attrs` map if the variable is already in
                # the netCDF file or it is a scalar variable (no shape defined). This avoids
                # creating measured variables that were not measured in this profile.
                prof_attrs = attrs.copy()
                vars_to_update = OrderedDict()
                for vname, vobj in prof_attrs['variables'].items():
                    if vname in ncd.variables or ('shape' not in vobj and 'type' in vobj):
                        if 'shape' in vobj:
                            # Assign coordinates
                            vobj['attributes']['coordinates'] = '{} {} {} {}'.format(
                                axis_names.get('t'),
                                axis_names.get('z'),
                                axis_names.get('x'),
                                axis_names.get('y'),
                            )
                        vars_to_update[vname] = vobj
                    else:
                        # L.debug("Skipping missing variable: {}".format(vname))
                        pass

                prof_attrs['variables'] = vars_to_update
                ncd.__apply_meta_interface__(prof_attrs)

                # Set trajectory value
                ncd.id = traj_name
                ncd.variables['trajectory'][0] = traj_name

                # Set profile_* data
                set_profile_data(ncd, profile, profile_index)

                # Set *_uv data
                set_uv_data(ncd, profile)

            # Move to final destination
            safe_makedirs(os.path.dirname(output_file))
            os.chmod(tmp_path, 0o664)
            shutil.move(tmp_path, output_file)
            L.info('Created: {}'.format(output_file))
        except BaseException as e:
            L.exception('Error: {}'.format(e))
            continue
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
        'file',
        help="Combined ASCII file to process into NetCDF"
    )
    parser.add_argument(
        'config_path',
        help='Path to configuration files for this specific glider deployment.'
    )
    parser.add_argument(
        'output_path',
        help='Path to folder to save NetCDF output. A directory named after '
             'the deployment will be created here'
    )
    parser.add_argument(
        "-r",
        "--reader_class",
        help="Glider reader to interpret the data",
        default='slocum'
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


def create_dataset(file, reader_class, config_path, output_path, subset, **filters):

    processed_df, mode = process_dataset(file, reader_class, **filters)

    if processed_df is None:
        return 1

    attrs = read_attrs(config_path)

    # Optionally, remove any variables from the dataframe that do not have metadata assigned
    if subset is True:
        all_columns = set(processed_df.columns)
        reserved_columns = [
            'trajectory',
            'profile',
            't',
            'x',
            'y',
            'z',
        ]
        removable_columns = all_columns - set(reserved_columns)
        orphans = removable_columns - set(attrs.get('variables', {}).keys())
        L.info(
            "Excluded from output (absent from JSON config):\n  * {}".format('\n  * '.join(orphans))
        )
        processed_df = processed_df.drop(orphans, axis=1)

    return create_netcdf(attrs, processed_df, output_path, mode)


def main_create():
    setup_cli_logger(logging.INFO)

    parser = create_arg_parser()
    args = parser.parse_args()

    filter_args = vars(args)
    # Remove non-filter args into positional arguments
    file = filter_args.pop('file')
    config_path = filter_args.pop('config_path')
    output_path = filter_args.pop('output_path')
    subset = filter_args.pop('subset')

    # Move reader_class to a class
    reader_class = filter_args.pop('reader_class')
    if reader_class == 'slocum':
        reader_class = SlocumReader

    return create_dataset(
        file=file,
        reader_class=reader_class,
        config_path=config_path,
        output_path=output_path,
        subset=subset,
        **filter_args
    )


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
                            L.warning('{} - {}'.format(args.file, x['msgs']))
        return 1
    except BaseException as e:
        L.warning('{} - {}'.format(args.file, e))
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
    setup_cli_logger(logging.INFO)

    parser = check_arg_parser()
    args = parser.parse_args()

    # Check filenames
    if args.file is None:
        raise ValueError('Must specify path to NetCDF file')

    return check_dataset(args)
