#!python
# coding=utf-8
import os
import sys
import argparse

from pyinotify import (
    IN_CLOSE_WRITE,
    IN_MOVED_TO,
    Notifier,
    NotifierError,
    ProcessEvent,
    WatchManager
)

from gutils import setup_cli_logger
from gutils.nc import create_dataset
from gutils.slocum import SlocumReader

import logging
L = logging.getLogger(__name__)


class Ascii2NetcdfProcessor(ProcessEvent):

    def my_init(self, outputs_path, configs_path, subset, template, **filters):
        self.outputs_path = outputs_path
        self.configs_path = configs_path
        self.subset = subset
        self.template = template
        self.filters = filters

    def valid_file(self, name):
        _, extension = os.path.splitext(name)
        if extension.lower() in self.VALID_EXTENSIONS:
            return True
        return False

    def process_IN_CLOSE(self, event):
        if self.valid_file(event.name):
            self.convert_to_netcdf(event)

    def process_IN_MOVED_TO(self, event):
        if self.valid_file(event.name):
            self.convert_to_netcdf(event)


class Slocum2NetcdfProcessor(Ascii2NetcdfProcessor):

    VALID_EXTENSIONS = ['.dat']

    def my_init(self, *args, **kwargs):
        super(Slocum2NetcdfProcessor, self).my_init(*args, **kwargs)

    def convert_to_netcdf(self, event):
        glider_folder_name = os.path.basename(event.path)
        glider_config_folder = os.path.join(self.configs_path, glider_folder_name)
        if not os.path.isdir(glider_config_folder):
            L.error("Config folder {} not found!".format(glider_config_folder))
            return

        glider_output_folder = os.path.join(self.outputs_path, glider_folder_name)

        create_dataset(
            file=event.pathname,
            reader_class=SlocumReader,
            config_path=glider_config_folder,
            output_path=glider_output_folder,
            subset=self.subset,
            template=self.template,
            **self.filters
        )


def create_netcdf_arg_parser():

    parser = argparse.ArgumentParser(
        description="Monitor a directory for new ASCII glider data and outputs NetCDF."
    )
    parser.add_argument(
        '-d',
        '--data_path',
        help='Path to ASCII glider data directory',
        default=os.environ.get('GUTILS_ASCII_DIRECTORY')
    )
    parser.add_argument(
        '-r',
        '--reader_class',
        help='Glider reader to interpret the data',
        default='slocum'
    )
    parser.add_argument(
        '-o',
        '--outputs',
        help='Where to place the newly generated NetCDF files.',
        default=os.environ.get('GUTILS_NETCDF_DIRECTORY')
    )
    parser.add_argument(
        '-c',
        '--configs',
        help="Folder to look for NetCDF global and glider "
             "JSON configuration files.  Default is './config'.",
        default=os.environ.get('GUTILS_CONFIG_DIRECTORY', './config')
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
    parser.add_argument(
        "-t",
        "--template",
        help="The template to use when writing netCDF files. Options: [filepath], trajectory, ioos_ngdac",
        default=os.environ.get('GUTILS_NETCDF_TEMPLATE', 'trajectory')
    )
    parser.add_argument(
        "--daemonize",
        help="To daemonize or not to daemonize",
        type=bool,
        default=False
    )
    parser.set_defaults(subset=True)

    return parser


def main_to_netcdf():
    setup_cli_logger(logging.INFO)

    parser = create_netcdf_arg_parser()
    args = parser.parse_args()

    filter_args = vars(args)
    # Remove non-filter args into positional arguments
    data_path = filter_args.pop('data_path')
    configs = filter_args.pop('configs')
    outputs = filter_args.pop('outputs')
    subset = filter_args.pop('subset')
    daemonize = filter_args.pop('daemonize')
    template = filter_args.pop('template')

    # Move reader_class to a class
    reader_class = filter_args.pop('reader_class')
    if reader_class == 'slocum':
        reader_class = SlocumReader

    if not data_path:
        L.error("Please provide a --data_path agrument or set the "
                "GUTILS_ASCII_DIRECTORY environmental variable")
        sys.exit(parser.print_usage())

    # Add inotify watch
    wm = WatchManager()
    mask = IN_MOVED_TO | IN_CLOSE_WRITE
    wm.add_watch(
        data_path,
        mask,
        rec=True,
        auto_add=True
    )

    # Convert ASCII data to NetCDF using a specific reader class
    if reader_class == SlocumReader:
        processor = Slocum2NetcdfProcessor(
            outputs_path=outputs,
            configs_path=configs,
            subset=subset,
            template=template,
            **filter_args
        )
    notifier = Notifier(wm, processor, read_freq=10)
    # Enable coalescing of events. This merges event types of the same type on the same file
    # together over the `read_freq` specified in the Notifier.
    notifier.coalesce_events()

    try:
        L.info("Watching {} and Outputting NetCDF to {}".format(
            data_path,
            outputs)
        )
        notifier.loop(daemonize=daemonize)
    except NotifierError:
        L.exception('Unable to start notifier loop')
        return 1

    L.info("GUTILS ascii_to_netcdf Exited Successfully")
    return 0
