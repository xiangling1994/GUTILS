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
from gutils.slocum import SlocumMerger

import logging
L = logging.getLogger(__name__)


class Binary2AsciiProcessor(ProcessEvent):

    def my_init(self, outputs_path, **kwargs):
        self.outputs_path = outputs_path

    def valid_file(self, name):
        _, extension = os.path.splitext(name)
        if extension.lower() in self.VALID_EXTENSIONS:
            return True
        return False

    def process_IN_CLOSE(self, event):
        if self.valid_file(event.name):
            self.convert_to_ascii(event)

    def process_IN_MOVED_TO(self, event):
        if self.valid_file(event.name):
            self.convert_to_ascii(event)


class Slocum2AsciiProcessor(Binary2AsciiProcessor):

    # (flight, science) file pairs
    PAIRS = {
        '.dbd': '.ebd',
        '.sbd': '.tbd',
        '.mbd': '.nbd'
    }

    @property
    def VALID_EXTENSIONS(self):
        # Only fire events for the FLIGHT files. The science file will be searched for but we don't
        # want to fire events for both flight AND science files to due race conditions down
        # the chain
        return self.PAIRS.keys()

    def my_init(self, *args, **kwargs):
        super(Slocum2AsciiProcessor, self).my_init(*args, **kwargs)

    def check_for_pair(self, event):

        base_name, extension = os.path.splitext(event.name)

        # Look for the other file and append to the final_pair if it exists
        # If we got this far we already know the extension is in self.PAIRS.keys()
        oext = self.PAIRS[extension.lower()]
        possible_files = [
            os.path.join(event.path, base_name + oext),
            os.path.join(event.path, base_name + oext.upper())
        ]
        for p in possible_files:
            if os.path.isfile(p):
                _, file_ext = os.path.splitext(p)
                return [event.name, base_name + file_ext]

    def convert_to_ascii(self, event):
        file_pairs = self.check_for_pair(event)

        # Create a folder inside of the output directory for this glider folder name.
        glider_folder_name = os.path.basename(event.path)
        outputs_folder = os.path.join(self.outputs_path, glider_folder_name)

        merger = SlocumMerger(
            event.path,
            outputs_folder,
            cache_directory=event.path,  # Default the cache directory to the data folder
            globs=file_pairs
        )
        merger.convert()


def create_ascii_arg_parser():

    parser = argparse.ArgumentParser(
        description="Monitor a directory for new binary glider data and outputs ASCII."
    )
    parser.add_argument(
        "-d",
        "--data_path",
        help="Path to binary glider data directory",
        default=os.environ.get('GUTILS_BINARY_DIRECTORY')
    )
    parser.add_argument(
        "-t",
        "--type",
        help="Glider type to interpret the data",
        default='slocum'
    )
    parser.add_argument(
        "-o",
        "--outputs",
        help="Where to place the newly generated ASCII files.",
        default=os.environ.get('GUTILS_ASCII_DIRECTORY')
    )
    parser.add_argument(
        "--daemonize",
        help="To daemonize or not to daemonize",
        type=bool,
        default=False
    )

    return parser


def main_to_ascii():
    setup_cli_logger(logging.WARNING)

    parser = create_ascii_arg_parser()
    args = parser.parse_args()

    if not args.data_path:
        L.error("Please provide a --data_path agrument or set the "
                "GUTILS_DATA_DIRECTORY environmental variable")
        sys.exit(parser.print_usage())

    wm = WatchManager()
    mask = IN_MOVED_TO | IN_CLOSE_WRITE
    wm.add_watch(
        args.data_path,
        mask,
        rec=True,
        auto_add=True
    )

    # Convert binary data to ASCII
    if args.type == 'slocum':
        processor = Slocum2AsciiProcessor(
            outputs_path=args.outputs
        )
    notifier = Notifier(wm, processor, read_freq=10)  # Read every 10 seconds
    # Enable coalescing of events. This merges event types of the same type on the same file
    # together over the `read_freq` specified in the Notifier.
    notifier.coalesce_events()

    try:
        L.info("Watching {} and Outputting ASCII to {}".format(
            args.data_path,
            args.outputs)
        )
        notifier.loop(daemonize=args.daemonize)
    except NotifierError:
        L.exception('Unable to start notifier loop')
        return 1

    L.info("GUTILS binary_to_ascii Exited Successfully")
    return 0
