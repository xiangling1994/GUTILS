#!python
# coding=utf-8
import os
import sys
import argparse
from ftplib import FTP
from collections import namedtuple

import netCDF4 as nc4
from pyinotify import (
    IN_CLOSE_WRITE,
    IN_MOVED_TO,
    Notifier,
    NotifierError,
    ProcessEvent,
    WatchManager
)

from gutils import setup_cli_logger
from gutils.nc import check_dataset

import logging
L = logging.getLogger(__name__)


class Netcdf2FtpProcessor(ProcessEvent):

    def my_init(self, ftp_url, ftp_user, ftp_pass):
        self.ftp_url = ftp_url
        self.ftp_user = ftp_user
        self.ftp_pass = ftp_pass

    def process_IN_CLOSE(self, event):
        f = namedtuple('Check_Arguments', ['file'])
        args = f(file=event.pathname)
        if self.valid_extension(event.name) and check_dataset(args) == 0:
            self.upload_file(event)

    def process_IN_MOVED_TO(self, event):
        f = namedtuple('Check_Arguments', ['file'])
        args = f(file=event.pathname)
        if self.valid_extension(event.name) and check_dataset(args) == 0:
            self.upload_file(event)

    def valid_extension(self, name):
        _, ext = os.path.splitext(name)
        if ext in ['.nc', 'nc4']:
            return True

        L.error('Unrecognized file extension: {}'.format(ext))
        return False

    def upload_file(self, event):
        ftp = None
        try:
            ftp = FTP(self.ftp_url)
            ftp.login(self.ftp_user, self.ftp_pass)

            with nc4.Dataset(event.pathname) as ncd:
                if not hasattr(ncd, 'id'):
                    raise ValueError("No 'id' global attribute")
                # Change into the correct deployment directory
                try:
                    ftp.cwd(ncd.id)
                except BaseException:
                    ftp.mkd(ncd.id)
                    ftp.cwd(ncd.id)

            with open(event.pathname, 'rb') as fp:
                # Upload NetCDF file
                ftp.storbinary(
                    'STOR {}'.format(event.name),
                    fp
                )
                L.info("Uploaded file: {}".format(event.name))

        except BaseException as e:
            L.error('Could not upload: {} to {}. {}.'.format(event.pathname, self.ftp_url, e))

        finally:
            if ftp is not None:
                ftp.quit()


def create_arg_parser():

    parser = argparse.ArgumentParser(
        description="Monitor a directory for new netCDF glider data and "
                    "upload the netCDF files to an FTP site."
    )
    parser.add_argument(
        "-d",
        "--data_path",
        help="Path to the glider data netCDF output directory",
        default=os.environ.get('GUTILS_NETCDF_DIRECTORY')
    )
    parser.add_argument(
        "--ftp_url",
        help="Path to the glider data netCDF output directory",
        default=os.environ.get('GUTILS_FTP_URL')
    )
    parser.add_argument(
        "--ftp_user",
        help="FTP username, defaults to 'anonymous'",
        default=os.environ.get('GUTILS_FTP_USER', 'anonymous')
    )
    parser.add_argument(
        "--ftp_pass",
        help="FTP password, defaults to an empty string",
        default=os.environ.get('GUTILS_FTP_PASS', '')
    )
    parser.add_argument(
        "--daemonize",
        help="To daemonize or not to daemonize",
        type=bool,
        default=False
    )

    return parser


def main():
    setup_cli_logger(logging.INFO)

    parser = create_arg_parser()
    args = parser.parse_args()

    if not args.data_path:
        L.error("Please provide an --data_path agrument or set the "
                "GUTILS_NETCDF_DIRECTORY environmental variable")
        sys.exit(parser.print_usage())

    if not args.ftp_url:
        L.error("Please provide an --ftp_url agrument or set the "
                "GUTILS_FTP_URL environmental variable")
        sys.exit(parser.print_usage())

    wm = WatchManager()
    mask = IN_MOVED_TO | IN_CLOSE_WRITE
    wm.add_watch(
        args.data_path,
        mask,
        rec=True,
        auto_add=True
    )

    processor = Netcdf2FtpProcessor(
        ftp_url=args.ftp_url,
        ftp_user=args.ftp_user,
        ftp_pass=args.ftp_pass,
    )
    notifier = Notifier(wm, processor, read_freq=10)  # Read every 10 seconds
    # Enable coalescing of events. This merges event types of the same type on the same file
    # together over the `read_freq` specified in the Notifier.
    notifier.coalesce_events()

    try:
        L.info("Watching {} and Uploading to {}".format(
            args.data_path,
            args.ftp_url)
        )
        notifier.loop(daemonize=args.daemonize)
    except NotifierError:
        L.exception('Unable to start notifier loop')
        return 1
    except BaseException as e:
        L.exception(e)
        return 1

    L.info("GUTILS netcdf_to_ftp Exited Successfully")
    return 0
