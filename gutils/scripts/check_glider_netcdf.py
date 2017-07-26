#!python
# coding=utf-8
import os
import sys
import json
import tempfile
import argparse

from compliance_checker.runner import ComplianceChecker, CheckSuite

import logging
L = logging.getLogger('gutils.nc')


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


def create_arg_parser():
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


def main():
    parser = create_arg_parser()
    args = parser.parse_args()

    # Check filenames
    if args.file is None:
        raise ValueError('Must specify path to NetCDF file')

    # If running on command line, add a console handler
    L.addHandler(logging.StreamHandler())

    return check_dataset(args)


if __name__ == '__main__':
    sys.exit(main())
