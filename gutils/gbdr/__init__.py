#!/usr/bin/env python
import os
import shutil
from collections import OrderedDict
from glob import glob
from tempfile import mkdtemp

import pandas as pd

from gutils import generate_stream

import logging
L = logging.getLogger(__name__)


class MergedASCIIReader(object):

    def __init__(self, ascii_file):
        self.ascii_file = ascii_file
        self.metadata, self.headers, self.data = self.read()

    def read(self):
        metadata = OrderedDict()
        headers = None
        units = None
        with open(self.ascii_file, 'rt') as af:
            for li, al in enumerate(af):
                if 'm_present_time' in al:
                    headers = al.strip().split(' ')
                elif headers is not None:
                    units = al.strip().split(' ')
                    data_start = li + 1
                    break
                else:
                    title, value = al.split(':', maxsplit=1)
                    metadata[title.strip()] = value.strip()

        column_descriptions = OrderedDict()
        for head, unit in zip(headers, units):
            column_descriptions[head] = { 'units': unit }

        df = pd.read_csv(
            self.ascii_file,
            index_col=None,
            skiprows=data_start,
            header=None,
            names=headers,
            sep=' ',
            skip_blank_lines=True
        )
        return metadata, column_descriptions, df


class MergedASCIICreator(object):
    """
    Merges flight and science data files into an ASCII file.

    Copies files matching the regex in source_directory to their own temporary directory
    before processing since the Rutgers supported script only takes foldesr as input

    Returns a list of flight/science files that were processed into ASCII files
    """

    def __init__(self, source_directory, destination_directory, cache_directory=None, globs=None):

        globs = globs or []

        self.tmpdir = mkdtemp(prefix='gutils_convert_')
        self.matched_files = []
        self.cache_directory = cache_directory or source_directory
        self.destination_directory = destination_directory
        self.source_directory = source_directory

        for g in globs:
            self.matched_files += list(glob(
                os.path.join(
                    source_directory,
                    g
                )
            ))

    def __del__(self):
        # Remove tmpdir
        shutil.rmtree(self.tmpdir)

    def convert(self):
        # Copy to tempdir
        for f in self.matched_files:
            fname = os.path.basename(f)
            tmpf = os.path.join(self.tmpdir, fname)
            shutil.copy2(f, tmpf)

        if not os.path.isdir(self.destination_directory):
            os.makedirs(self.destination_directory)

        # Run conversion script
        convert_binary_path = os.path.join(
            os.path.dirname(__file__),
            'convertDbds.sh'
        )
        pargs = [
            convert_binary_path,
            '-q',
            '-p',
        ]
        if self.cache_directory:
            pargs += ['-c', self.cache_directory]

        pargs.append(self.tmpdir)
        pargs.append(self.destination_directory)
        command_output, return_code = generate_stream(pargs)

        # Return
        processed = []

        output_files = command_output.read().split('\n')
        # iterate and every time we hit a .dat file we return the cache

        binary_files = []
        for x in output_files:
            fname = os.path.basename(x)
            _, suff = os.path.splitext(fname)
            if suff == '.dat':
                processed.append({
                    'ascii': os.path.join(self.destination_directory, fname),
                    'binary': sorted(binary_files)
                })
                binary_files = []
            else:
                binary_files.append(os.path.join(self.source_directory, fname))

        return processed
