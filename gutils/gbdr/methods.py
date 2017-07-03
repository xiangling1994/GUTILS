#!/usr/bin/env python
from __future__ import division  # always return floats when dividing

import re
import os
import math
import subprocess
from glob import glob
from whichcraft import which
from six import StringIO

import logging
L = logging.getLogger(__name__)

dbd2asc_path = which('dbd2asc')  # conda
if dbd2asc_path is None:
    dbd2asc_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bin', 'dbd2asc')  # pip


def parse_glider_filename(filename):
    """
    Parses a glider filename and returns details in a dictionary

    Parameters
    ----------
    filename : str
        A filename to parse

    Returns
    -------
    dict
        Returns dictionary with the following keys:

            * 'glider': glider name
            * 'year': data file year created
            * 'day': data file julian date created
            * 'mission': data file mission id
            * 'segment': data file segment id
            * 'type': data file type
    """
    head, tail = os.path.split(filename)

    matches = re.search(r"([\w\d\-]+)-(\d+)-(\d+)-(\d+)-(\d+)\.(\w+)$", tail)

    if matches is not None:
        return {
            'path': head,
            'glider': matches.group(1),
            'year': int(matches.group(2)),
            'day': int(matches.group(3)),
            'mission': int(matches.group(4)),
            'segment': int(matches.group(5)),
            'type': matches.group(6)
        }
    else:
        raise ValueError(
            "Filename ({}) not in usual glider format: "
            "<glider name>-<year>-<julian day>-"
            "<mission>-<segment>.<extenstion>".format(filename)
        )


def generate_glider_filename(description):
    """Converts a glider data file details dictionary to filename

    Parameters
    ----------
    description : dict
        A glider data file details dict

    Returns
    -------
    str
        Generated glider filename
    """
    filename = (
        "{glider}-{year:d}-{day:03d}-{mission:d}-{segment}.{type}".format(**description)
    )
    return os.path.join(description['path'], filename)


def generate_stream(processArgs):
    """ Runs a given process and outputs the resulting text as a StringIO

    Parameters
    ----------
    processArgs : list
        Arguments to run in a process

    Returns
    -------
    StringIO
        Resulting text
    """
    process = subprocess.Popen(processArgs, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, _ = process.communicate()
    return StringIO(stdout), process.returncode


def can_find_bd_index(path):
    # Iterate through previous segment files
    processArgs = [dbd2asc_path, '-c', '/tmp', path]
    returncode = 1
    file_details = parse_glider_filename(path)
    while returncode == 1 and file_details['segment'] >= 0:
        processArgs[3] = generate_glider_filename(file_details)
        stream, returncode = generate_stream(processArgs)
        file_details['segment'] -= 1

    # Report whether index found or not
    return returncode == 0


def process_file(path):
    """Processes a single glider data file.

    Intelligently falls back if previous index file has not
    been processed for given data file.

    Parameters
    ----------
    path : str
        Path to a glider data file

    Returns
    -------
    StringIO
        Resulting text

    Raises
    ------
    ValueError
        If data index cannot be found for given data file.
    """
    processArgs = [dbd2asc_path, '-c', '/tmp', path]
    stream, returncode = generate_stream(processArgs)

    # Fallback to find index bd file
    if returncode == 1:
        if can_find_bd_index(path):
            stream, returncode = generate_stream(processArgs)
        else:
            raise KeyError("Cannot find data file index for: %s" % path)

    return stream


def process_all_of_type(path, extension):
    """Process glider data files of one type

    No fallback.  Assumed that operation big enough to avoid this issue.

    Parameters
    ----------
    path : str
        Path to a folder of glider data files
    extension: str
        Extension for the type of glider you wish to process

    Returns
    -------
    StringIO
        Resulting text
    """
    processArgs = [dbd2asc_path, '-c', '/tmp']
    filesWildCard = '%s/*.%s' % (path, extension)
    processArgs.extend(glob(filesWildCard))
    stream, returncode = generate_stream(processArgs)
    return stream


def process_file_list(filePaths):
    """Process a list of glider data files to ASCII.

    Intelligently falls back for each file if necessary.

    Parameters
    ----------
    filePaths : list
        List of glider data files to process to ASCII

    Returns
    -------
    StringIO
        Resulting text

    Raises
    ------
    KeyError
        If it cannot generate an index for a given file.
    """
    processArgs = [dbd2asc_path, '-c', '/tmp']

    for filePath in filePaths:
        processArgs.append(filePath)

    stream, returncode = generate_stream(processArgs)

    # Fallback in case the cache is not available
    if returncode == 1:
        for filePath in filePaths:
            if not can_find_bd_index(filePath):
                raise KeyError(
                    "Cannot find data file index for: {}".format(filePath)
                )

        # Reprocess the file list
        stream, returncode = generate_stream(processArgs)

    return stream


def create_glider_BD_ASCII_reader(filePaths):
    """Creates a glider binary data reader over a set of files

    Parameters
    ----------
    filePaths : list
        List of glider data files to process to ASCII

    Returns
    -------
    StringIO
        Resulting text

    Raises
    ------
    KeyError
        If it cannot generate an index for a given file.
    """
    return process_file_list(filePaths)


def find_glider_BD_headers(reader):
    """Finds and returns available headers in a set of glider data files

    Parameters
    ----------
    reader : StringIO
        Glider binary data reader output

    Returns
    -------
    list
        A list of headers

    Raises
    ------
    EOFError
        When no headers are found in the file
    """

    # Bleed off extraneous headers
    # Stop when sci_m_present_time is found
    line = reader.readline()
    while len(line) > 0 and line.find('m_present_time') == -1:
        line = reader.readline()

    if line is '':
        raise EOFError('No headers found before end of file.')

    headersTemp = [ x for x in line.split(' ') if x.strip() ]

    unitsLine = reader.readline()
    if unitsLine is not None:
        unitsTemp = [ x for x in unitsLine.split(' ') if x.strip() ]
    else:
        raise EOFError('No units found before end of file.')

    headers = []
    for header, unit in zip(headersTemp, unitsTemp):
        if header and unit:
            description = {
                'name': header,
                'units': unit,
                'is_point': header.find('lat') != -1 or header.find('lon') != -1
            }
            headers.append(description)

    # Remove extraneous bytes line
    reader.readline()

    return headers


def get_decimal_degrees(lat_lon):
    """Converts NMEA GPS format (DDDmm.mmmm) to decimal degrees (DDD.dddddd)

    Parameters
    ----------
    lat_lon : str
        NMEA GPS coordinate (DDDmm.mmmm)

    Returns
    -------
    float
        Decimal degree coordinate (DDD.dddddd) or math.nan
    """

    # Absolute value of the coordinate
    try:
        pos_lat_lon = abs(lat_lon)
    except (TypeError, ValueError):
        return math.nan

    # Calculate NMEA degrees as an integer
    nmea_degrees = int(pos_lat_lon // 100) * 100

    # Subtract the NMEA degrees from the absolute value of lat_lon and divide by 60
    # to get the minutes in decimal format
    gps_decimal_minutes = (pos_lat_lon - nmea_degrees) / 60

    # Divide NMEA degrees by 100 and add the decimal minutes
    decimal_degrees = (nmea_degrees // 100) + gps_decimal_minutes

    # Round to 6 decimal places
    decimal_degrees = round(decimal_degrees, 6)

    if lat_lon < 0:
        return -decimal_degrees

    return decimal_degrees


def map_line(reader, headers):
    """Maps all non-NaN values in a glider data file to a known header

    Parameters
    ----------
    reader : StringIO
        Glider binary data reader output
    headers : list
        Headers discovered in data file

    Returns
    -------
    dict
        Mapping of glider data to a known header
    """

    readings = {}

    line = reader.readline()

    if len(line) == 0:
        raise EOFError('That\'s all the data!')

    line = line.rstrip()

    value_strings = line.split(' ')
    for i, string in enumerate(value_strings):
        if string != 'NaN':
            value = float(string)

            if i < len(headers):
                if headers[i]['is_point']:
                    value = get_decimal_degrees(value)
                key = headers[i]['name'] + "-" + headers[i]['units']
                readings[key] = value

    # Provide generic timestamp regardless of type for iterator
    # convenience
    # Keep originals for those interested
    if 'm_present_time-timestamp' in readings:
        readings['timestamp'] = readings['m_present_time-timestamp']
    elif 'sci_m_present_time-timestamp' in readings:
        readings['timestamp'] = readings['sci_m_present_time-timestamp']

    return readings
