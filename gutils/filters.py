#!python
# coding=utf-8
import pandas as pd

from gutils.yo import assign_profiles
from gutils.slocum import SlocumReader

import logging
L = logging.getLogger(__name__)


def default_filter(dataset):
    dataset = filter_profile_depth(dataset)
    dataset = filter_profile_number_of_points(dataset)
    dataset = filter_profile_timeperiod(dataset)
    dataset = filter_profile_distance(dataset)
    return dataset


def filter_profiles(dataset, conditional, reindex=True):
    """Filters out profiles that do not meet some criteria

    Returns the filtered set of profiles
    """
    filtered = dataset.groupby('profile').filter(conditional).copy()

    # Re-index the profiles
    if reindex is True:
        profs = filtered.profile.unique()
        for ix, p in enumerate(profs):
            filtered.loc[filtered.profile == p, 'profile'] = ix

    return filtered


def filter_profile_depth(dataset, below=None, reindex=True):
    """Filters out profiles that are not completely below a certain depth (Default: 1m). This is
    depth positive down.

    Returns a DataFrame with a subset of profiles
    """

    if below is None:
        below = 1

    def conditional(profile):
        return profile.z.max() >= below

    return filter_profiles(dataset, conditional, reindex)


def filter_profile_timeperiod(dataset, timespan_condition=None, reindex=True):
    """Filters out profiles that do not span a specified number of seconds
    (Default: 10 seconds)

    Returns a DataFrame with a subset of profiles
    """
    if timespan_condition is None:
        timespan_condition = 10

    def conditional(profile):
        timespan = profile.t.max() - profile.t.min()
        return timespan >= pd.Timedelta(timespan_condition, unit='s')

    return filter_profiles(dataset, conditional, reindex)


def filter_profile_distance(dataset, distance_condition=None, reindex=True):
    """Filters out profiles that do not span a specified vertical distance
    (Default: 1m)

    Returns a DataFrame with a subset of profiles
    """
    if distance_condition is None:
        distance_condition = 1

    def conditional(profile):
        distance = abs(profile.z.max() - profile.z.min())
        return distance >= distance_condition

    return filter_profiles(dataset, conditional, reindex)


def filter_profile_number_of_points(dataset, points_condition=None, reindex=True):
    """Filters out profiles that do not have a specified number of points
    (Default: 5 points)

    Returns a DataFrame with a subset of profiles
    """
    if points_condition is None:
        points_condition = 3

    def conditional(profile):
        return len(profile) >= points_condition

    return filter_profiles(dataset, conditional, reindex)


def process_dataset(file=None, reader_class=None, subset=True, filter_z=None, filter_points=None, filter_time=None, filter_distance=None):

    # Check filename
    if file is None:
        raise ValueError('Must specify path to combined ASCII file')

    reader_class = reader_class or SlocumReader

    try:
        reader = reader_class(file)
        data = reader.standardize()

        # Optionally, remove any variables from the dataframe that do not have metadata assigned
        if subset is True:
            orphans = set(data.columns) - set(attrs.get('variables', {}).keys())
            L.info(
                "Excluded from output because there was no metadata: {}".format(orphans)
            )
            data = data.drop(orphans, axis=1)

        # Find profile breaks
        profiles = assign_profiles(data)

        # Filter data
        filtered = filter_profile_depth(profiles, below=filter_z)
        filtered = filter_profile_number_of_points(filtered, points_condition=filter_points)
        filtered = filter_profile_timeperiod(filtered, timespan_condition=filter_time)
        filtered = filter_profile_distance(filtered, distance_condition=filter_distance)

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
        raise

    return filtered, reader.mode
