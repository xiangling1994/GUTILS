#!/usr/bin/env python
import math

import numpy as np
import pandas as pd

from gutils import (
    masked_epoch,
    validate_glider_args,
    clean_dataset,
    boxcar_smooth_dataset
)
from gutils.yo.filters import default_filter

import logging
L = logging.getLogger(__name__)


# For Readability
TIME_DIM = 0
DATA_DIM = 1


def binarize_diff(data):
    data[data <= 0] = -1
    data[data >= 0] = 1
    return data


def calculate_delta_depth(interp_data):
    delta_depth = np.diff(interp_data)
    delta_depth = binarize_diff(delta_depth)
    delta_depth = boxcar_smooth_dataset(delta_depth, 2)
    return delta_depth


def assign_profiles(df, tsint=None):
    """Returns the start and stop timestamps for every profile indexed from the
    depth timeseries
    Parameters:
        time, depth
    Returns:
        A Nx2 array of the start and stop timestamps indexed from the yo
    Use filter_yo_extrema to remove invalid/incomplete profiles
    """

    profile_df = df.copy()
    tmp_df = df.copy()

    if tsint is None:
        tsint = 5

    # Set negative depth values to NaN
    tmp_df.loc[tmp_df.z <= 0, 'z'] = math.nan
    # Remove NaN rows
    tmp_df = tmp_df.dropna(subset=['z'])

    if len(tmp_df) < 2:
        L.debug('Skipping yo that contains < 2 rows')
        return np.empty((0, 2))

    # Make 't' epochs and not a DateTimeIndex
    tmp_df['t'] = masked_epoch(tmp_df.t)

    # Create the fixed timestamp array from the min timestamp to the max timestamp
    # spaced by tsint intervals
    ts = np.arange(tmp_df.t.min(), tmp_df.t.max(), tsint)
    # Stretch estimated values for interpolation to span entire dataset
    interp_z = np.interp(
        ts,
        tmp_df.t,
        tmp_df.z,
        left=tmp_df.z.iloc[0],
        right=tmp_df.z.iloc[-1]
    )

    filtered_z = boxcar_smooth_dataset(interp_z, tsint // 2)
    delta_depth = calculate_delta_depth(filtered_z)

    p_inds = np.empty((0, 2))
    inflections = np.where(np.diff(delta_depth) != 0)[0]
    p_inds = np.append(p_inds, [[0, inflections[0]]], axis=0)

    for p in range(len(inflections) - 1):
        p_inds = np.append(p_inds, [[inflections[p], inflections[p + 1]]], axis=0)
    p_inds = np.append(p_inds, [[inflections[-1], len(ts) - 1]], axis=0)

    # Start profile index
    profile_index = 0
    ts_window = tsint * 2
    profile_df['profile'] = math.nan  # Fill profile with nans

    # Iterate through the profile start/stop indices
    for p0, p1 in p_inds:

        min_time = pd.to_datetime(ts[int(p0)] - ts_window, unit='s')
        max_time = pd.to_datetime(ts[int(p1)] + ts_window, unit='s')

        # Get rows between the min and max time
        time_between = profile_df.t.between(min_time, max_time, inclusive=True)

        # Get indexes of the between rows since we can't assign by the range due to NaT values
        ixs = profile_df.loc[time_between].index.tolist()

        # Set the rows profile column to the profile id
        profile_df.loc[ixs[0]:ixs[-1], 'profile'] = profile_index

        # Increment the profile index
        profile_index += 1

    # L.info(
    #     list(zip(
    #         profile_df.t.values[180:200].tolist(),
    #         profile_df.profile.values[180:200].tolist(),
    #         profile_df.z.values[180:200].tolist(),
    #     ))
    # )
    return profile_df
