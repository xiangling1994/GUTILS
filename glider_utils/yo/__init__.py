
import numpy as np
from scipy.interpolate import interp1d
import math

from glider_utils import (
    TIME_DIM,
    DATA_DIM,
    validate_glider_dataset,
    clean_dataset,
    boxcar_smooth_dataset
)


def interpolate_yos(dataset, interval):
    ts = np.arange(
        np.min(dataset[:, TIME_DIM]),
        np.max(dataset[:, TIME_DIM]),
        interval
    )
    # Use cubic to approximate yo
    f = interp1d(dataset[:, TIME_DIM], dataset[:, DATA_DIM], kind='cubic')
    interp_data = f(ts)
    return np.column_stack((ts, interp_data))


def binarize_diff(data):
    data[data <= 0] = -1
    data[data >= 0] = 1
    return data


def calculate_delta_depth(interp_data, interval):
    delta_depth = np.diff(interp_data[:, DATA_DIM])
    delta_depth = binarize_diff(delta_depth)

    delta_depth = boxcar_smooth_dataset(delta_depth, 2)

    return delta_depth


def create_profile_entry(dataset, start, end):
    time_start = dataset[start, TIME_DIM]
    time_end = dataset[end-1, TIME_DIM]
    depth_start = dataset[start, DATA_DIM]
    depth_end = dataset[end-1, DATA_DIM]
    return {
        'index_bounds': (start, end),
        'time_bounds': (time_start, time_end),
        'depth_bounds': (depth_start, depth_end)
    }


def find_yo_extrema(dataset, interval=10):
    """Returns timestamps, row indices, and depth and time bounds
    corresponding to glider yo profiles

    Returns the timestamps and row indices corresponding the peaks and valleys
    (profiles start/stop) found in the time-depth array, tz.  All indices are
    returned.

    Parameters:
        'dataset': An N by 2 numpy array of time, depth pairs

    Optional Parameters:
        'interval': NUMBER
            Specify an alternate interval for resampling the time-series prior
            to indexing.  Default: 10

    Returns:
        {
            'index_bounds': (<starting index>, <ending index of profile>)
            'time_bounds': (<start time of profile>, <end time of profile>)
            'depth_bounds': (<start depth>, <end depth>)
        }

    Use filter_yo_extrema to remove invalid/incomplete profiles
    """

    dataset = validate_glider_dataset(dataset)

    est_data = dataset.copy()

    # Set negative depth values to NaN
    est_data[est_data[:, DATA_DIM] <= 0] = float('nan')

    est_data = clean_dataset(est_data)

    interp_data = interpolate_yos(est_data, interval)

    interp_data[:, DATA_DIM] = (
        boxcar_smooth_dataset(interp_data[:, DATA_DIM], math.ceil(interval/2))
    )

    delta_depth = calculate_delta_depth(interp_data, interval)

    interp_indices = np.argwhere(delta_depth == 0).flatten()

    profiles = []

    start_index = 0
    for index in interp_indices:
        # Find original index
        end_index = np.abs(dataset[:, 0] - interp_data[index, 0]).argmin()

        profiles.append(
            create_profile_entry(dataset, start_index, end_index)
        )

        start_index = end_index

    if start_index < len(dataset):
        profiles.append(
            create_profile_entry(dataset, start_index, len(dataset))
        )

    return profiles
