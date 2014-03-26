
import numpy as np
from scipy.interpolate import interp1d

from glider_utils import (
    TIME_DIM,
    DATA_DIM,
    validate_glider_dataset,
    clean_dataset,
    boxcar_smooth_dataset
)


def interpolate_yos(estimated_dataset, dataset):
    # Use cubic to approximate yo
    f = interp1d(
        estimated_dataset[:, TIME_DIM],
        estimated_dataset[:, DATA_DIM],
        kind='cubic'
    )
    return f(dataset[:, TIME_DIM])


def binarize_diff(data):
    data[data <= 0] = -1
    data[data >= 0] = 1
    return data


def calculate_delta_depth(interp_data):
    delta_depth = np.diff(interp_data)
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


def find_yo_extrema(dataset):
    """Returns timestamps, row indices, and depth and time bounds
    corresponding to glider yo profiles

    Returns the timestamps and row indices corresponding the peaks and valleys
    (profiles start/stop) found in the time-depth array, tz.  All indices are
    returned.

    Parameters:
        'dataset': An N by 2 numpy array of time, depth pairs

    Returns:
        'profiled_dataset': An N by 3 numpy array of time, depth,
            and profile pairs

    Use filter_yo_extrema to remove invalid/incomplete profiles
    """

    dataset = validate_glider_dataset(dataset)

    est_data = dataset.copy()

    # Set negative depth values to NaN
    est_data[est_data[:, DATA_DIM] <= 0] = float('nan')

    est_data = clean_dataset(est_data)

    # Stretch estimated values for interpolation to span entire dataset
    est_data[0, TIME_DIM] = dataset[0, TIME_DIM]
    est_data[-1, TIME_DIM] = dataset[-1, TIME_DIM]

    interp_data = interpolate_yos(est_data, dataset)

    interp_data = boxcar_smooth_dataset(interp_data, 5)

    delta_depth = calculate_delta_depth(interp_data)

    interp_indices = np.argwhere(delta_depth == 0).flatten()

    profiled_dataset = np.zeros((len(dataset), 3))
    profiled_dataset[:, TIME_DIM] = dataset[:, TIME_DIM]
    profiled_dataset[:, DATA_DIM] = interp_data

    start_index = 0
    for profile_id, end_index in enumerate(interp_indices):
        profiled_dataset[start_index:end_index, 2] = profile_id

        start_index = end_index

    if start_index < len(profiled_dataset):
        profiled_dataset[start_index:, 2] = len(interp_indices)-1

    return profiled_dataset
