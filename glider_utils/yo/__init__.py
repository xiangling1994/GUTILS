
import numpy as np
from scipy.interpolate import interp1d
from scipy.signal import boxcar, convolve
import math


# Readability
TIME_DIM = 0
DEPTH_DIM = 1


def clean_est_data(est_data):
    # Set negative depth values to NaN
    est_data[est_data[:, DEPTH_DIM] <= 0] = float('nan')

    # Get rid of NaNs
    est_data = est_data[~np.isnan(est_data[:, DEPTH_DIM])]

    # Get rid of successive duplicate timestamps
    # NOTE: Should not be a problem with merged datasets
    keep_indexes = np.append([True], np.diff(est_data[:, TIME_DIM]) != 0)
    est_data = est_data[keep_indexes]

    return est_data


def interpolate_depth(est_data, interval):
    ts = np.arange(
        np.min(est_data[:, TIME_DIM]),
        np.max(est_data[:, TIME_DIM]),
        interval
    )
    f = interp1d(est_data[:, TIME_DIM], est_data[:, DEPTH_DIM], kind='cubic')
    interp_depth = f(ts)
    return np.column_stack((ts, interp_depth))


def boxcar_smooth_data(interp_data, window_size):
    window = boxcar(window_size)
    return convolve(interp_data, window, 'same') / window_size


def binarize_diff(data):
    data[data <= 0] = -1
    data[data >= 0] = 1
    return data


def calculate_delta_depth(interp_data, interval):
    delta_depth = np.diff(interp_data[:, DEPTH_DIM])
    delta_depth = binarize_diff(delta_depth)

    delta_depth = boxcar_smooth_data(delta_depth, 2)

    return delta_depth


def create_profile_entry(dataset, start, end):
    time_start = dataset[start, TIME_DIM]
    time_end = dataset[end-1, TIME_DIM]
    depth_start = dataset[start, DEPTH_DIM]
    depth_end = dataset[end-1, DEPTH_DIM]
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

    if len(dataset[:, TIME_DIM]) < 2:
        raise IndexError('The time series must have at least two values')

    # Set NC_FILL_VALUES to NaN for consistency if NetCDF lib available
    try:
        from netCDF4 import default_fillvals as NC_FILL_VALUES
        dataset[dataset[:, TIME_DIM] == NC_FILL_VALUES['f8'], TIME_DIM] = float('nan')  # NOQA
        dataset[dataset[:, DEPTH_DIM] == NC_FILL_VALUES['f8'], DEPTH_DIM] = float('nan')  # NOQA
    except ImportError:
        pass

    # Test for finite values
    if len(dataset[np.isfinite(dataset[:, TIME_DIM])]) == 0:
        raise ValueError('Time array has no finite values')
    if len(dataset[np.isfinite(dataset[:, DEPTH_DIM])]) == 0:
        raise ValueError('Depth array has no finite values')

    est_data = dataset.copy()

    est_data = clean_est_data(est_data)

    interp_data = interpolate_depth(est_data, interval)

    interp_data[:, DEPTH_DIM] = (
        boxcar_smooth_data(interp_data[:, DEPTH_DIM], math.ceil(interval/2))
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
