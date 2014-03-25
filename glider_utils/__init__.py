import numpy as np
from scipy.signal import boxcar, convolve

# Readability
TIME_DIM = 0
DATA_DIM = 1


def clean_dataset(dataset):
    # Get rid of NaNs
    dataset = dataset[~np.isnan(dataset[:, DATA_DIM])]

    # Get rid of successive duplicate timestamps
    # NOTE: Should not be a problem with merged datasets
    keep_indexes = np.append([True], np.diff(dataset[:, TIME_DIM]) != 0)
    dataset = dataset[keep_indexes]

    return dataset


def boxcar_smooth_dataset(dataset, window_size):
    window = boxcar(window_size)
    return convolve(dataset, window, 'same') / window_size


def validate_glider_dataset(dataset):
    """Validates a glider dataset

    Performs the following changes and checks:
    * Makes sure that there are at least 2 points in the dataset
    * Checks for netCDF4 fill types and changes them to NaNs
    * Tests for finite values in time and depth arrays

    Returns validated dataset
    """

    if len(dataset[:, TIME_DIM]) < 2:
        raise IndexError('The time series must have at least two values')

    # Set NC_FILL_VALUES to NaN for consistency if NetCDF lib available
    try:
        from netCDF4 import default_fillvals as NC_FILL_VALUES
        dataset[dataset[:, TIME_DIM] == NC_FILL_VALUES['f8'], TIME_DIM] = float('nan')  # NOQA
        dataset[dataset[:, DATA_DIM] == NC_FILL_VALUES['f8'], DATA_DIM] = float('nan')  # NOQA
    except ImportError:
        pass

    # Test for finite values
    if len(dataset[np.isfinite(dataset[:, TIME_DIM])]) == 0:
        raise ValueError('Time array has no finite values')
    if len(dataset[np.isfinite(dataset[:, DATA_DIM])]) == 0:
        raise ValueError('Depth array has no finite values')

    return dataset
