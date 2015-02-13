import numpy as np
from scipy.signal import boxcar, convolve


def clean_dataset(dataset):
    # Get rid of NaNs
    dataset = dataset[~np.isnan(dataset[:, 1:]).any(axis=1), :]

    return dataset


def boxcar_smooth_dataset(dataset, window_size):
    window = boxcar(window_size)
    return convolve(dataset, window, 'same') / window_size


def validate_glider_args(*args):
    """Validates a glider dataset

    Performs the following changes and checks:
    * Makes sure that there are at least 2 points in the dataset
    * Checks for netCDF4 fill types and changes them to NaNs
    * Tests for finite values in time and depth arrays
    """

    arg_length = len(args[0])

    # Time is assumed to be the first dataset
    if arg_length < 2:
        raise IndexError('The time series must have at least two values')

    for arg in args:
        # Make sure all arguments have the same length
        if len(arg) != arg_length:
            raise ValueError('Arguments must all be the same length')

        # Set NC_FILL_VALUES to NaN for consistency if NetCDF lib available
        try:
            from netCDF4 import default_fillvals as NC_FILL_VALUES
            arg[arg == NC_FILL_VALUES['f8']] = float('nan')  # NOQA
        except ImportError:
            pass

        # Test for finite values
        if len(arg[np.isfinite(arg)]) == 0:
            raise ValueError('Data array has no finite values')
