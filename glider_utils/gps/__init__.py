from glider_utils import (
    TIME_DIM,
    validate_glider_dataset,
    clean_dataset
)

from scipy.interpolate import interp1d

# Readability
LAT_DIM = 1
LON_DIM = 2


def interpolate_gps(dataset):
    """Calculates interpolated GPS coordinates between the two surfacings
    in a single glider binary data file.

    Parameters:
        'dataset': An N by 3 numpy array of time, depth pairs

    Returns interpolated gps dataset over entire time domain of dataset
    """

    dataset = validate_glider_dataset(dataset)
    est_data = dataset.copy()

    est_data = clean_dataset(est_data)

    # If only one GPS point, make it the same for the entire dataset
    if len(est_data) == 1:
        dataset[:, LAT_DIM:LON_DIM+1] = est_data[0, LAT_DIM:LON_DIM+1]
    else:
        # Bump the first GPS timestamp to the first dataset timestamp
        est_data[0, TIME_DIM] = dataset[0, TIME_DIM]

        # Drop the last GPS timestamp to the last dataset timestamp
        est_data[-1, TIME_DIM] = dataset[-1, TIME_DIM]

        # Interpolate data
        f_lat = interp1d(
            est_data[:, TIME_DIM],
            est_data[:, LAT_DIM],
            kind='linear'
        )
        f_lon = interp1d(
            est_data[:, TIME_DIM],
            est_data[:, LON_DIM],
            kind='linear'
        )
        dataset[:, LAT_DIM] = f_lat(dataset[:, TIME_DIM])
        dataset[:, LON_DIM] = f_lon(dataset[:, TIME_DIM])

    return dataset
