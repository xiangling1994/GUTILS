
from glider_utils import (
    validate_glider_dataset,
)

from gsw.gibbs.practical_salinity import SP_from_C

COND_DIM = 1
TEMP_DIM = 2
PRES_DIM = 3
SALI_DIM = 4

import numpy as np


def calculate_practical_salinity(dataset):
    """Calculates practical salinity given glider conductivity, temperature,
    and pressure using Gibbs gsw SP_from_C function.

    Parameters:
        'dataset': An N x 4 array of timestamps(UNIX epoch),
            conductivity (S/m), temperature (C), and pressure (bar).

    Returns:
        'salinity_dataset': An N x 5 array of timestamps (UNIX epoch),
            conductivity (mS/cm), temperature (C), pressure (dbar) and
            salinity (psu PSS-78).
    """

    salinity_dataset = np.zeros((len(dataset), 5))
    for i in range(0, 4):
        salinity_dataset[:, i] = dataset[:, i]

    salinity_dataset = validate_glider_dataset(salinity_dataset)

    # Convert S/m to mS/cm
    salinity_dataset[:, COND_DIM] *= 10

    # Convert bar to dbar
    salinity_dataset[:, PRES_DIM] *= 10

    salinity_dataset[:, SALI_DIM] = SP_from_C(
        salinity_dataset[:, COND_DIM],
        salinity_dataset[:, TEMP_DIM],
        salinity_dataset[:, PRES_DIM]
    )

    return salinity_dataset
