#!/usr/bin/env python
import warnings

from gutils import (
    validate_glider_args,
)

from gsw.gibbs.practical_salinity import SP_from_C
from gsw.gibbs.conversions import SA_from_SP, CT_from_t
from gsw.gibbs.density_enthalpy_48 import rho


def calculate_practical_salinity(time, conductivity, temperature, pressure):
    """Calculates practical salinity given glider conductivity, temperature,
    and pressure using Gibbs gsw SP_from_C function.

    Parameters:
        time, conductivity (S/m), temperature (C), and pressure (bar).

    Returns:
        salinity (psu PSS-78).
    """

    validate_glider_args(time, conductivity, temperature, pressure)

    # Convert S/m to mS/cm
    mS_conductivity = conductivity * 10

    # Convert bar to dbar
    dBar_pressure = pressure * 10

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return SP_from_C(
            mS_conductivity,
            temperature,
            dBar_pressure
        )


def calculate_density(time, temperature, pressure, salinity, latitude, longitude):
    """Calculates density given glider practical salinity, pressure, latitude,
    and longitude using Gibbs gsw SA_from_SP and rho functions.

    Parameters:
        time (UNIX epoch),
        temperature (C), pressure (bar), salinity (psu PSS-78),
        latitude (decimal degrees), longitude (decimal degrees)

    Returns:
        density (kg/m**3),
    """

    validate_glider_args(time, temperature, pressure, salinity, latitude, longitude)

    dBar_pressure = pressure * 10

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        absolute_salinity = SA_from_SP(
            salinity,
            dBar_pressure,
            longitude,
            latitude
        )

        conservative_temperature = CT_from_t(
            absolute_salinity,
            temperature,
            dBar_pressure
        )

        density = rho(
            absolute_salinity,
            conservative_temperature,
            dBar_pressure
        )

        return density
