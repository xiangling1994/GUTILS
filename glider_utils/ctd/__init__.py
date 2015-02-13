
from glider_utils import (
    validate_glider_args,
)

from gsw.gibbs.practical_salinity import SP_from_C
from gsw.gibbs.conversions import SA_from_SP, CT_from_t
from gsw.gibbs.density_enthalpy_48 import rho


def calculate_practical_salinity(timestamps,
                                 conductivity, temperature, pressure):
    """Calculates practical salinity given glider conductivity, temperature,
    and pressure using Gibbs gsw SP_from_C function.

    Parameters:
        timestamp, conductivity (S/m), temperature (C), and pressure (bar).

    Returns:
        salinity (psu PSS-78).
    """

    validate_glider_args(timestamps, conductivity, temperature, pressure)

    # Convert S/m to mS/cm
    mS_conductivity = conductivity * 10

    # Convert bar to dbar
    dBar_pressure = pressure * 10

    return SP_from_C(
        mS_conductivity,
        temperature,
        dBar_pressure
    )


def calculate_density(timestamps,
                      temperature, pressure, salinity,
                      latitude, longitude):
    """Calculates density given glider practical salinity, pressure, latitude,
    and longitude using Gibbs gsw SA_from_SP and rho functions.

    Parameters:
        timestamps (UNIX epoch),
        temperature (C), pressure (bar), salinity (psu PSS-78),
        latitude (decimal degrees), longitude (decimal degrees)

    Returns:
        density (kg/m**3),
        absolute salinity(g/kg), conservative temperature (C)
    """

    validate_glider_args(timestamps,
                         temperature, pressure, salinity,
                         latitude, longitude)

    dBar_pressure = pressure * 10

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
