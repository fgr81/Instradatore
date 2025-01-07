from datetime import datetime, time
import pytz
import numpy as np
import xarray as xr
from scipy.special import gamma
import cftime
from pydap.client import open_url
from pydap.cas.urs import setup_session
import sys
import os
import glob
import logging 

logging.basicConfig(filename='log/sand_flux.log', level=logging.DEBUG)

# Root directory for processing
root_lavoro = f"data/{sys.argv[1]}/"

OV_VAL = 9.96921e+36  # CAP post-processing induces the overflow on this numeric value


def logica_comune(func):
    """
    Decorator to add common logic before and after specific algorithm execution.

    Args:
        func (function): The function to wrap.

    Returns:
        function: The wrapped function with common logic.
    """
    def wrapper(*args, **kwargs):
        logging.info(func.__name__)

        logging.debug('step_iniziale')
        filtered_ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_a = step_iniziale(*args, **kwargs)

        logging.debug('func')
        result_algo = func(filtered_ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_a)

        logging.debug('calcola metriche')
        new_ds = calcola_metriche(*result_algo)

        logging.debug('salvo netcdf')
        file_dest = f"{root_lavoro}/sand_flux_data/{func.__name__}__th_{sys.argv[3]}.nc"
        new_ds.to_netcdf(file_dest)

        return 0
    return wrapper


def step_iniziale(*args, **kwargs):
    """
    Loads and preprocesses the dataset for sand flux computation.

    Returns:
        tuple: Contains the dataset, surface pressure, surface temperature, wind components, 
               stress, and air density.
    """
    logging.debug("Open dataset...")
    ds = xr.open_dataset(f"{root_lavoro}/03340.atmos_diurn.nc", decode_times=False)
    logging.debug("Done.")

    psf = ds['ps']
    tsf = ds['temp_bot']
    wind_surf_x = ds['ucomp_bot']
    wind_surf_y = ds['vcomp_bot']

    R = 189.020
    rho_a = psf / (R * tsf)

    CD = 0.003  # Drag coefficient
    wind_speed = np.sqrt(wind_surf_x**2 + wind_surf_y**2)
    stress = CD * rho_a * wind_speed**2

    return ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_a


@logica_comune
def fenton2018(ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_a):
    """
    Implements the sand flux calculation based on Fenton et al. (2018).

    Args:
        ds (xarray.Dataset): Input dataset.
        psf (xarray.DataArray): Surface pressure.
        tsf (xarray.DataArray): Surface temperature.
        wind_surf_x (xarray.DataArray): Eastward wind component.
        wind_surf_y (xarray.DataArray): Northward wind component.
        stress (xarray.DataArray): Surface stress.
        rho_a (xarray.DataArray): Air density.

    Returns:
        tuple: Contains sand flux, wind direction, and wind components.
    """
    logging.debug('Calculating u* thresholds.')
    AN = 0.0123
    G = 3.72
    RHO_P = 3000.0
    GAM = 0.0003
    D = 0.0001

    ustar_ft = np.sqrt(AN * ((RHO_P * G * D / rho_a) + (GAM / (rho_a * D))))

    logging.debug('Calculating downwind direction.')
    downwind = np.arctan2(wind_surf_x, wind_surf_y) * (180. / np.pi)
    downwind = xr.where((downwind < 0.), downwind + 360., downwind)

    C1 = 5.5e-3
    C2 = 49.
    DP = 100.
    C3 = 0.29
    C4 = 3.84e-3
    ustar_it = C1 * (700. / psf) ** (1. / 6) * (220. / tsf) ** 0.4 * np.exp((C2 / DP) ** 3 + C3 * np.sqrt(DP) - C4 * DP)

    logging.debug('Calculating potential sand transport Q.')
    K = 2.75
    CQ = 2.61
    gammak = gamma(1. + (1. / K))
    ustar = np.sqrt(stress / rho_a)

    inexp_ft = -1 * (ustar_ft * gammak / ustar) ** K
    inexp_it = -1 * (ustar_it * gammak / ustar) ** K

    p_tr = np.exp(inexp_ft) + np.exp(inexp_ft) * ((np.exp(inexp_it) - np.exp(inexp_ft)) / (1 - np.exp(inexp_it) + np.exp(inexp_ft)))

    q = p_tr * CQ * (rho_a / G) * (ustar - ustar_it) * (ustar + ustar_it) ** 2

    return q, downwind, wind_surf_x, wind_surf_y


@logica_comune
def rubanenko2023(ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_f):
    """
    Implements the sand flux calculation based on Rubanenko et al. (2023).

    Args:
        ds (xarray.Dataset): Input dataset.
        psf (xarray.DataArray): Surface pressure.
        tsf (xarray.DataArray): Surface temperature.
        wind_surf_x (xarray.DataArray): Eastward wind component.
        wind_surf_y (xarray.DataArray): Northward wind component.
        stress (xarray.DataArray): Surface stress.
        rho_f (float): Fluid density threshold.

    Returns:
        tuple: Contains sand flux, wind direction, and wind components.
    """
    logging.debug('Calculating downwind direction.')
    downwind = np.arctan2(wind_surf_x, wind_surf_y) * (180. / np.pi)
    downwind = xr.where((downwind < 0.), downwind + 360., downwind)

    logging.debug('Calculating Q...')
    tau = stress
    tau_it = float(sys.argv[3])
    q = xr.where(
        tau > tau_it,
        np.sqrt(tau_it / rho_f) * (tau - tau_it),
        0.
    )
    return q, downwind, wind_surf_x, wind_surf_y


def calcola_metriche(q, downwind, wind_surf_x, wind_surf_y):
    """
    Calculates metrics for sand transport analysis.

    Args:
        q (xarray.DataArray): Sand flux.
        downwind (xarray.DataArray): Wind direction.
        wind_surf_x (xarray.DataArray): Eastward wind component.
        wind_surf_y (xarray.DataArray): Northward wind component.

    Returns:
        xarray.Dataset: Dataset with calculated metrics.
    """
    dp = np.sum(q)
    q_x = np.sin(np.deg2rad(downwind)) * q
    q_y = np.cos(np.deg2rad(downwind)) * q
    rdd = np.degrees(np.arctan2(np.sum(q_x), np.sum(q_y)))
    rdp = np.sqrt(np.sum(q_x) ** 2 + np.sum(q_y) ** 2)
    rdp_dp = rdp / dp

    logging.debug(f"DP = {dp}")
    logging.debug(f"RDD = {rdd} degrees")
    logging.debug(f"RDP = {rdp}")
    logging.debug(f"RDP/DP = {rdp_dp}")

    new_ds = xr.Dataset({
        'wind_surf_x': wind_surf_x,
        'wind_surf_y': wind_surf_y,
        'downwind': downwind,
        'q': q,
        'rdd': rdd,
        'rdp': rdp,
        'rdp_dp': rdp_dp,
        'dp': dp,
    })

    new_ds = new_ds.roll(lon=new_ds.dims['lon'] // 2, roll_coords=True)
    new_ds['lon'] = (new_ds['lon'] + 180) % 360 - 180

    return new_ds


if sys.argv[2] == "fenton2018":
    fenton2018()
elif sys.argv[2] == "rubanenko2023":
    rubanenko2023()

