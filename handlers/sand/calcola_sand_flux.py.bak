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

#root_lavoro = sys.argv[1]
root_lavoro = f"data/{sys.argv[1]}/"

OV_VAL = 9.96921e+36  # CAP post-processing induces the overflow on this numeric value

# Definizione del decoratore
def logica_comune(func):
    def wrapper(*args, **kwargs):
        # Parte comune iniziale
        logging.info(func.__name__)

        logging.debug('step_iniziale')
        filtered_ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_a = step_iniziale(*args, **kwargs)

        # Esecuzione del passo centrale
        logging.debug('func')
        result_algo = func(filtered_ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_a)

        # Parte comune finale
        logging.debug('calcola metriche')
        new_ds = calcola_metriche(*result_algo)

        logging.debug('salvo netcdf')
        file_dest = f"{root_lavoro}/sand_flux_data/{func.__name__}__th_{sys.argv[3]}.nc"
        
        new_ds.to_netcdf(file_dest)

        return 0
    
    return wrapper

def step_iniziale(*args, **kwargs):
    logging.debug("Open dataset...")
    # fmg 281124 ds = xr.open_dataset(f"{root_lavoro}/atmos_daily_zstd_sel.nc", decode_times=False)
    # fmg 021224 ds = xr.open_dataset(f"{root_lavoro}/03340.atmos_average_zstd.nc", decode_times=False)
    ds = xr.open_dataset(f"{root_lavoro}/03340.atmos_diurn.nc", decode_times=False)
    logging.debug("Done.")

    #numts = ds.sizes['time']
    #logging.debug(f"risoluzione temporale: {round( (668/numts)**(-1))} ist. per sol")

    psf = ds['ps']
    ''' fmg 281124 adatto al file dati preso dalla nasa 
    tsf = ds['surf_temp']
    wind_surf_x = ds['ukd']
    wind_surf_y = ds['vkd']
    '''
    tsf = ds['temp_bot']
    wind_surf_x = ds['ucomp_bot']
    wind_surf_y = ds['vcomp_bot']

    # fmg 021224 questo campo non c'è -- co2 = ds['co2ice_sfc']
    R = 189.020
    rho_a = psf / (R * tsf)

    #####
    # ATMOS_DIURN
    #####
    # fmg 021224 
    # Nei file diurn forniti dalla nasa non c'è lo stress
    #   quindi me lo devo calcolare, uso la formula:
    #   tau = rho_a * qrt(u_star) * Cd 
    # stress = ds['stress']
    CD = 0.003  # L'ho preso da AmesGCM/atmos_param_mars/surface_flux.F90  ->  real    :: cd_drag_cnst          =  0.003
    wind_speed = np.sqrt(wind_surf_x**2 + wind_surf_y**2)
    stress = CD * rho_a * wind_speed**2

    
    #mask_no_co2 = co2 == 0.0  # True dove non c'è co2 ghiacciata
    #filtered_ds = ds.where(mask_no_co2, drop=True)

    #return filtered_ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_a
    return ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_a


# Definizione dell'algoritmo specifico (Fenton 2018)
@logica_comune
def fenton2018(ds, psf, tsf, wind_surf_x, wind_surf_y, stress, rho_a):
    logging.debug('Calculating u* thresholds.')
    AN = 0.0123
    G = 3.72
    R = 189.020
    RHO_P = 3000.0
    GAM = 0.0003
    D = 0.0001
    #rho_a = psf / (R * tsf)
    
    ustar_ft = np.sqrt(AN * ((RHO_P * G * D / rho_a) + (GAM / (rho_a * D))))
    
    logging.debug('Calcularing downwind')
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
    ###
    # Calculate wind direction.
    ###
    logging.debug('Rubanenko2023 -- Calcularing downwind')
    downwind = np.arctan2(wind_surf_x, wind_surf_y) * (180. / np.pi)  # Wind direction.
    downwind = xr.where((downwind < 0.), downwind + 360., downwind)

    logging.debug('Calculating Q...')
    # Convert wind shear to bed stress ,   Ref: (2) page 4 of RUBANENKO et al.
    # tau = rho_f * u_star_2_min_zstd
    tau = stress
    # tau_it = 0.01  # 0.01 N/m² corrisponde a circa 0.00102 kg/m².
    tau_it = float(sys.argv[3])
    logging.debug(f"tau_it={tau_it}")
    q = xr.where(
        tau > tau_it,
        np.sqrt(tau_it / rho_f) * (tau - tau_it),
        0.
    )
    
    return q, downwind, wind_surf_x, wind_surf_y

# Funzione finale per calcolare metriche comuni
def calcola_metriche(q, downwind, wind_surf_x, wind_surf_y):
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


    #####
    # ATMOS_DIURN: Unifica le dimensioni temporali time e time_of_day_24
    #
    #####
    time = new_ds['time']
    time_of_day_24 = new_ds['time_of_day_24']
    new_time = (time.values[:, np.newaxis] + time_of_day_24.values[np.newaxis, :] / 24).flatten()
    new_ds2 = new_ds.stack(new_time=("time", "time_of_day_24")).reset_index("new_time")
    new_ds2 = new_ds2.assign_coords(new_time=new_time)
    new_ds2 = new_ds2.drop_vars(["time", "time_of_day_24"])  

    # Rinomina la nuova dimensione
    new_ds2 = new_ds2.rename({"new_time": "time"})

    return new_ds2

# Esecuzione
#alg = "fenton2018"
#file_dest = f"{root_lavoro}/sand_flux_{alg}.nc"
#ds = fenton2018()  # Restituisce il nuovo dataset con le metriche
#ds.to_netcdf(file_dest)

#alg = "rubanenko2023"
#file_dest = f"{root_lavoro}/sand_flux_{alg}.nc"
#ds = rubanenko2023()
#ds.to_netcdf(file_dest)

logging.debug(sys.argv[2])

if sys.argv[2] == "fenton2018":
    fenton2018()
elif sys.argv[2] == "rubanenko2023":
    rubanenko2023()
