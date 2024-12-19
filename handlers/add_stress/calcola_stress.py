import sys
import numpy as np
from netCDF4 import Dataset
import logging

prefix = sys.argv[2]  # Primo argomento passato
current = sys.argv[3]  
_next = sys.argv[4]  
root_lavoro = sys.argv[5]  
tipo_dati = sys.argv[6]  

logging.basicConfig(filename='calcola_stress.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(filename)s - Line %(lineno)d - %(message)s')

logging.debug(f"* calcola_stress.py  {prefix=} {current=} {_next=} {root_lavoro=} {tipo_dati=}")


# Caricamento dei dati
file_path = f"{root_lavoro}/{prefix}.atmos_{tipo_dati}_Ls{current}_{_next}_zagl.nc"
ds = Dataset(file_path, 'r+')  # Apertura in modalità lettura e scrittura

# Variabili dal file
z = ds.variables['zagl'][:]  
u = ds.variables['ucomp'][:]  
v = ds.variables['vcomp'][:]  
rho = ds.variables['rho_f'][:]

# Quote selezionate (in metri)
selected_z = [15, 30, 50]
selected_indices = [np.argmin(np.abs(z - sz)) for sz in selected_z]  # Indici delle quote

# Filtra le componenti U e V per le quote selezionate
u_selected = u[:, :, selected_indices, :, :]
v_selected = v[:, :, selected_indices, :, :]

# Array per salvare i risultati
shear_u = np.zeros((u.shape[0], u.shape[1], u.shape[3], u.shape[4]))
shear_v = np.zeros((v.shape[0], v.shape[1], v.shape[3], v.shape[4]))

logging.debug("Calcolo dello shear usando la regressione lineare")
for t in range(u.shape[0]):  # Tempo
    for tod in range(u.shape[1]):  # Ora del giorno
        for i in range(u.shape[3]):  # Latitudine
            for j in range(u.shape[4]):  # Longitudine
                # Profili verticali di u e v per le quote selezionate
                z_subset = np.array(selected_z)
                u_profile = u_selected[t, tod, :, i, j]
                v_profile = v_selected[t, tod, :, i, j]

                # Filtra i dati validi
                valid_u = ~np.isnan(u_profile)
                valid_v = ~np.isnan(v_profile)

                # Regressione lineare su u
                if np.sum(valid_u) > 1:
                    z_u = z_subset[valid_u]
                    u_u = u_profile[valid_u]
                    z_mean_u = np.mean(z_u)
                    u_mean = np.mean(u_u)
                    shear_u[t, tod, i, j] = np.sum((z_u - z_mean_u) * (u_u - u_mean)) / np.sum((z_u - z_mean_u) ** 2)
                else:
                    shear_u[t, tod, i, j] = np.nan

                # Regressione lineare su v
                if np.sum(valid_v) > 1:
                    z_v = z_subset[valid_v]
                    v_v = v_profile[valid_v]
                    z_mean_v = np.mean(z_v)
                    v_mean = np.mean(v_v)
                    shear_v[t, tod, i, j] = np.sum((z_v - z_mean_v) * (v_v - v_mean)) / np.sum((z_v - z_mean_v) ** 2)
                else:
                    shear_v[t, tod, i, j] = np.nan

logging.debug("Calcolo della magnitudine dello shear")
shear_magnitude = np.sqrt(shear_u**2 + shear_v**2)

# Calcolo dello stress (\tau = \rho \cdot shear^2)
stress = rho * shear_magnitude**2

logging.debug("Aggiungi la variabile "stress" al dataset e salva")
if 'stress' not in ds.variables:
    stress_var = ds.createVariable('stress', 'f4', ('time', 'time_of_day_24', 'lat', 'lon'), fill_value=np.nan)
    stress_var.units = "Pa"
    stress_var.long_name = "Wind stress"
else:
    stress_var = ds.variables['stress']

stress_var[:] = stress

# Chiudi il file NetCDF
ds.close()

#TODO logging e  messaggi per il report
